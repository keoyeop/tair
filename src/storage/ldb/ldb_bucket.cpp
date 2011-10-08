/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb db engine implementation
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/util.hpp"
#include "storage/storage_manager.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_bucket.hpp"
#include "ldb_comparator.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using namespace tair::common;

      LdbBucket::LdbBucket() : db_(NULL), cache_(NULL), scan_it_(NULL)
      {
        db_path_[0] = '\0';
        scan_end_key_ = std::string(LDB_KEY_META_SIZE, '\0');
      }

      LdbBucket::LdbBucket(storage::storage_manager* cache) : db_(NULL), cache_(dynamic_cast<tair::mdb_manager*>(cache)), scan_it_(NULL)
      {
        db_path_[0] = '\0';
        scan_end_key_ = std::string(LDB_KEY_META_SIZE, '\0');
      }

      LdbBucket::~LdbBucket()
      {
        stop();
      }

      bool LdbBucket::start(int bucket_number)
      {
        bool ret = true;
        if (NULL == db_)
        {
          // enable to config multi path.. ?
          const char* data_dir = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_DATA_DIR, LDB_DEFAULT_DATA_DIR);

          if (NULL == data_dir)
          {
            log_error("ldb data dir path not config, item: %s.%s", TAIRLDB_SECTION, LDB_DATA_DIR);
            ret = false;
          }
          else
          {
            // leveldb data path
            snprintf(db_path_, PATH_MAX, "%s/tair_ldb_%06d", data_dir, bucket_number);

            leveldb::Options options;
            sanitize_option(options);

            log_debug("init ldb : max_open_file: %d, write_buffer: %d", options.max_open_files, options.write_buffer_size);
            leveldb::Status status = leveldb::DB::Open(options, db_path_, &db_);
            if (!status.ok())
            {
              log_error("ldb init database fail, error: %s", status.ToString().c_str());
              ret = false;
            }
            else
            {
              if (!(ret = bg_task_.start(this)))
              {
                log_error("start bg task fail");
              }
              else
              {
                stat_manager_.start(bucket_number, db_path_);
                log_debug("ldb init database %d ok", bucket_number);
              }
            }
          }
        }

        return ret;
      }

      void LdbBucket::stop()
      {
        log_debug("stop ldb %s", db_path_);
        if (db_ != NULL)
        {
          delete db_;
          db_ = NULL;
        }
      }

      void LdbBucket::destroy()
      {
        stop();

        leveldb::Status status = leveldb::DestroyDB(db_path_, leveldb::Options());
        if (!status.ok())
        {
          log_error("remove ldb database fail. path: %s, error: %s", db_path_, status.ToString().c_str());
        }
        else
        {
          log_debug("destroy ldb database ok: %s", db_path_);
        }
      }

      int LdbBucket::put(int bucket_number, data_entry& key, data_entry& value, bool version_care, uint32_t expire_time)
      {
        assert(db_ != NULL);

        uint32_t cdate = 0, mdate = 0, edate = 0;
        int stat_data_size = 0, stat_use_size = 0;

        if (key.data_meta.cdate == 0 || version_care)
        {
          cdate = time(NULL);
          mdate = cdate;
          if(expire_time > 0)
          {
            edate = expire_time >= static_cast<uint32_t>(mdate) ? expire_time : mdate + expire_time;
          }
        }
        else
        {
          cdate = key.data_meta.cdate;
          mdate = key.data_meta.mdate;
          edate = key.data_meta.edate;
        }

        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number, edate);
        LdbItem ldb_item;
        int rc = do_get(ldb_key, ldb_item, false); // not fill cache

        if (TAIR_RETURN_SUCCESS == rc)
        {
          // ldb already check expired. no need here.
          cdate = ldb_item.meta().cdate_; // set back the create time
          if (version_care)
          {
            // item care version, check version
            if (key.data_meta.version != 0
                && key.data_meta.version != ldb_item.meta().version_)
            {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }

          if (rc == TAIR_RETURN_SUCCESS)
          {
            stat_data_size -= ldb_key.key_size() + ldb_item.value_size();
            stat_use_size -= ldb_key.size() + ldb_item.size();
          }
        }
        else
        {
          rc = TAIR_RETURN_SUCCESS; // get fail does not matter
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          ldb_item.meta().flag_ = value.data_meta.flag;
          ldb_item.meta().cdate_ = cdate;
          ldb_item.meta().mdate_ = mdate;
          ldb_item.meta().edate_ = edate;
          if (version_care)
          {
            ldb_item.meta().version_++;
          }
          else
          {
            ldb_item.meta().version_ = key.data_meta.version;
          }

          ldb_item.set(value.get_data(), value.get_size());

          // TODO: option specified ..
          log_debug("::put edate %u, now: %u. %d:%s", edate, mdate, ldb_key.size(), ldb_key.data());
          rc = do_put(ldb_key, ldb_item);

          if (TAIR_RETURN_SUCCESS == rc)
          {
            stat_data_size += ldb_key.key_size() + ldb_item.value_size();
            stat_use_size += ldb_key.size() + ldb_item.size();
            stat_manager_.stat_add(key.area, stat_data_size, stat_use_size);
          }

          //update key's meta info
          key.data_meta.flag = ldb_item.meta().flag_;
          key.data_meta.cdate = ldb_item.meta().cdate_;
          key.data_meta.edate = edate;
          key.data_meta.mdate = ldb_item.meta().mdate_;
          key.data_meta.version = ldb_item.meta().version_;
          key.data_meta.keysize = key.get_size();
          key.data_meta.valsize = value.get_size();
        }

        log_debug("ldb::put %d, key len: %d, value len: %d", rc, key.get_size(), value.get_size());
        return rc;
      }

      int LdbBucket::get(int bucket_number, data_entry& key, data_entry& value)
      {
        assert(db_ != NULL);

        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
        LdbItem ldb_item;
        int rc = do_get(ldb_key, ldb_item, true);

        if (TAIR_RETURN_SUCCESS == rc)
        {
          // already check expired. no need here.
          value.set_data(ldb_item.value(), ldb_item.value_size());

          // update meta info
          key.data_meta.flag = value.data_meta.flag = ldb_item.meta().flag_;
          key.data_meta.cdate = value.data_meta.cdate = ldb_item.meta().cdate_;
          key.data_meta.edate = value.data_meta.edate = ldb_item.meta().edate_;
          key.data_meta.mdate = value.data_meta.mdate = ldb_item.meta().mdate_;
          key.data_meta.version = value.data_meta.version = ldb_item.meta().version_;
          key.data_meta.keysize = value.data_meta.keysize = key.get_size();
          key.data_meta.valsize = value.data_meta.valsize = ldb_item.value_size();
        }

        log_debug("ldb::get %d, key len: %d, value len: %d", rc, key.get_size(), value.get_size());

        return rc;
      }

      int LdbBucket::remove(int bucket_number, data_entry& key, bool version_care)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
        LdbItem ldb_item;

        if (version_care)
        {
          rc = do_get(ldb_key, ldb_item, false);
          if (TAIR_RETURN_SUCCESS == rc)
          {
            if (key.data_meta.version != 0
                && key.data_meta.version != ldb_item.meta().version_)
            {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          rc = do_remove(ldb_key);
          if (TAIR_RETURN_SUCCESS == rc)
          {
            stat_manager_.stat_sub(key.area, ldb_key.key_size() + ldb_item.value_size(), ldb_key.size() + ldb_item.size());
          }
        }

        log_debug("ldb::get %d, key len: %d", rc, key.get_size());

        return rc;
      }

      bool LdbBucket::begin_scan(int bucket_number)
      {
        bool ret = true;
        if (scan_it_ != NULL)   // not close previous scan
        {
          delete scan_it_;
          scan_it_ = NULL;
        }

        char scan_key[LDB_KEY_META_SIZE];
        LdbKey::build_key_meta(scan_key, bucket_number);

        scan_it_ = db_->NewIterator(leveldb::ReadOptions());
        if (NULL == scan_it_)
        {
          log_error("get ldb scan iterator fail");
          ret = false;
        }
        else
        {
          scan_it_->Seek(leveldb::Slice(scan_key, sizeof(scan_key)));
        }

        if (ret)
        {
          LdbKey::build_key_meta(const_cast<char*>(scan_end_key_.data()), bucket_number+1);
        }
        return ret;
      }

      bool LdbBucket::end_scan()
      {
        if (scan_it_ != NULL)
        {
          delete scan_it_;
          scan_it_ = NULL;
        }
        return true;
      }

      bool LdbBucket::get_next_item(item_data_info* &data, bool& still_have)
      {
        bool ret = false;
        still_have = false;

        if (NULL == scan_it_)
        {
          log_error("not begin_scan");
        }
        else
        {
          while(1)
          {
            if (scan_it_->Valid())
            {
              if (scan_it_->key().ToString() < scan_end_key_)
              {
                LdbKey ldb_key;
                LdbItem ldb_item;
                ldb_key.assign(const_cast<char*>(scan_it_->key().data()), scan_it_->key().size());
                ldb_item.assign(const_cast<char*>(scan_it_->value().data()), scan_it_->value().size());

                int key_size = ldb_key.key_size(), value_size = ldb_item.value_size();
                int total_size = ITEM_HEADER_LEN + key_size + value_size;
                data = (item_data_info *) new char[total_size];
                data->header.keysize = key_size;
                data->header.version = ldb_item.meta().version_;
                data->header.valsize = value_size;
                data->header.cdate = ldb_item.meta().cdate_;
                data->header.mdate = ldb_item.meta().mdate_;
                data->header.edate = ldb_item.meta().edate_;

                memcpy(data->m_data, ldb_key.key(), key_size);
                memcpy(data->m_data+key_size, ldb_item.value(), value_size);

                ret = true;
              }
              scan_it_->Next();
            }
            break;
          }
        }

        return ret;
      }

      // TODO: save
      void LdbBucket::get_stat(tair_stat* stat)
      {
        if (NULL != db_)        // not init now, no stat
        {
          log_debug("ldb bucket get stat %p", stat);
          std::string stat_value;
          if (get_db_stat(db_, stat_value, "stats"))
          {
            // maybe return
            log_info("ldb status: %s", stat_value.c_str());
            if (get_db_stat(db_, stat_value, "ranges"))
            {
              log_info("ldb level ranges: %s", stat_value.c_str());
            }
          }
          else
          {
            log_error("get ldb status fail, uncompleted status: %s", stat_value.c_str());
          }

          if (stat != NULL)
          {
            tair_pstat *pstat = stat_manager_.get_stat();
            for(int i = 0; i < TAIR_MAX_AREA_COUNT; i++)
            {
              stat[i].data_size_value += pstat[i].data_size();
              stat[i].use_size_value += pstat[i].use_size();
              stat[i].item_count_value += pstat[i].item_count();
            }
          }
        }
      }

      int LdbBucket::clear_bucket(int32_t bucket_number)
      {
        int64_t sequence = 0;
        get_db_stat(db_, sequence, "sequence");
        return gc_.add_bucket(bucket_number, sequence, time(NULL));
      }

      int LdbBucket::clear_area(int32_t area)
      {
        int64_t sequence = 0;
        // clear cache.
        cache_->clear(area);
        get_db_stat(db_, sequence, "sequence");
        return gc_.add_area(area, sequence, time(NULL));
      }

      int LdbBucket::do_put(LdbKey& ldb_key, LdbItem& ldb_item)
      {
        // consider if put db successfully but put cache fail, then the consistency between db and cache will be got dirty.
        // so clear cache first to avoid dirty data once fail in any condition.

        int rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
        if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // remove ocurr unexpected fail. maybe can not happen
        {
          log_error("::put. remove cache fail, rc: %d", rc);
        }
        else
        {
          leveldb::Status status = db_->Put(leveldb::WriteOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                            leveldb::Slice(ldb_item.data(), ldb_item.size()));
          if (!status.ok())
          {
            log_error("update ldb item fail. %s", status.ToString().c_str());
            rc = TAIR_RETURN_FAILED;
          }
          else                  // fill cache
          {
            rc = TAIR_RETURN_SUCCESS;
            int tmp_rc = cache_->raw_put(ldb_key.key(), ldb_key.key_size(), ldb_item.data(), ldb_item.size(),
                                         ldb_item.meta().flag_, ldb_item.meta().edate_);
            if (tmp_rc != TAIR_RETURN_SUCCESS) // ignore fail. cause cache has no such data is OK.
            {
              log_debug("::put. put cache fail: %d", tmp_rc);
            }
          }
        }

        return rc;
      }

      int LdbBucket::do_get(LdbKey& ldb_key, LdbItem& ldb_item, bool fill_cache)
      {
        std::string value;
        int rc = cache_->raw_get(ldb_key.key(), ldb_key.key_size(), value);

        if (TAIR_RETURN_SUCCESS == rc) // cache hit
        {
          log_debug("ldb cache hit");
          ldb_item.assign(const_cast<char*>(value.data()), value.size());
        }
        else if (rc != TAIR_RETURN_DATA_EXPIRED) // cache miss, but not expired, cause cache expired, db expired too.
        {
          leveldb::Status status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                            &value);
          if (status.ok())
          {
            rc = TAIR_RETURN_SUCCESS;
            ldb_item.assign(const_cast<char*>(value.data()), value.size());
            if (fill_cache)     // fill cache
            {
              int tmp_rc = cache_->raw_put(ldb_key.key(), ldb_key.key_size(), ldb_item.data(), ldb_item.size(),
                                           ldb_item.meta().flag_, ldb_item.meta().edate_);
              if (tmp_rc != TAIR_RETURN_SUCCESS) // ignore return value.
              {
                log_debug("::get. put cache fail, rc: %d", tmp_rc);
              }
            }
          }
          else
          {
            log_debug("get ldb item not found");
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;            
          }
        }

        return rc;
      }

      int LdbBucket::do_remove(LdbKey& ldb_key)
      {
        // first remvoe cache, then db
        int rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
        if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // cache must remove successfully, except not exist.
        {
          log_error("remove cache fail. rc: %d", rc);
        }
        else
        {
          leveldb::Status status = db_->Delete(leveldb::WriteOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()));
          if (status.ok())
          {
            rc = TAIR_RETURN_SUCCESS;
          }
          else
          {
            log_error("remove ldb item fail: %s", status.ToString().c_str()); // ignore return status
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
          }
        }

        return rc;
      }

      void LdbBucket::sanitize_option(leveldb::Options& options)
      {
        options.error_if_exists = false; // exist is ok
        options.create_if_missing = true; // create if not exist
        options.comparator = LdbComparator(&gc_); // self-defined comparator
        options.paranoid_checks = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_INIT_CHECK, 0) > 0;
        options.max_open_files = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_OPEN_FILES, 655350);
        options.write_buffer_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_WRITE_BUFFER_SIZE, 4194304); // 4M
        options.block_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_SIZE, 4096); // 4K
        options.block_restart_interval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_RESTART_INTERVAL, 16); // 16
        options.compression = static_cast<leveldb::CompressionType>(TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_COMPRESSION, leveldb::kSnappyCompression));
        options.kL0_CompactionTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_COMPACTION_TRIGGER, 4);
        options.kL0_SlowdownWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_SLOWDOWN_WRITE_TRIGGER, 8);
        options.kL0_StopWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_STOP_WRITE_TRIGGER, 12);
        options.kMaxMemCompactLevel = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_MEMCOMPACT_LEVEL, 2);
        options.kTargetFileSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_TARGET_FILE_SIZE, 2097152);
        options.kBlockSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_SIZE, 4096);
        // remainning avaliable config: comparator, env, block cache.
      }

    }
  }
}
