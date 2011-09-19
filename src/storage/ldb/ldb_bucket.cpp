/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/util.hpp"
#include "ldb_bucket.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using namespace tair::common;

      LdbBucket::LdbBucket() : db_(NULL), scan_it_(NULL)
      {
        db_path_[0] = '\0';
        mutex_ = new tbsys::CThreadMutex[LOCKER_SIZE];
        scan_end_key_[0] = '\0';
      }

      LdbBucket::~LdbBucket()
      {
        if (mutex_ != NULL)
        {
          delete[] mutex_;
        }
        stop();
      }

      bool LdbBucket::start(int bucket_number)
      {
        bool ret = true;
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
            stat_manager_.start(bucket_number, db_path_);
            log_debug("ldb init database %d ok", bucket_number);
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

      void LdbBucket::destory()
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

        int cdate = 0;
        int mdate = 0;
        int edate = 0;
        int stat_data_size = 0, stat_use_size = 0;
        int rc = TAIR_RETURN_SUCCESS;

        if(key.data_meta.cdate == 0 || version_care)
        {
          cdate = time(NULL);
          mdate = cdate;
          if(expire_time > 0)
          {
            edate = expire_time > static_cast<uint32_t>(mdate) ? expire_time : mdate + expire_time;
          }
        }
        else
        {
          cdate = key.data_meta.cdate;
          mdate = key.data_meta.mdate;
          edate = key.data_meta.edate;
        }

        tbsys::CThreadGuard mutex_guard(get_mutex(key));

        LdbKey ldb_key(bucket_number, key.get_data(), key.get_size());
        LdbItem ldb_item;
        std::string old_value;
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                          &old_value);

        if (status.ok())
        {
          // key already exist
          ldb_item.assign(const_cast<char*>(old_value.data()), old_value.length());

          if (ldb_item.is_expired())
          {
            ldb_item.meta().version_ = 0;
          }
          else
          {
            cdate = ldb_item.meta().cdate_; // set back the create time
            if (version_care)
            {
              // item is not expired & care version, check version
              if (key.data_meta.version != 0
                  && key.data_meta.version != ldb_item.meta().version_)
              {
                rc = TAIR_RETURN_VERSION_ERROR;
              }
            }
          }

          if (rc == TAIR_RETURN_SUCCESS)
          {
            stat_data_size -= ldb_key.key_size() + ldb_item.value_size();
            stat_use_size -= ldb_key.size() + ldb_item.size();
          }
        }
        else if (!status.IsNotFound()) // occure error, but not key NotFound
        {
          log_error("get old item from ldb database fail. %s", status.ToString().c_str());
          rc = TAIR_RETURN_FAILED;
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
          status = db_->Put(leveldb::WriteOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                            leveldb::Slice(ldb_item.data(), ldb_item.size()));

          if (!status.ok())
          {
            log_error("update ldb item fail. %s", status.ToString().c_str());
            rc = TAIR_RETURN_FAILED;
          }
          else
          {
            stat_data_size += ldb_key.key_size() + ldb_item.value_size();
            stat_use_size += ldb_key.size() + ldb_item.size();
            stat_manager_.stat_add(key.area, stat_data_size, stat_use_size);
          }

          //update key's meta info
          key.data_meta.flag = ldb_item.meta().flag_;
          key.data_meta.cdate = ldb_item.meta().cdate_;
          key.data_meta.edate = ldb_item.meta().edate_;
          key.data_meta.mdate = ldb_item.meta().mdate_;
          key.data_meta.version = ldb_item.meta().version_;
          key.data_meta.keysize = key.get_size();
          key.data_meta.valsize = value.get_size();
        }

        log_debug("ldb::put %s, key len: %d, value len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size(), value.get_size());
        return rc;
      }

      int LdbBucket::get(int bucket_number, data_entry& key, data_entry& value)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        std::string old_value;
        LdbKey ldb_key(bucket_number, key.get_data(), key.get_size());
        LdbItem ldb_item;
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                          &old_value);

        if (status.IsNotFound())
        {
          log_debug("get ldb item not found");
          rc = TAIR_RETURN_DATA_NOT_EXIST;
        }
        else if (status.ok())
        {
          ldb_item.assign(const_cast<char*>(old_value.data()), old_value.length());

          if (ldb_item.is_expired())
          {
            log_debug("remove expire data return: %d", remove(bucket_number, key, false));
            rc = TAIR_RETURN_DATA_EXPIRED;
          }
          else
          {
            value.set_data(ldb_item.value(), ldb_item.value_size());
            
            //update meta info
            key.data_meta.flag = value.data_meta.flag = ldb_item.meta().flag_;
            key.data_meta.cdate = value.data_meta.cdate = ldb_item.meta().cdate_;
            key.data_meta.edate = value.data_meta.edate = ldb_item.meta().edate_;
            key.data_meta.mdate = value.data_meta.mdate = ldb_item.meta().mdate_;
            key.data_meta.version = value.data_meta.version = ldb_item.meta().version_;
            key.data_meta.keysize = value.data_meta.keysize = key.get_size();
            key.data_meta.valsize = value.data_meta.valsize = ldb_item.value_size();
          }
        }
        else                    // get occur error
        {
          log_error("get ldb item fail: %s", status.ToString().c_str());
          rc = TAIR_RETURN_FAILED;
        }

        log_debug("ldb::get %s, key len: %d, value len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size(), value.get_size());

        return rc;
      }

      int LdbBucket::remove(int bucket_number, data_entry& key, bool version_care)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        tbsys::CThreadGuard mutex_guard(get_mutex(key));
        LdbKey ldb_key(bucket_number, key.get_data(), key.get_size());
        LdbItem ldb_item;
        leveldb::Status status;

        if (version_care)
        {
          std::string old_value;
          status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()),
                            &old_value);

          if (status.ok())
          {
            ldb_item.assign(const_cast<char*>(old_value.data()), old_value.length());

            if (key.data_meta.version != 0
                && key.data_meta.version != ldb_item.meta().version_)
            {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }
          else                    // get occur error
          {
            log_error("get ldb item fail: %s", status.ToString().c_str());
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
          }
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          status = db_->Delete(leveldb::WriteOptions(), leveldb::Slice(ldb_key.data(), ldb_key.size()));
          if (!status.ok())
          {
            log_error("remove ldb item fail: %s", status.ToString().c_str()); // ignore return status
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
          }
          else
          {
            stat_manager_.stat_sub(key.area, ldb_key.key_size() + ldb_item.value_size(), ldb_key.size() + ldb_item.size());
          }
        }

        log_debug("ldb::get %s, key len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size());

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
        LdbKey::build_key_meta(bucket_number, scan_key);

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
          LdbKey::build_key_meta(bucket_number+1, scan_end_key_);
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
        scan_end_key_[0] = '\0';
        return true;
      }

      bool LdbBucket::get_next_item(item_data_info* &data, bool& still_have)
      {
        bool ret = true;
        still_have = false;

        if (NULL == scan_it_)
        {
          log_error("not begin_scan");
          ret = false;
        }
        else
        {
          while(1)
          {
            if (scan_it_->Valid() && scan_it_->key().ToString() < scan_end_key_)
            {
              LdbItem ldb_item;
              ldb_item.assign(const_cast<char*>(scan_it_->value().data()), scan_it_->value().size());
          
              if (ldb_item.is_expired())
              {
                // TODO: remove
                continue;
              }
              else
              {
                int key_size = scan_it_->key().size();
                int total_size = ITEM_HEADER_LEN + key_size + ldb_item.value_size();
                data = (item_data_info *) new char[total_size];
                data->header.keysize = key_size;
                data->header.version = ldb_item.meta().version_;
                data->header.valsize = ldb_item.value_size();
                data->header.cdate = ldb_item.meta().cdate_;
                data->header.mdate = ldb_item.meta().mdate_;
                data->header.edate = ldb_item.meta().edate_;

                memcpy(data->m_data, scan_it_->key().data(), key_size);
                memcpy(data->m_data+key_size, ldb_item.value(), ldb_item.value_size());
              }
              scan_it_->Next();
            }
          }
        }

        return 0;
      }

      void LdbBucket::get_stat(tair_stat* stat)
      {
        if (NULL != db_)        // not init now, no stat
        {
          log_debug("ldb bucket get stat %p", stat);
          std::string stat_value;
          if (get_db_stat(stat_value))
          {
            // maybe return
            log_info("ldb status: %s", stat_value.c_str());
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

      // just string value
      bool LdbBucket::get_db_stat(std::string& value)
      {
        assert(db_ != NULL);
        value.clear();

        bool ret = true;
        char name[32];
        snprintf(name, sizeof(name), "%s", "leveldb.stats");
        std::string stat_value;
          
        if (!(ret = db_->GetProperty(leveldb::Slice(std::string(name)), &stat_value)))
        {
          log_error("get db stats fail");
        }
        else
        {
          value += stat_value;
        }
        return ret;
      }

      void LdbBucket::sanitize_option(leveldb::Options& options)
      {
        options.error_if_exists = false; // exist is ok
        options.create_if_missing = true; // create if not exist
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

      tbsys::CThreadMutex* LdbBucket::get_mutex(tair::common::data_entry& key)
      {
        return mutex_ + util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
      }

    }
  }
}

