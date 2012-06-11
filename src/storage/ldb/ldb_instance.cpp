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

#include <leveldb/env.h>
#include <leveldb/write_batch.h>

#include "common/define.hpp"
#include "common/util.hpp"
#include "common/directory_op.hpp"
#include "storage/storage_manager.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_comparator.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using namespace tair::common;

      LdbInstance::LdbInstance()
        : index_(0), db_version_care_(true), mutex_(NULL), db_(NULL), cache_(NULL),
          scan_it_(NULL), scan_bucket_(-1), still_have_(true), gc_(this)
      {
        db_path_[0] = '\0';
        stat_manager_ = new STAT_MANAGER_MAP();
      }

      LdbInstance::LdbInstance(int32_t index, bool db_version_care,
                               storage::storage_manager* cache)
        : index_(index), db_version_care_(db_version_care), mutex_(NULL), db_(NULL),
          cache_(dynamic_cast<tair::mdb_manager*>(cache)),
          scan_it_(NULL), scan_bucket_(-1), still_have_(true), gc_(this)
      {
        if (cache_ != NULL)
        {
          mutex_ = new tbsys::CThreadMutex[LOCKER_SIZE];
        }
        db_path_[0] = '\0';
        stat_manager_ = new STAT_MANAGER_MAP();
      }

      LdbInstance::~LdbInstance()
      {
        stop();
        if (mutex_ != NULL)
        {
          delete[] mutex_;
        }

        if (stat_manager_ != NULL)
        {
          for (STAT_MANAGER_MAP_ITER it = stat_manager_->begin(); it != stat_manager_->end(); ++it)
          {
            delete it->second;
          }
          delete stat_manager_;
        }

        // delete allocated env.
        if (options_.env != NULL)
        {
          delete options_.env;
        }
        // delete allocated comparator
        if (options_.comparator != NULL)
        {
          delete options_.comparator;
        }
      }

      bool LdbInstance::init_db()
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
            snprintf(db_path_, sizeof(db_path_), "%s%d/ldb", data_dir, index_);

            if (!(ret = tbsys::CFileUtil::mkdirs(db_path_)))
            {
              log_error("mkdir fail: %s", db_path_);
            }
            else
            {
              sanitize_option();

              log_info("init ldb %d: max_open_file: %d, write_buffer: %d", index_, options_.max_open_files, options_.write_buffer_size);
              leveldb::Status status = leveldb::DB::Open(options_, db_path_, &db_);

              if (!status.ok())
              {
                log_error("ldb init database fail, error: %s", status.ToString().c_str());
                ret = false;
              }
              else
              {
                if (!(ret = bg_task_.start(this)))
                {
                  log_error("start bg task fail. destroy db");
                }
                else if (!(ret = gc_.start()))
                {
                  log_error("start gc factory fail. destroy db");
                }

                if (!ret)
                {
                  destroy();
                }
              }
            }
          }
        }

        return ret;
      }

      bool LdbInstance::init_buckets(std::vector<int32_t> buckets)
      {
        bool ret = init_db();
        if (ret)
        {
          STAT_MANAGER_MAP* tmp_stat_manager = new STAT_MANAGER_MAP(*stat_manager_);

          for (std::vector<int32_t>::iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(*it);
            if (stat_it != tmp_stat_manager->end())
            {
              log_debug("bucket %d already inited.", *it);
            }
            else
            {
              stat_manager* new_stat = new stat_manager();
              new_stat->start(*it, db_path_);
              (*tmp_stat_manager)[*it] = new_stat;
              log_debug("bucket %d init ok.", *it);
            }
          }

          STAT_MANAGER_MAP* old_stat = stat_manager_;
          stat_manager_ = tmp_stat_manager;
          usleep(100);
          delete old_stat;
        }
        return ret;
      }

      void LdbInstance::close_buckets(std::vector<int32_t> buckets)
      {
        STAT_MANAGER_MAP* tmp_stat_manager = new STAT_MANAGER_MAP(*stat_manager_);
        std::vector<int32_t> gc_buckets;
        std::vector<stat_manager*> stop_stats;

        // for gc. when close, bucket can't write (migrate maybe)
        // so it is ok to get sequence and file_number one time.
        for (std::vector<int32_t>::iterator it = buckets.begin(); it != buckets.end(); ++it)
        {
          STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(*it);

          if (stat_it == tmp_stat_manager->end())
          {
            log_warn("close bucket %d fail: not exist.", *it);
          }
          else
          {
            gc_buckets.push_back(*it);
            stop_stats.push_back(stat_it->second);
            tmp_stat_manager->erase(stat_it);
          }
        }

        STAT_MANAGER_MAP* old_stat = stat_manager_;
        stat_manager_ = tmp_stat_manager;
        usleep(100);
        // add gc
        gc_.add(gc_buckets, GC_BUCKET);

        for (std::vector<stat_manager*>::iterator it = stop_stats.begin(); it != stop_stats.end(); ++it)
        {
          (*it)->destroy();
          delete (*it);
        }
        delete old_stat;

        // no bucket exist. destory db
        if (stat_manager_->empty())
        {
          destroy();
        }
      }

      void LdbInstance::stop()
      {
        gc_.stop();
        bg_task_.stop();

        log_warn("stop ldb %s", db_path_);
        if (db_ != NULL)
        {
          delete db_;
          db_ = NULL;
        }
        STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
        for (STAT_MANAGER_MAP_ITER it = tmp_stat_manager->begin(); it != tmp_stat_manager->end(); ++it)
        {
          it->second->stop();
        }
      }

      void LdbInstance::destroy()
      {
        stop();
        gc_.destroy();

        leveldb::Status status = leveldb::DestroyDB(db_path_, options_);
        if (!status.ok())
        {
          log_error("remove ldb database fail. path: %s, error: %s", db_path_, status.ToString().c_str());
        }
        else
        {
          log_debug("destroy ldb database ok: %s", db_path_);
        }
      }

      int LdbInstance::put(int bucket_number, tair::common::data_entry& key,
                           tair::common::data_entry& value,
                           bool version_care, uint32_t expire_time)
      {
        assert(db_ != NULL);

        uint32_t cdate = 0, mdate = 0, edate = 0;
        int stat_data_size = 0, stat_use_size = 0, item_count = 1;

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
        tbsys::CThreadGuard mutex_guard(get_mutex(key));
        std::string db_value;
        int rc = TAIR_RETURN_SUCCESS;

        // db version care
        if (db_version_care_ && version_care)
        {
          rc = do_get(ldb_key, db_value, false, false); // not fill cache or update cache stat

          if (TAIR_RETURN_SUCCESS == rc)
          {
            ldb_item.assign(const_cast<char*>(db_value.data()), db_value.size());
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
              item_count = 0;
            }
          }
          else
          {
            rc = TAIR_RETURN_SUCCESS; // get fail does not matter
          }
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

          rc = do_put(ldb_key, ldb_item, SHOULD_PUT_FILL_CACHE(key.data_meta.flag));

          if (TAIR_RETURN_SUCCESS == rc)
          {
            stat_data_size += ldb_key.key_size() + ldb_item.value_size();
            stat_use_size += ldb_key.size() + ldb_item.size();
            stat_add(bucket_number, key.area, stat_data_size, stat_use_size, item_count);
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

      // batch_put is for importing data as fast as possible, so lock and cache/db consistency is ignored,
      // once used when db is running, it may have the risk of data inconsistency.
      int LdbInstance::batch_put(int bucket_number, int area, mput_record_vec* record_vec, bool version_care)
      {
        if (record_vec->size() <= 0)
        {
          return TAIR_RETURN_FAILED;
        }

        leveldb::WriteBatch batch;
        int stat_data_size = 0, stat_use_size = 0, item_count = record_vec->size();

        int rc = TAIR_RETURN_SUCCESS;

        // NO lock here, cache maybe dirty
        for (mput_record_vec::iterator it = record_vec->begin() ; it != record_vec->end(); ++it)
        {
          data_entry& key = *((*it)->key);
          data_entry& value = (*it)->value->get_d_entry();
          uint32_t cdate = 0, mdate = 0, edate = 0;
          if (key.data_meta.cdate == 0 || version_care)
          {
            cdate = time(NULL);
            mdate = cdate;
            edate = (*it)->value->get_expire();
            if(edate > 0 && edate < static_cast<uint32_t>(mdate))
            {
              edate += mdate;
            }
          }
          else
          {
            cdate = key.data_meta.cdate;
            mdate = key.data_meta.mdate;
            edate = key.data_meta.edate;
          }

          data_entry mkey = key;
          // merge area
          mkey.merge_area(area);

          LdbKey ldb_key(mkey.get_data(), mkey.get_size(), bucket_number, 0);
          LdbItem ldb_item;
          // db version care
          if (db_version_care_)
          {
            std::string db_value;
            rc = do_get(ldb_key, db_value, false, false); // not fill cache or update cache stat

            if (TAIR_RETURN_SUCCESS == rc)
            {
              ldb_item.assign(const_cast<char*>(db_value.data()), db_value.size());
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
                item_count--;
              }
            }
            else
            {
              rc = TAIR_RETURN_SUCCESS; // get fail does not matter
            }
          }

          if (TAIR_RETURN_SUCCESS != rc)
          {
            break;
          }

          // just remove cache.
          // avoid cache lock, we can clear all cache when dump over
          // if (cache_ != NULL)
          // {
          //   cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
          // }

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
          batch.Put(leveldb::Slice(ldb_key.data(), ldb_key.size()),
                    leveldb::Slice(ldb_item.data(), ldb_item.size()));

          stat_data_size += ldb_key.key_size() + ldb_item.value_size();
          stat_use_size += ldb_key.size() + ldb_item.size();

          //update key's meta info
          key.data_meta.flag = ldb_item.meta().flag_;
          key.data_meta.cdate = ldb_item.meta().cdate_;
          key.data_meta.edate = edate;
          key.data_meta.mdate = ldb_item.meta().mdate_;
          key.data_meta.version = ldb_item.meta().version_;
          key.data_meta.keysize = key.get_size();
          key.data_meta.valsize = value.get_size();
        }

        if (TAIR_RETURN_SUCCESS == rc)
        {
          leveldb::Status status = db_->Write(write_options_, &batch, bucket_number);

          if (!status.ok())
          {
            log_error("update batch ldb fail. %s", status.ToString().c_str());
            rc = TAIR_RETURN_FAILED;
          }
          else
          {
            stat_add(bucket_number, area, stat_data_size, stat_use_size, item_count);
          }
        }

        return rc;
      }

      int LdbInstance::get(int bucket_number, tair::common::data_entry& key, tair::common::data_entry& value)
      {
        assert(db_ != NULL);

        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
        LdbItem ldb_item;
        std::string db_value;

        tbsys::CThreadGuard mutex_guard(get_mutex(key));
        int rc = do_get(ldb_key, db_value, true);

        if (TAIR_RETURN_SUCCESS == rc)
        {
          ldb_item.assign(const_cast<char*>(db_value.data()), db_value.size());
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

      int LdbInstance::remove(int bucket_number, tair::common::data_entry& key, bool version_care)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        LdbKey ldb_key(key.get_data(), key.get_size(), bucket_number);
        LdbItem ldb_item;
        std::string db_value;
        tbsys::CThreadGuard mutex_guard(get_mutex(key));

        if (db_version_care_ && version_care)
        {
          rc = do_get(ldb_key, db_value, false, false);
          if (TAIR_RETURN_SUCCESS == rc)
          {
            ldb_item.assign(const_cast<char*>(db_value.data()), db_value.size());
            if (version_care &&
                key.data_meta.version != 0 &&
                key.data_meta.version != ldb_item.meta().version_)
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
            stat_sub(bucket_number, key.area, ldb_key.key_size() + ldb_item.value_size(), ldb_key.size() + ldb_item.size(), 1);
          }
        }

        log_debug("ldb::get %d, key len: %d", rc, key.get_size());

        return rc;
      }

      int LdbInstance::op_cmd(ServerCmdType cmd, std::vector<std::string>& params)
      {
        assert(db_ != NULL);
        int ret = TAIR_RETURN_SUCCESS;
        log_warn("op cmd %d, param size: %d", cmd, params.size());

        switch (cmd) {
        case TAIR_SERVER_CMD_FLUSH_MMT:
        {
          leveldb::Status status = db_->ForceCompactMemTable();
          if (!status.ok())
          {
            log_error("op cmd flush mem fail: %s", status.ToString().c_str());
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
        case TAIR_SERVER_CMD_RESET_DB: // just rename here
        {
          // delete directory may cost too much time. we just rename here.
          // if (!DirectoryOp::delete_directory_recursively(db_path_))
          // {
          //   log_error("delete db path fail: %s", db_path_);
          //   ret = TAIR_RETURN_FAILED;
          // }
          // just rename to back db path
          std::string back_db_path = get_back_path(db_path_);
          if (::access(db_path_, F_OK) != 0)
          {
            // just consider it's ok. maybe ::access fail because other reason.
            log_warn("resetdb but orignal db path is not exist: %s, ignore it.", db_path_);
          }
          else if (::rename(db_path_, back_db_path.c_str()) != 0)
          {
            log_error("rename db %s to back db %s fail. error: %s", db_path_, back_db_path.c_str(), strerror(errno));
            ret = TAIR_RETURN_FAILED;
          }
          break;
        }
        default:
        {
          ret = TAIR_RETURN_NOT_SUPPORTED;
          break;
        }
        }
        return ret;
      }

      bool LdbInstance::begin_scan(int bucket_number)
      {
        log_info("begin scan");
        bool ret = true;
        if (scan_it_ != NULL)   // not close previous scan
        {
          delete scan_it_;
          scan_it_ = NULL;
        }

        char scan_key[LDB_KEY_META_SIZE];
        LdbKey::build_key_meta(scan_key, bucket_number);

        leveldb::ReadOptions scan_read_options = read_options_;
        scan_read_options.fill_cache = false; // not fill cache
        scan_it_ = db_->NewIterator(scan_read_options);
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
          scan_bucket_ = bucket_number;
          still_have_ = true;
        }
        return ret;
      }

      bool LdbInstance::end_scan()
      {
        if (scan_it_ != NULL)
        {
          delete scan_it_;
          scan_it_ = NULL;
          scan_bucket_ = -1;
        }
        return true;
      }

      bool LdbInstance::get_next_items(std::vector<item_data_info*>& list)
      {
        list.clear();

        static const int32_t migrate_batch_size =
          TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MIGRATE_BATCH_SIZE, 1048576); // 1M default
        static const int32_t migrate_batch_count =
          TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MIGRATE_BATCH_COUNT, 2000); // 2000 default

        if (NULL == scan_it_)
        {
          log_error("not begin_scan");
        }
        else if (still_have_)
        {
          LdbKey ldb_key;       // reuse is ok.
          LdbItem ldb_item;
          int32_t key_size = 0, value_size = 0, total_size = 0, batch_size = 0, batch_count = 0;

          while (batch_size < migrate_batch_size && batch_count < migrate_batch_count && scan_it_->Valid())
          {
            // match bucket
            if (LdbKey::decode_bucket_number(scan_it_->key().data() + LDB_EXPIRED_TIME_SIZE) == scan_bucket_)
            {
              ldb_key.assign(const_cast<char*>(scan_it_->key().data()), scan_it_->key().size());
              ldb_item.assign(const_cast<char*>(scan_it_->value().data()), scan_it_->value().size());

              key_size = ldb_key.key_size();
              value_size = ldb_item.value_size();
              total_size = ITEM_HEADER_LEN + key_size + value_size;
              item_data_info* data = (item_data_info*) new char[total_size];
              data->header.keysize = key_size;
              data->header.version = ldb_item.meta().version_;
              data->header.valsize = value_size;
              data->header.flag = ldb_item.meta().flag_;
              data->header.cdate = ldb_item.meta().cdate_;
              data->header.mdate = ldb_item.meta().mdate_;
              data->header.edate = ldb_item.meta().edate_;

              memcpy(data->m_data, ldb_key.key(), key_size);
              memcpy(data->m_data+key_size, ldb_item.value(), value_size);

              list.push_back(data);
              scan_it_->Next();
              batch_size += total_size;
              ++batch_count;
            }
            else
            {
              still_have_ = false;
              break;
            }
          }
          log_debug("migrate count: %d, size: %d", list.size(), batch_size);
          if (list.empty())
          {
            still_have_ = false;
          }
        }

        return still_have_;
      }

      void LdbInstance::get_stats(tair_stat* stat)
      {
        if (NULL != db_)        // not init now, no stat
        {
          if (TBSYS_LOGGER._level > TBSYS_LOG_LEVEL_WARN)
          {
            log_debug("ldb bucket get stat %p", stat);
            std::string stat_value;
            if (get_db_stat(db_, stat_value, "stats"))
            {
              log_info("ldb status: %s", stat_value.c_str());
            }
            else
            {
              log_error("get ldb status fail, uncompleted status: %s", stat_value.c_str());
            }
          }

          if (stat != NULL)
          {
            // get all stat information
            STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
            for (STAT_MANAGER_MAP_ITER it = tmp_stat_manager->begin(); it != tmp_stat_manager->end(); ++it)
            {
              tair_pstat *pstat = it->second->get_stat();
              for (int i = 0; i < TAIR_MAX_AREA_COUNT; i++)
              {
                stat[i].data_size_value += pstat[i].data_size();
                stat[i].use_size_value += pstat[i].use_size();
                stat[i].item_count_value += pstat[i].item_count();
              }
            }
          }
        }
      }

      int LdbInstance::clear_area(int32_t area)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (area >= 0)
        {
          STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
          for (STAT_MANAGER_MAP_ITER it = tmp_stat_manager->begin(); it != tmp_stat_manager->end(); ++it)
          {
            it->second->stat_reset(area); // clear stat value for this area.
          }
          ret = gc_.add(area, GC_AREA);
        }
        return ret;
      }

      bool LdbInstance::exist(int32_t bucket_number)
      {
        STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
        return tmp_stat_manager->find(bucket_number) != tmp_stat_manager->end();
      }

      int LdbInstance::do_put(LdbKey& ldb_key, LdbItem& ldb_item, bool fill_cache)
      {
        int rc = TAIR_RETURN_SUCCESS;
        leveldb::Status status = db_->Put(write_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                          leveldb::Slice(ldb_item.data(), ldb_item.size()));
        if (!status.ok())
        {
          log_error("update ldb item fail. %s", status.ToString().c_str());
          rc = TAIR_RETURN_FAILED;
        }
        else if (cache_ != NULL)
        {
          // client's requsting fill_cache
          if (fill_cache)
          {
            log_debug("fill cache");
            rc = cache_->raw_put(ldb_key.key(), ldb_key.key_size(), ldb_item.data(), ldb_item.size(),
                                 ldb_item.meta().flag_, ldb_item.meta().edate_);
            if (rc != TAIR_RETURN_SUCCESS) // what happend
            {
              log_error("::put. put cache fail, rc: %d", rc);
            }
          }
          else
          {
            log_debug("not fill cache");
            rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size());
            if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // what happened ?
            {
              log_error("::put. remove cache fail: %d", rc);
            }
            else
            {
              rc = TAIR_RETURN_SUCCESS;
            }
          }
        }

        return rc;
      }

      int LdbInstance::do_get(LdbKey& ldb_key, std::string& value, bool fill_cache, bool update_stat)
      {
        int rc = TAIR_RETURN_FAILED;
        if (cache_ != NULL)
        {
          rc = cache_->raw_get(ldb_key.key(), ldb_key.key_size(), value, update_stat);
          if (TAIR_RETURN_SUCCESS == rc) // cache hit
          {
            log_debug("ldb cache hit");
          }
        }

        // cache miss, but not expired, cause cache expired, db expired too.
        if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_EXPIRED)
        {
          leveldb::Status status = db_->Get(read_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()),
                                            &value);
          if (status.ok())
          {
            rc = TAIR_RETURN_SUCCESS;
            if (fill_cache && cache_ != NULL)     // fill cache
            {
              LdbItem ldb_item;
              ldb_item.assign(const_cast<char*>(value.data()), value.size());
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

      int LdbInstance::do_remove(LdbKey& ldb_key)
      {
        // first remvoe db, then cache
        int rc = TAIR_RETURN_SUCCESS;
        leveldb::Status status = db_->Delete(write_options_, leveldb::Slice(ldb_key.data(), ldb_key.size()));
        if (!status.ok())
        {
          log_error("remove ldb item fail: %s", status.ToString().c_str()); // ignore return status
          rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
        }

        if (TAIR_RETURN_SUCCESS == rc && cache_ != NULL)
        {
          rc = cache_->raw_remove(ldb_key.key(), ldb_key.key_size()); // should always succeed
          if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) // what happened ?
          {
            log_error("remove cache fail. rc: %d", rc);
          }
          else
          {
            rc = TAIR_RETURN_SUCCESS; // data not exist mean remove successfully
          }
        }
        return rc;
      }

      void LdbInstance::stat_add(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count)
      {
        STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
        STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(bucket_number);
        if (stat_it != tmp_stat_manager->end())
        {
          stat_it->second->stat_add(area, data_size, use_size, item_count);
        }
      }

      void LdbInstance::stat_sub(int32_t bucket_number, int32_t area, int32_t data_size, int32_t use_size, int32_t item_count)
      {
        STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
        STAT_MANAGER_MAP_ITER stat_it = tmp_stat_manager->find(bucket_number);
        if (stat_it != tmp_stat_manager->end())
        {
          stat_it->second->stat_sub(area, data_size, use_size, item_count);
        }
      }

      void LdbInstance::get_buckets(std::vector<int32_t>& buckets)
      {
        STAT_MANAGER_MAP* tmp_stat_manager = stat_manager_;
        for (STAT_MANAGER_MAP_ITER it = tmp_stat_manager->begin(); it != tmp_stat_manager->end(); ++it)
        {
          buckets.push_back(it->first);
        }
      }

      void LdbInstance::sanitize_option()
      {
        options_.error_if_exists = false; // exist is ok
        options_.create_if_missing = true; // create if not exist
        options_.comparator = new LdbComparatorImpl(&gc_); // self-defined comparator
        options_.paranoid_checks = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_PARANOID_CHECK, 0) > 0;
        options_.max_open_files = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_OPEN_FILES, 655350);
        options_.write_buffer_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_WRITE_BUFFER_SIZE, 4194304); // 4M
        options_.max_mem_usage_for_memtable = atoll(TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_MAX_MEM_USAGE_FOR_MEMTABLE, "1073741824"));
        options_.block_size = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_SIZE, 4096); // 4K
        options_.block_restart_interval = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BLOCK_RESTART_INTERVAL, 16); // 16
        options_.compression = static_cast<leveldb::CompressionType>(TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_COMPRESSION, leveldb::kSnappyCompression));
        options_.kL0_CompactionTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_COMPACTION_TRIGGER, 4);
        options_.kL0_SlowdownWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_SLOWDOWN_WRITE_TRIGGER, 8);
        options_.kL0_StopWritesTrigger = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_L0_STOP_WRITE_TRIGGER, 12);
        options_.kMaxMemCompactLevel = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_MAX_MEMCOMPACT_LEVEL, 2);
        options_.kTargetFileSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_TARGET_FILE_SIZE, 2097152);
        options_.kBaseLevelSize = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BASE_LEVEL_SIZE, 10485760);
        // Env::Default() is a global static instance.
        // We allocate one env to one leveldb instance here.
        options_.env = leveldb::Env::Instance();
        read_options_.verify_checksums = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_READ_VERIFY_CHECKSUMS, 0) != 0;
        read_options_.fill_cache = true;
        write_options_.sync = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_WRITE_SYNC, 0) != 0;
        // remainning avaliable config: comparator, env, block cache.
      }

      tbsys::CThreadMutex* LdbInstance::get_mutex(const tair::common::data_entry& key)
      {
        tbsys::CThreadMutex* ret = NULL;
        if (mutex_ != NULL)     // no cache, no lock.
        {
          ret = mutex_ + util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
        }
        return ret;
      }

    }
  }
}
