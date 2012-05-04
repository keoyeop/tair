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

#include "storage/mdb/mdb_factory.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_manager.hpp"
#include "ldb_instance.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      UsingLdbManager::UsingLdbManager() :
        ldb_instance_(NULL), db_count_(0), cache_(NULL), cache_file_path_(""), next_(NULL), time_(0)
      {
      }
      UsingLdbManager::~UsingLdbManager()
      {
        destroy();
      }
      bool UsingLdbManager::can_destroy()
      {
        return time(NULL) - time_ > DESTROY_LDB_MANGER_INTERVAL_S;
      }
      void UsingLdbManager::destroy()
      {
        char time_buf[32];
        tbsys::CTimeUtil::timeToStr(time_, time_buf);
        log_warn("destroy last ldb manager. time: %s", time_buf);

        if (ldb_instance_ != NULL)
        {
          for (int i = 0; i < db_count_; i++)
          {
            delete ldb_instance_[i];
          }
          delete [] ldb_instance_;
          ldb_instance_ = NULL;
        }
        if (cache_ != NULL)
        {
          delete cache_;
          if (::unlink(cache_file_path_.c_str()) != 0)
          {
            log_error("unlink cache file fail: %s, error: %s", cache_file_path_.c_str(), strerror(errno));
          }
        }
      }



      LdbManager::LdbManager() : cache_(NULL), scan_ldb_(NULL), using_head_(NULL), using_tail_(NULL)
      {
        init();
      }

      LdbManager::~LdbManager()
      {
        destroy();
      }

      int LdbManager::init()
      {
        std::string cache_stat_path;
        if (TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_CACHE, 1) > 0)
        {
          cache_ = dynamic_cast<tair::mdb_manager*>(mdb_factory::create_embedded_mdb());
          if (NULL == cache_)
          {
            log_error("init ldb memory cache fail.");
          }
          else
          {
            const char* log_file_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_FILE, "server.log");
            const char* pos = strrchr(log_file_path, '/');
            if (NULL == pos)
            {
              cache_stat_path.assign("./");
            }
            else
            {
              cache_stat_path.assign(log_file_path, pos - log_file_path);
            }

            if (!cache_stat_.start(cache_stat_path.c_str(),
                                   TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_CACHE_STAT_FILE_SIZE, (20*1<<20))))
            {
              log_error("start cache stat fail.");
            }
          }
        }

        bool db_version_care = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_VERSION_CARE, 1) > 0;
        db_count_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_INSTANCE_COUNT, 1);
        ldb_instance_ = new LdbInstance*[db_count_];
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i] = new LdbInstance(i + 1, db_version_care, cache_);
        }

        log_warn("ldb storage engine construct count: %d, db version care: %s, use cache: %s, cache stat path: %s",
                 db_count_, db_version_care ? "yes" : "no", cache_ != NULL ? "yes" : "no", cache_stat_path.c_str());

        return TAIR_RETURN_SUCCESS;
      }

      int LdbManager::destroy()
      {
        LdbInstance** instance = ldb_instance_;
        ldb_instance_ = NULL;
        if (instance != NULL)
        {
          for (int32_t i = 0; i < db_count_; ++i)
          {
            delete instance[i];
          }
        }

        delete[] instance;

        if (cache_ != NULL)
        {
          delete cache_;
          cache_ = NULL;
        }

        while (using_head_ != NULL)
        {
          UsingLdbManager* current = using_head_;
          using_head_ = using_head_->next_;
          delete current;
        }

        return TAIR_RETURN_SUCCESS;
      }

      int LdbManager::put(int bucket_number, data_entry& key, data_entry& value, bool version_care, int expire_time)
      {
        log_debug("ldb::put");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->put(bucket_number, key, value, version_care, expire_time);
        }

        return rc;
      }

      int LdbManager::batch_put(int bucket_number, int area, mput_record_vec* record_vec, bool version_care)
      {
        log_debug("ldb::batch_put");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->batch_put(bucket_number, area, record_vec, version_care);
        }

        return rc;
      }

      int LdbManager::get(int bucket_number, data_entry& key, data_entry& value)
      {
        log_debug("ldb::get");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->get(bucket_number, key, value);
        }

        return rc;
      }

      int LdbManager::remove(int bucket_number, data_entry& key, bool version_care)
      {
        log_debug("ldb::remove");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->remove(bucket_number, key, version_care);
        }

        return rc;
      }

      int LdbManager::clear(int32_t area)
      {
        log_debug("ldb::clear %d", area);
        int ret = TAIR_RETURN_SUCCESS;

        bool cmd_clear_area = false;

        for (int32_t i = 0; i < db_count_; ++i)
        {
          if (-10 == area)
          {
            ldb_instance_[i]->gc_factory()->pause_gc();
          }
          else if (-11 == area)
          {
            ldb_instance_[i]->gc_factory()->resume_gc();
          }
          else
          {
            cmd_clear_area = true;
            int tmp_ret = ldb_instance_[i]->clear_area(area);
            if (tmp_ret != TAIR_RETURN_SUCCESS)
            {
              ret = tmp_ret;
              log_error("clear area %d for instance %d fail.", area, i); // just continue
            }
          }
        }

        // clear cache
        if (cmd_clear_area && cache_ != NULL)
        {
          cache_->clear(area);
        }
        return ret;
      }

      bool LdbManager::init_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::init buckets: %d", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        bool ret = false;

        if (1 == db_count_)
        {
          ret = ldb_instance_[0]->init_buckets(buckets);
        }
        else
        {
          std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];

          for (std::vector<int>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            tmp_buckets[bucket_to_instance(*it)].push_back(*it);
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            log_warn("instance %d own %d buckets.", i+1, tmp_buckets[i].size());
            ret = ldb_instance_[i]->init_buckets(tmp_buckets[i]);
            if (!ret)
            {
              log_error("init buckets for db instance %d fail", i + 1);
              break;
            }
          }
          delete[] tmp_buckets;
        }

        return ret;
      }

      void LdbManager::close_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::close buckets: %d", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        if (1 == db_count_)
        {
          ldb_instance_[0]->close_buckets(buckets);
        }
        else
        {
          std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];
          for (std::vector<int>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            tmp_buckets[bucket_to_instance(*it)].push_back(*it);
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            ldb_instance_[i]->close_buckets(tmp_buckets[i]);
          }
          delete[] tmp_buckets;
        }

        // clear mdb cache
        if (cache_ != NULL)
        {
          cache_->close_buckets(buckets);
        }
      }

      // only one bucket can scan at any time ?
      void LdbManager::begin_scan(md_info& info)
      {
        if ((scan_ldb_ = get_db_instance(info.db_id)) == NULL)
        {
          log_error("scan bucket[%u] not exist", info.db_id);
        }
        else
        {
          if (!scan_ldb_->begin_scan(info.db_id))
          {
            log_error("begin scan bucket[%u] fail", info.db_id);
          }
        }
      }

      void LdbManager::end_scan(md_info& info)
      {
        if (scan_ldb_ != NULL)
        {
          scan_ldb_->end_scan();
          scan_ldb_ = NULL;
        }
      }

      bool LdbManager::get_next_items(md_info& info, std::vector<item_data_info*>& list)
      {
        bool ret = true;
        if (NULL == scan_ldb_)
        {
          ret = false;
          log_error("scan bucket not opened");
        }
        else
        {
          ret = scan_ldb_->get_next_items(list);
          log_debug("get items %d", list.size());
        }
        return ret;
      }

      void LdbManager::set_area_quota(int area, uint64_t quota)
      {
        // we consider set area quota to cache
        if (cache_ != NULL)
        {
          cache_->set_area_quota(area, quota);
        }
      }

      void LdbManager::set_area_quota(std::map<int, uint64_t>& quota_map)
      {
        if (cache_ != NULL)
        {
          cache_->set_area_quota(quota_map);
        }
      }

      int LdbManager::op_cmd(ServerCmdType cmd, std::vector<std::string>& params)
      {
        int ret = TAIR_RETURN_FAILED;

        tbsys::CThreadGuard guard(&lock_);

        for (int32_t i = 0; i < db_count_; ++i)
        {
          ret = ldb_instance_[i]->op_cmd(cmd, params);
          if (ret != TAIR_RETURN_SUCCESS)
          {
            break;
          }
        }

        // do something special
        if (ret == TAIR_RETURN_SUCCESS && TAIR_SERVER_CMD_RESET_DB == cmd)
        {
          ret = do_reset_db();
        }

        return ret;
      }

      int LdbManager::do_reset_db()
      {
        int ret = TAIR_RETURN_SUCCESS;
        std::string back_cache_path;
        if (cache_ != NULL)
        {
          // rotate using cache file
          std::string cache_file_path = std::string("/dev/shm/") + mdb_param::mdb_path; // just hard code
          back_cache_path = get_back_path(cache_file_path.c_str());
          if (::rename(cache_file_path.c_str(), back_cache_path.c_str()) != 0)
          {
            log_error("resetdb cache %s to back cache %s fail. error: %s",
                      cache_file_path.c_str(), back_cache_path.c_str(), strerror(errno));
            ret = TAIR_RETURN_FAILED;
          }
        }

        if (TAIR_RETURN_SUCCESS == ret)
        {
          // init new ldb cache
          mdb_manager* new_cache = NULL;
          if (TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_CACHE, 1) > 0)
          {
            log_info("reinit new cache");
            new_cache = dynamic_cast<tair::mdb_manager*>(mdb_factory::create_embedded_mdb());
            if (NULL == new_cache)
            {
              log_error("reinit ldb memory cache fail.");
              ret = TAIR_RETURN_FAILED;
            }
          }

          if (TAIR_RETURN_SUCCESS == ret)
          {
            // init new ldb instance
            // get orignal buckets deployment
            log_info("reinit ldb instance");
            std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];
            for (int32_t i = 0; i < db_count_; ++i)
            {
              ldb_instance_[i]->get_buckets(tmp_buckets[i]);
            }

            bool db_version_care = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_VERSION_CARE, 1) > 0;
            LdbInstance** new_ldb_instance = new LdbInstance*[db_count_];
            for (int32_t i = 0; i < db_count_; ++i)
            {
              new_ldb_instance[i] = new LdbInstance(i + 1, db_version_care, new_cache);
              log_debug("reinit instance %d own %d buckets.", i+1, tmp_buckets[i].size());
              if (!new_ldb_instance[i]->init_buckets(tmp_buckets[i]))
              {
                log_error("resetdb reinit db fail. instance: %d", i+1);
                ret = TAIR_RETURN_FAILED;
                break;
              }
            }

            if (TAIR_RETURN_SUCCESS == ret)
            {
              UsingLdbManager* new_using_mgr = new UsingLdbManager;
              new_using_mgr->ldb_instance_ = ldb_instance_;
              new_using_mgr->db_count_ = db_count_;
              new_using_mgr->cache_ = cache_;
              new_using_mgr->cache_file_path_ = back_cache_path;
              new_using_mgr->time_ = time(NULL);

              // switch
              log_warn("ldb resetdb now");
              cache_ = new_cache;
              ldb_instance_ = new_ldb_instance;

              if (using_head_ != NULL)
              {
                using_tail_->next_ = new_using_mgr;
                using_tail_ = new_using_mgr;
              }
              else
              {
                using_head_ = using_tail_ = new_using_mgr;
              }
            }
            delete[] tmp_buckets;
          }
        }

        return ret;
      }

      void LdbManager::get_stats(tair_stat* stat)
      {
        tbsys::CThreadGuard guard(&lock_);
        log_debug("ldbmanager get stats");

        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i]->get_stats(stat);
        }
        if (cache_ != NULL)
        {
          // lock hold, static is ok.
          static cache_stat ldb_cache_stat[TAIR_MAX_AREA_COUNT];
          cache_->raw_get_stats(ldb_cache_stat);
          cache_stat_.save(ldb_cache_stat, TAIR_MAX_AREA_COUNT);
        }

        // clear using manager
        if (using_head_ != NULL)
        {
          if (using_head_->can_destroy())
          {
            UsingLdbManager* current = using_head_;
            using_head_ = using_head_->next_;
            if (using_head_ == NULL)
            {
              using_tail_ = NULL;
            }
            delete current;
          }
        }
      }

      void LdbManager::set_bucket_count(uint32_t bucket_count)
      {
        if (this->bucket_count == 0)
        {
          this->bucket_count = bucket_count;
          if (cache_ != NULL)
          {
            // set cache (mdb_manager) bucket count
            cache_->set_bucket_count(bucket_count);
          }
        }
      }

      // integer hash function
      int LdbManager::hash(int h)
      {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >> 10);
        h += (h << 3);
        h ^= (h >> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >> 16);
      }

      LdbInstance* LdbManager::get_db_instance(const int bucket_number)
      {
        LdbInstance* ret = NULL;
        if (NULL != ldb_instance_)
        {
          ret = (1 == db_count_) ? ldb_instance_[0] :
            ldb_instance_[bucket_to_instance(bucket_number)];
          assert(ret != NULL);
          if (!ret->exist(bucket_number))
          {
            ret = NULL;
          }
        }
        return ret;
      }

      int LdbManager::bucket_to_instance(int bucket_number)
      {
        return hash(bucket_number) % db_count_;
      }
    }
  }
}
