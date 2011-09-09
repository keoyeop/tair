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

#include "ldb_manager.hpp"
#include "ldb_bucket.hpp"

namespace tair
{
  namespace storage 
  {
    namespace ldb
    {
      SingleLdbInstance::SingleLdbInstance()
      {
        ldb_bucket_ = new LdbBucket();
        bucket_set_.resize(10240, false);
      }

      SingleLdbInstance::~SingleLdbInstance()
      {
        delete ldb_bucket_;
        ldb_bucket_ = NULL;
      }

      bool SingleLdbInstance::init_buckets(const std::vector <int>& buckets)
      {
        bool ret = true;
        int bucket_number = 0;

        if (!buckets.empty())
        {
          if (!(ret = ldb_bucket_->start(buckets[0])))
          {
            log_error("init single ldb failed");
          }
          else
          {
            for (size_t i = 0; i < buckets.size(); i++)
            {
              bucket_number = buckets[i];
              // dynamic resize
              if (bucket_number > static_cast<int>(bucket_set_.size()))
              {
                bucket_set_.resize(bucket_number, false);
              }
              if (bucket_set_.test(bucket_number))
              {
                log_info("bucket [%d] already exist", bucket_number);
              }
              else
              {
                bucket_set_.set(bucket_number);
              }
            }
          }
        }

        return ret;
      }

      void SingleLdbInstance::close_buckets(const std::vector <int>& buckets)
      {
        int bucket_number = 0;
        for (size_t i = 0; i< buckets.size(); i++)
        {
          bucket_number = buckets[i];
          if (bucket_set_.test(bucket_number))
          {
            bucket_set_.reset(bucket_number);
            // TODO: destroy specified bucket
          }
          else
          {
            log_info("bucket [%d] not exist", bucket_number);
          }
        }
      }

      LdbBucket* SingleLdbInstance::get_bucket(const int bucket_number)
      {
        LdbBucket* ret = NULL;
        if (bucket_set_.test(bucket_number))
        {
          ret = ldb_bucket_;
        }
        return ret;
      }

      // MultiLdbInstance
      MultiLdbInstance::MultiLdbInstance()
      {
        buckets_map_ = new LDB_BUCKETS_MAP();
      }

      MultiLdbInstance::~MultiLdbInstance()
      {
        LDB_BUCKETS_MAP::iterator it;
        for (it = buckets_map_->begin(); it != buckets_map_->end(); ++it)
        {
          delete it->second;
        }

        delete buckets_map_;
      }

      bool MultiLdbInstance::init_buckets(const std::vector <int>& buckets)
      {
        LDB_BUCKETS_MAP* temp_buckets_map = new LDB_BUCKETS_MAP(*buckets_map_);
        bool ret = true;

        for (size_t i = 0; i < buckets.size(); i++)
        {
          int bucket_number = buckets[i];
          LdbBucket* bucket = get_bucket(bucket_number);
          if (bucket != NULL)
          {
            log_info("bucket [%d] already exist", bucket_number);
            continue;
          }

          LdbBucket* new_bucket = new LdbBucket();
          if (!(ret = new_bucket->start(bucket_number)))
          {
            log_error("init bucket[%d] failed", bucket_number);
            delete new_bucket;
            break;
          }

          (*temp_buckets_map)[bucket_number] = new_bucket;
        }

        if (ret)
        {
          LDB_BUCKETS_MAP* old_bucket_map = buckets_map_;
          buckets_map_ = temp_buckets_map;
          usleep(100);
          delete old_bucket_map;
        }

        return ret;
      }

      void MultiLdbInstance::close_buckets(const std::vector <int>& buckets)
      {
        LDB_BUCKETS_MAP* temp_buckets_map = new LDB_BUCKETS_MAP(*buckets_map_);
        std::vector<LdbBucket*> rm_buckets;

        for (size_t i = 0; i < buckets.size(); ++i)
        {
          int bucket_number = buckets[i];
          LdbBucket* bucket = get_bucket(bucket_number);
          if (bucket == NULL)
          {
            log_info("bucket [%d] not exist", bucket_number);
            continue;
          }

          temp_buckets_map->erase(bucket_number);
          rm_buckets.push_back(bucket);
        }

        LDB_BUCKETS_MAP* old_buckets_map = buckets_map_;
        buckets_map_ = temp_buckets_map;
        usleep(1000);
        for (size_t i = 0; i < rm_buckets.size(); ++i)
        {
          rm_buckets[i]->destory();
          delete rm_buckets[i];
        }

        delete old_buckets_map;
      }

      LdbBucket* MultiLdbInstance::get_bucket(const int bucket_number)
      {
        log_debug("get bucket %d", bucket_number);

        LdbBucket* ret = NULL;

        LDB_BUCKETS_MAP::iterator it = buckets_map_->find(bucket_number);
        if (it != buckets_map_->end())
        {
          ret = it->second;
        }

        return ret;
      }

      //////////// LdbManager
      LdbManager::LdbManager()
      {
        // for test which one is better
        int strategy = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_INIT_STRATEGY, 0);
        if (strategy != 0)
        {
          ldb_instance_ = new SingleLdbInstance();
        }
        else
        {
          ldb_instance_ = new MultiLdbInstance();
        }
        log_debug("ldb storage engine construct %d", strategy);
      }

      LdbManager::~LdbManager()
      {
        delete ldb_instance_;
      }

      int LdbManager::put(int bucket_number, data_entry& key, data_entry& value, bool version_care, int expire_time)
      {
        log_debug("ldb::put");
        int rc = TAIR_RETURN_SUCCESS;
        LdbBucket* bucket = ldb_instance_->get_bucket(bucket_number);

        if (bucket == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = bucket->put(key, value, version_care, expire_time);  
        }

        return rc;
      }

      int LdbManager::get(int bucket_number, data_entry& key, data_entry& value)
      {
        log_debug("ldb::get");
        int rc = TAIR_RETURN_SUCCESS;
        LdbBucket* bucket = ldb_instance_->get_bucket(bucket_number);

        if (bucket == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = bucket->get(key, value);
        }

        return rc;
      }

      int LdbManager::remove(int bucket_number, data_entry& key, bool version_care)
      {
        log_debug("ldb::remove");
        int rc = TAIR_RETURN_SUCCESS;
        LdbBucket* bucket = ldb_instance_->get_bucket(bucket_number);

        if (bucket == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = bucket->remove(key, version_care);
        }

        return rc;
      }

      int LdbManager::clear(int area)
      {
        log_debug("ldb::clear");
        return 0;
      }

      bool LdbManager::init_buckets(const std::vector<int>& buckets)
      {
        log_debug("ldb::init buckets");
        tbsys::CThreadGuard guard(&lock_);        
        return ldb_instance_->init_buckets(buckets);
      }

      void LdbManager::close_buckets(const std::vector<int>& buckets)
      {
        log_debug("ldb::close buckets");
        tbsys::CThreadGuard guard(&lock_);
        return ldb_instance_->close_buckets(buckets);
      }

      void LdbManager::begin_scan(md_info& info)
      {
          
      }

      void LdbManager::end_scan(md_info& info)
      {
          
      }

      bool LdbManager::get_next_items(md_info& info, std::vector <item_data_info *>& list)
      {
        return true;          
      }

      void LdbManager::set_area_quota(int area, uint64_t quota)
      {
      }

      void LdbManager::set_area_quota(std::map<int, uint64_t>& quota_map)
      {
      }

      void LdbManager::get_stats(tair_stat* stat)
      {
          
      }
    }
  }
}
