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

#ifndef TAIR_STORAGE_LDB_MANAGER_H
#define TAIR_STORAGE_LDB_MANAGER_H

#include <boost/dynamic_bitset.hpp>

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbBucket;
      // for test single or multi leveldb instance
      class LdbInstance
      {
      public:
        LdbInstance(){}
        virtual ~LdbInstance(){}

        virtual bool init_buckets(const std::vector <int>& buckets) = 0;
        virtual void close_buckets(const std::vector <int>& buckets) = 0;
        virtual LdbBucket* get_bucket(const int bucket_number) = 0;
        virtual void get_stats(tair_stat* stat) = 0;
        virtual int clear(int32_t area) = 0;
      };

      class SingleLdbInstance : public LdbInstance
      {
      public:
        SingleLdbInstance(storage::storage_manager* cache);
        virtual ~SingleLdbInstance();

        virtual bool init_buckets(const std::vector <int>& buckets);
        virtual void close_buckets(const std::vector <int>& buckets);
        virtual LdbBucket* get_bucket(const int bucket_number);
        virtual void get_stats(tair_stat* stat);
        virtual int clear(int32_t area);

      private:
        LdbBucket* ldb_bucket_;
        boost::dynamic_bitset<> bucket_set_;
      };

      class MultiLdbInstance : public LdbInstance
      {
      public:
        typedef __gnu_cxx::hash_map <int, LdbBucket*> LDB_BUCKETS_MAP;

        MultiLdbInstance(storage_manager* cache);
        virtual ~MultiLdbInstance();

        virtual bool init_buckets(const std::vector <int>& buckets);
        virtual void close_buckets(const std::vector <int>& buckets);
        virtual LdbBucket* get_bucket(const int bucket_number);
        virtual void get_stats(tair_stat* stat);
        virtual int clear(int32_t area);

      private:
        LDB_BUCKETS_MAP* buckets_map_;
        storage_manager* cache_;
      };

      // just close to kdb_manager.
      // manager hold buckets. single or multi leveldb instance based on config for test 
      // it works, maybe make it common manager level.
      class LdbManager : public tair::storage::storage_manager
      {
      public:
        LdbManager();
        ~LdbManager();

        int put(int bucket_number, data_entry& key, data_entry& value,
                bool version_care, int expire_time);
        int get(int bucket_number, data_entry& key, data_entry& value);
        int remove(int bucket_number, data_entry& key, bool version_care);
        int clear(int area);

        bool init_buckets(const std::vector <int>& buckets);
        void close_buckets(const std::vector <int>& buckets);

        void begin_scan(md_info& info);
        void end_scan(md_info& info);
        bool get_next_items(md_info& info, std::vector <item_data_info *>& list);

        void set_area_quota(int area, uint64_t quota);
        void set_area_quota(std::map<int, uint64_t>& quota_map);

        void get_stats(tair_stat* stat);

      private:
        LdbInstance* ldb_instance_;
        storage_manager* cache_;
        LdbBucket* scan_ldb_;
        tbsys::CThreadMutex lock_;
      };
    }
  }
}

#endif
