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

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)
#include <unordered_map>
#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"
#include "ldb_cache_stat.hpp"
#include "ldb_remote_sync_logger.hpp"

namespace tair
{
  class operation_record;
  class mdb_manager;
  namespace storage
  {
    namespace ldb
    {
      class LdbInstance;
      class LdbBalancer;

      ////////////////////////////////
      // UsingLdbManager
      ////////////////////////////////
      static const int32_t DESTROY_LDB_MANGER_INTERVAL_S = 30; // 30s
      static const int32_t FLUSH_LDB_MEM_INTERVAL_S = 60;      // 60s
      class UsingLdbManager
      {
      public:
        UsingLdbManager();
        ~UsingLdbManager();

        bool can_destroy();
        void destroy();

      public:
        LdbInstance** ldb_instance_;
        int32_t db_count_;
        mdb_manager** cache_;
        int32_t cache_count_;
        std::string base_cache_path_;
        UsingLdbManager* next_;
        uint32_t time_;
      };

      // Bucket partition indexer
      class BucketIndexer
      {
      public:
        BucketIndexer() {};
        virtual ~BucketIndexer() {};


        // bucket => instance index
        typedef std::unordered_map<int32_t, int32_t> BUCKET_INDEX_MAP;
        // instance index => buckets
        typedef std::vector<std::vector<int32_t> > INDEX_BUCKET_MAP;

        virtual int sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                    INDEX_BUCKET_MAP& index_map, bool& updated, bool close = false) = 0;
        virtual int32_t bucket_to_index(int32_t bucket_number, bool& recheck) = 0;
        virtual int reindex(int32_t bucket, int32_t from, int32_t to) = 0;
        virtual void get_index_map(INDEX_BUCKET_MAP& result) = 0;

        static void get_index_map(const BUCKET_INDEX_MAP& bucket_map, int32_t index_count, INDEX_BUCKET_MAP& index_map);
        static std::string to_string(const INDEX_BUCKET_MAP& index_map);
        static std::string to_string(int32_t index_count, const BUCKET_INDEX_MAP& bucket_map);
        static BucketIndexer* new_bucket_indexer(const char* strategy);
      };

      // bucket hash partition indexer
      class HashBucketIndexer : public BucketIndexer
      {
      public:
        HashBucketIndexer();
        virtual ~HashBucketIndexer();

        virtual int sharding_bucket(int32_t total, const std::vector<int>& buckets,
                                    INDEX_BUCKET_MAP& index_map, bool& updated, bool close = false);
        virtual int32_t bucket_to_index(int32_t bucket_number, bool& recheck);
        virtual int reindex(int32_t bucket, int32_t from, int32_t to);
        virtual void get_index_map(INDEX_BUCKET_MAP& result);

      private:
        int hash(int32_t key);

      private:
        int32_t total_;
      };

      // bucket common map partition indexer
      static const char BUCKET_INDEX_DELIM = ':';
      class MapBucketIndexer : public BucketIndexer
      {
      public:
        MapBucketIndexer();
        virtual ~MapBucketIndexer();

        virtual int sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                    INDEX_BUCKET_MAP& index_map, bool& updated, bool close = false);
        virtual int32_t bucket_to_index(int32_t bucket_number, bool& recheck);
        virtual int reindex(int32_t bucket, int32_t from, int32_t to);
        virtual void get_index_map(INDEX_BUCKET_MAP& result);

      private:
        int close_sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                  INDEX_BUCKET_MAP& index_map, bool& updated);
        int do_sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                               INDEX_BUCKET_MAP& index_map, bool& updated);

        int update_bucket_index(int32_t total, BUCKET_INDEX_MAP* new_bucket_map);
        int save_bucket_index(int32_t total, BUCKET_INDEX_MAP& bucket_index_map);
        int load_bucket_index();

      private:
        char bucket_index_file_path_[TAIR_MAX_PATH_LEN];
        BUCKET_INDEX_MAP* bucket_map_;
        int32_t total_;
        bool can_update_;
      };



      // manager hold buckets. single or multi leveldb instance based on config for test
      // it works, maybe make it common manager level.
      class LdbManager : public tair::storage::storage_manager
      {
        friend class LdbRemoteSyncLogger;
      public:
        LdbManager();
        ~LdbManager();

        int put(int bucket_number, data_entry& key, data_entry& value,
                bool version_care, int expire_time);
        int direct_mupdate(int bucket_number, const std::vector<operation_record*>& kvs);
        int batch_put(int bucket_number, int area, mput_record_vec* record_vec, bool version_care);
        int get(int bucket_number, data_entry& key, data_entry& value, bool stat);
        int remove(int bucket_number, data_entry& key, bool version_care);
        int clear(int area);

        int get_range(int bucket_number, data_entry& key_start, data_entry& end_key, int offset, int limit, int type, std::vector<data_entry*>& result, bool &has_next);
        int del_range(int bucket_number, data_entry& key_start, data_entry& end_key, int offset, int limit, int type, std::vector<data_entry*>& result, bool &has_next);

        bool init_buckets(const std::vector <int>& buckets);
        void close_buckets(const std::vector <int>& buckets);

        void begin_scan(md_info& info);
        void end_scan(md_info& info);
        bool get_next_items(md_info& info, std::vector <item_data_info *>& list);

        void set_area_quota(int area, uint64_t quota);
        void set_area_quota(std::map<int, uint64_t>& quota_map);

        int op_cmd(ServerCmdType cmd, std::vector<std::string>& params);

        void get_stats(tair_stat* stat);
        void set_bucket_count(uint32_t bucket_count);

        // get bucket index map
        void get_index_map(BucketIndexer::INDEX_BUCKET_MAP& result);
        // reindex bucket from `from instance to `to instance
        int reindex_bucket(int32_t bucket, int32_t from, int32_t to);
        // pause service(mainly write) for the bucket
        void pause_service(int32_t bucket);
        void resume_service(int32_t bucket);

        LdbInstance* get_instance(int32_t index);

      private:
        int init();
        int init_cache(mdb_manager**& new_cache, int32_t count);
        int destroy();

        int do_reset_db();
        int do_set_migrate_wait(int32_t cmd_wait_ms);
        int do_release_mem();
        void maybe_exec_cmd();

        int do_switch_db(LdbInstance** new_ldb_instance, mdb_manager** new_cache, std::string& base_cache_back_path);
        int do_reset_db_instance(LdbInstance**& new_ldb_instance, mdb_manager** new_cache);
        int do_reset_cache(mdb_manager**& new_cache, std::string& base_back_path);
        RecordLogger* get_remote_sync_logger();

        LdbInstance* get_db_instance(int bucket_number, bool write = true);

      private:
        LdbInstance** ldb_instance_;
        int32_t db_count_;
        BucketIndexer* bucket_indexer_;

        // out-of-servicing bucket(maybe paused for a while)
        // only support one now.
        // TODO: maybe hashmap to support multiple ones
        int32_t frozen_bucket_;

        bool use_bloomfilter_;
        mdb_manager** cache_;
        int32_t cache_count_;
        // cache stat
        CacheStat cache_stat_;
        LdbInstance* scan_ldb_;
        tbsys::CThreadMutex lock_;
        uint32_t migrate_wait_us_;
        UsingLdbManager* using_head_;
        UsingLdbManager* using_tail_;
        uint32_t last_release_time_;
        // remote sync
        LdbRemoteSyncLogger* remote_sync_logger_;
        // balancer
        LdbBalancer* balancer_;
      };
    }
  }
}

#endif
