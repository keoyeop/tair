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

#include <malloc.h>
#ifdef WITH_TCMALLOC
#include <google/malloc_extension.h>
#endif

#include "packets/mupdate_packet.hpp"
#include "storage/mdb/mdb_factory.hpp"
#include "storage/mdb/mdb_manager.hpp"
#include "ldb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_balancer.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      ////////////////////////////////
      // UsingLdbManager
      ////////////////////////////////
      UsingLdbManager::UsingLdbManager() :
        ldb_instance_(NULL), db_count_(0), cache_(NULL), cache_count_(0), base_cache_path_(""), next_(NULL), time_(0)
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

        destroy_container(ldb_instance_, db_count_);

        if (cache_ != NULL)
        {
          for (int32_t i = 0; i < cache_count_; ++i)
          {
            std::string cache_path = append_num(base_cache_path_, i);
            if (::shm_unlink(cache_path.c_str()) != 0)
            {
              log_error("unlink cache file fail: %s, error: %s", cache_path.c_str(), strerror(errno));
            }
          }

          destroy_container(cache_, cache_count_);
        }
      }

      ////////////////////////////////
      // HashBucketIndexer
      ////////////////////////////////
      static const char* BI_STRATEGY_MAP = "map";
      static const char* BI_STRATEGY_HASH = "hash";

      //////////////////// BucketIndexer
      BucketIndexer* BucketIndexer::new_bucket_indexer(const char* strategy)
      {
        BucketIndexer* indexer = NULL;
        if (NULL == strategy || strncmp(strategy, BI_STRATEGY_HASH, sizeof(BI_STRATEGY_HASH)-1) == 0)
        {
          indexer = new HashBucketIndexer();
        }
        else if (strncmp(strategy, BI_STRATEGY_MAP, sizeof(BI_STRATEGY_MAP)-1) == 0)
        {
          indexer = new MapBucketIndexer();
        }
        else
        {
          log_warn("unsupported bucket index strategy: %s, use default hash", strategy);
          indexer = new HashBucketIndexer();
        }
        return indexer;
      }

      void BucketIndexer::get_index_map(const BUCKET_INDEX_MAP& bucket_map, int32_t index_count, INDEX_BUCKET_MAP& index_map)
      {
        index_map.resize(index_count);
        for (BUCKET_INDEX_MAP::const_iterator it = bucket_map.begin(); it != bucket_map.end(); ++it)
        {
          index_map[it->second].push_back(it->first);
        }
      }

      std::string BucketIndexer::to_string(const INDEX_BUCKET_MAP& index_map)
      {
        std::string result("\n");
        char buf[16];
        for (size_t i = 0; i < index_map.size(); ++i)
        {
          snprintf(buf, sizeof(buf), "[%lu (%lu):", i, index_map[i].size());
          result.append(buf);
          std::vector<int32_t> buckets = index_map[i];
          // for friendly output-print
          std::sort(buckets.begin(), buckets.end());
          for (std::vector<int32_t>::iterator bit = buckets.begin();
               bit != buckets.end();
               ++bit)
          {
            snprintf(buf, sizeof(buf), " %d", *bit);
            result.append(buf);
          }
          result.append("]\n");
        }
        return result;
      }

      std::string BucketIndexer::to_string(int32_t index_count, const BUCKET_INDEX_MAP& bucket_map)
      {
        std::string result;
        result.reserve(bucket_map.size() * 16);
        char buf[16];
        snprintf(buf, sizeof(buf), "%d\n", index_count);
        result.append(buf);
        for (BUCKET_INDEX_MAP::const_iterator it = bucket_map.begin(); it != bucket_map.end(); ++it)
        {
          snprintf(buf, sizeof(buf), "%d%c%d\n", it->first, BUCKET_INDEX_DELIM, it->second);
          result.append(buf);
        }
        return result;
      }

      //////////////////// HashBucketIndexer
      HashBucketIndexer::HashBucketIndexer() : total_(0)
      {
      }

      HashBucketIndexer::~HashBucketIndexer()
      {
      }

      // integer hash function
      int HashBucketIndexer::hash(int h)
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

      int HashBucketIndexer::sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                             INDEX_BUCKET_MAP& index_map, bool& updated, bool close)
      {
        UNUSED(close);
        if (total <= 0)
        {
          log_error("sharing bucket fail. total count is invalid: %d", total);
          return TAIR_RETURN_FAILED;
        }

        total_ = total;
        updated = false;
        index_map.resize(total_);
        for (std::vector<int32_t>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
        {
          index_map[hash(*it) % total_].push_back(*it);
        }
        return TAIR_RETURN_SUCCESS;
      }

      int32_t HashBucketIndexer::bucket_to_index(int32_t bucket_number, bool& recheck)
      {
        // need recheck to make sure bucket exist
        recheck = true;
        return total_ <= 0 ? -1 : hash(bucket_number) % total_;
      }

      int HashBucketIndexer::reindex(int32_t bucket, int32_t from, int32_t to)
      {
        // hash bucket can't reindex
        return TAIR_RETURN_FAILED;
      }

      void HashBucketIndexer::get_index_map(INDEX_BUCKET_MAP& result)
      {
        // no bucket information
      }


      ////////////////////////////////
      // MapBucketIndexer
      ////////////////////////////////
      MapBucketIndexer::MapBucketIndexer() : bucket_map_(new BUCKET_INDEX_MAP()), total_(0)
      {
        const char* bucket_index_file_dir = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_BUCKET_INDEX_FILE_DIR, "data/bindex");
        snprintf(bucket_index_file_path_, sizeof(bucket_index_file_path_),
                 "%s/ldb_bucket_index_map", bucket_index_file_dir);
        if (!tbsys::CFileUtil::mkdirs(const_cast<char*>(std::string(bucket_index_file_dir).c_str())))
        {
          log_error("mkdir bucket index file dir fail: %s", bucket_index_file_dir);
        }
        can_update_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_BUCKET_INDEX_CAN_UPDATE, 1) != 0;
        load_bucket_index();
      }

      MapBucketIndexer::~MapBucketIndexer()
      {
        delete bucket_map_;
        bucket_map_ = NULL;
      }

      int MapBucketIndexer::sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                            INDEX_BUCKET_MAP& index_map, bool& updated, bool close)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (close)
        {
          ret = close_sharding_bucket(total, buckets, index_map, updated);
        }
        else
        {
          ret = do_sharding_bucket(total, buckets, index_map, updated);
        }
        return ret;
      }

      int32_t MapBucketIndexer::bucket_to_index(int32_t bucket_number, bool& recheck)
      {
        // no need recheck
        recheck = false;
        BUCKET_INDEX_MAP* bm = bucket_map_;
        BUCKET_INDEX_MAP::const_iterator it = bm->find(bucket_number);
        return (LIKELY(it != bm->end()) ? it->second : -1);
      }

      int MapBucketIndexer::reindex(int32_t bucket, int32_t from, int32_t to)
      {
        int ret = TAIR_RETURN_SUCCESS;
        BUCKET_INDEX_MAP* new_bucket_map = new BUCKET_INDEX_MAP(*bucket_map_);
        BUCKET_INDEX_MAP::iterator it = new_bucket_map->find(bucket);
        if (it == new_bucket_map->end())
        {
          log_error("reindex bucket %d but not exist", bucket);
          ret = TAIR_RETURN_FAILED;
        }
        else if (it->second != from)
        {
          log_error("reindex bucket %d but original conflict from: %d <=> %d",
                    bucket, it->second, from);
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          it->second = to;
          log_warn("reindex bucket %d: %d => %d", bucket, from, to);
          ret = update_bucket_index(total_, new_bucket_map);
        }

        return ret;
      }

      void MapBucketIndexer::get_index_map(INDEX_BUCKET_MAP& result)
      {
        BUCKET_INDEX_MAP bm(*bucket_map_);
        BucketIndexer::get_index_map(bm, total_, result);
      }

      int MapBucketIndexer::close_sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                                  INDEX_BUCKET_MAP& index_map, bool& updated)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (total != total_)
        {
          log_error("close buckets but index total changed %d <> %d", total, total_);
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          index_map.resize(total);
          BUCKET_INDEX_MAP* new_bucket_map = new BUCKET_INDEX_MAP(*bucket_map_);
          for (std::vector<int32_t>::const_iterator it = buckets.begin(); it != buckets.end(); ++it)
          {
            BUCKET_INDEX_MAP::iterator mit = new_bucket_map->find(*it);
            if (new_bucket_map->end() == mit)
            {
              log_error("close bucket %d but not exist", *it);
            }
            else
            {
              index_map[mit->second].push_back(mit->first);
              new_bucket_map->erase(mit);
            }
          }

          updated = (new_bucket_map->size() != bucket_map_->size());
          // update
          if (updated)
          {
            INDEX_BUCKET_MAP new_index_map;
            BucketIndexer::get_index_map(*new_bucket_map, total, new_index_map);
            log_warn("new bucket index: %s", BucketIndexer::to_string(new_index_map).c_str());
            ret = update_bucket_index(total, new_bucket_map);
          }
          else
          {
            delete new_bucket_map;
          }
        }

        return ret;
      }

      int MapBucketIndexer::do_sharding_bucket(int32_t total, const std::vector<int32_t>& buckets,
                                               INDEX_BUCKET_MAP& index_map, bool& updated)
      {
        if (!can_update_ && total == total_)
        {
          log_warn("map bucket indexer config not update.");
          BucketIndexer::get_index_map(*bucket_map_, total, index_map);
          return TAIR_RETURN_SUCCESS;
        }

        // LdbBalancer will do real balance now(moving data etc.),
        // so we just reserve original sharding status,
        // and sharding NEW buckets as even as possible.
        BUCKET_INDEX_MAP* new_bucket_map = new BUCKET_INDEX_MAP();
        std::vector<int32_t> new_buckets;

        for (std::vector<int32_t>::const_iterator bit = buckets.begin(); bit != buckets.end(); ++bit)
        {
          BUCKET_INDEX_MAP::iterator mit = bucket_map_->find(*bit);
          if (mit == bucket_map_->end())
          {
            new_buckets.push_back(*bit);
          }
          else
          {
            // reserve old map relation
            (*new_bucket_map)[mit->first] = mit->second;
          }
        }

        updated = !new_buckets.empty();
        if (!updated)
        {
          // no new buckets, no sharding
          BucketIndexer::get_index_map(*new_bucket_map, total, index_map);
          delete new_bucket_map;
          return TAIR_RETURN_SUCCESS;
        }

        std::random_shuffle(new_buckets.begin(), new_buckets.end());

        // To sharding as even as possible, we continue kicking out
        // rich ones whose owing is large than current average until
        // the remainings are really crying with hungry.
        INDEX_BUCKET_MAP new_index_map;
        BucketIndexer::get_index_map(*new_bucket_map, total, new_index_map);

        // list is friendly to erase()
        std::list<INDEX_BUCKET_MAP::iterator> hungrys;
        int32_t hungrys_size = 0;
        for (INDEX_BUCKET_MAP::iterator it = new_index_map.begin(); it != new_index_map.end(); ++it)
        {
          hungrys.push_back(it);
          hungrys_size++;
        }

        // first, get real hungry ones
        int32_t bucket_count = buckets.size();
        int32_t average = 0, remainder = 0;
        int32_t kicks = 0;
        do
        {
          // recalculate
          kicks = 0;
          average = bucket_count / hungrys_size;
          remainder = bucket_count % hungrys_size;

          for (std::list<INDEX_BUCKET_MAP::iterator>::iterator it = hungrys.begin();
               it != hungrys.end();)
          {
            int32_t owning = (*it)->size();
            // the formers have priority to get remainder(if exist)
            if (owning > average || (owning == average && remainder <= 0))
            {
              // this is a rich buddy, kick out
              it = hungrys.erase(it);
              --hungrys_size;
              bucket_count -= owning;
              kicks++;
            }
            else
            {
              ++it;
              if (owning == average && remainder > 0)
              {
                --remainder;
              }
            }
          }
        } while (kicks > 0);

        // now, we will feed hungrys
        average = bucket_count / hungrys_size;
        remainder = bucket_count % hungrys_size;

        std::vector<int32_t>::iterator bit = new_buckets.begin();

        for (std::list<INDEX_BUCKET_MAP::iterator>::iterator it = hungrys.begin();
             it != hungrys.end();
             ++it)
        {
          int32_t less = average - (*it)->size();
          if (less >= 0)
          {
            if (remainder > 0)
            {
              less++;
              remainder--;
            }

            if (less > 0)
            {
              // have good dinner
              for (int32_t i = 0; i < less; ++i, ++bit)
              {
                (*it)->push_back(*bit);
              }
            }
          }
        }

        // get bucket map
        for (size_t i = 0; i < new_index_map.size(); ++i)
        {
          for (std::vector<int32_t>::iterator vit = new_index_map[i].begin(); vit != new_index_map[i].end(); ++vit)
          {
            (*new_bucket_map)[*vit] = i;
          }
        }

        index_map = new_index_map;
        log_warn("new bucket index: %s", BucketIndexer::to_string(index_map).c_str());

        // bucket map should reserve last buckets,
        // close_sharding_bucket() will do real erasing work later.
        for (BUCKET_INDEX_MAP::iterator it = bucket_map_->begin(); it != bucket_map_->end(); ++it)
        {
          if (new_bucket_map->find(it->first) == new_bucket_map->end())
          {
            (*new_bucket_map)[it->first] = it->second;
          }
        }

        return update_bucket_index(total, new_bucket_map);
      }

      int MapBucketIndexer::update_bucket_index(int32_t total, BUCKET_INDEX_MAP* new_bucket_map)
      {
        int ret = save_bucket_index(total, *new_bucket_map);
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("save bucket index map fail, ret: %d", ret);
          delete new_bucket_map;
        }
        else
        {
          BUCKET_INDEX_MAP* old_bucket_map = bucket_map_;
          bucket_map_ = new_bucket_map;
          total_ = total;
          if (old_bucket_map != NULL)
          {
            ::usleep(20);
            delete old_bucket_map;
          }
        }
        return ret;
      }

      int MapBucketIndexer::save_bucket_index(int32_t total, BUCKET_INDEX_MAP& bucket_index_map)
      {
        int ret = TAIR_RETURN_SUCCESS;
        char tmp_file_name[TAIR_MAX_PATH_LEN];
        snprintf(tmp_file_name, sizeof(tmp_file_name), "%s.tmp", bucket_index_file_path_);
        FILE* new_file = ::fopen(tmp_file_name, "w");
        if (NULL == new_file)
        {
          log_error("open new bucket index file %s fail, error: %s", tmp_file_name, strerror(errno));
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          if (::access(bucket_index_file_path_, F_OK) == 0)
          {
            // backup old
            char backup_file_name[TAIR_MAX_PATH_LEN + 16];
            int len = snprintf(backup_file_name, sizeof(backup_file_name), "%s.", bucket_index_file_path_);
            tbsys::CTimeUtil::timeToStr(time(NULL), backup_file_name + len);
            if (::rename(bucket_index_file_path_, backup_file_name) != 0)
            {
              log_error("backup index file fail: %s", strerror(errno));
              // ignore fail
            }
          }

          std::string index_str = BucketIndexer::to_string(total, bucket_index_map);
          fprintf(new_file, "%s", index_str.c_str());

          if (::rename(tmp_file_name, bucket_index_file_path_) != 0)
          {
            log_error("change new bucket index file fail %s => %s, error: %s",
                      tmp_file_name, bucket_index_file_path_, strerror(errno));
            ret = TAIR_RETURN_FAILED;
          }
          else
          {
            ::fflush(new_file);
          }

          ::fclose(new_file);
        }
        return ret;
      }

      int MapBucketIndexer::load_bucket_index()
      {
        FILE* index_file = ::fopen(bucket_index_file_path_, "r");
        // bucket index file exist
        if (index_file != NULL)
        {
          log_info("bucket index file exist, load it: %s", bucket_index_file_path_);
          BUCKET_INDEX_MAP* new_bucket_map = new BUCKET_INDEX_MAP();
          int32_t total = 0;
          char buf[64];
          int32_t len = 0;
          char* pos = NULL;
          bool is_first = true;
          while (::fgets(buf, sizeof(buf), index_file) != NULL)
          {
            len = strlen(buf);
            if (len <= 1)
            {
              log_warn("invalid line in bucket index file: %s, ignore", buf);
              continue;
            }
            if ('\n' == buf[len-1])
            {
              buf[len-1] = '\0';
              --len;
            }
            if (is_first)
            {
              total = atoi(buf);
              is_first = false;
            }
            else
            {
              pos = strchr(buf, BUCKET_INDEX_DELIM);
              if (NULL == pos || '\0' == *(pos+1))
              {
                log_warn("invalid line in bucket index file: %s, ignore", buf);
                continue;
              }
              *pos = '\0';
              (*new_bucket_map)[atoi(buf)] = atoi(pos+1);
            }
          }

          ::fclose(index_file);
          BUCKET_INDEX_MAP* old_bucket_map = bucket_map_;
          bucket_map_ = new_bucket_map;
          total_ = total;
          if (old_bucket_map != NULL)
          {
            ::usleep(10);
            delete old_bucket_map;
          }
          INDEX_BUCKET_MAP index_map;
          BucketIndexer::get_index_map(*bucket_map_, total_, index_map);
          log_warn("load bucket index, index total: %d, bucket count: %lu, index map: %s",
                   total_, bucket_map_->size(), BucketIndexer::to_string(index_map).c_str());
        }

        return TAIR_RETURN_SUCCESS;
      }


      ////////////////////////////////
      // LdbManager
      ////////////////////////////////
      extern void get_bloom_stats(cache_stat* ldb_cache_stat);

      LdbManager::LdbManager() :
        frozen_bucket_(-1), use_bloomfilter_(false), cache_(NULL), cache_count_(0), scan_ldb_(NULL),
        migrate_wait_us_(0), using_head_(NULL), using_tail_(NULL), last_release_time_(0),
        remote_sync_logger_(NULL), balancer_(NULL)
      {
        init();
      }

      LdbManager::~LdbManager()
      {
        destroy();
      }

      int LdbManager::init()
      {
        db_count_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_INSTANCE_COUNT, 1);
        cache_count_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_CACHE_COUNT, 1);
        use_bloomfilter_ = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_USE_BLOOMFILTER, 0) > 0;

        if (cache_count_ > 0)
        {
          init_cache(cache_, cache_count_);
        }

        std::string cache_stat_path;
        if (cache_ != NULL || use_bloomfilter_)
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

        bool db_version_care = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_VERSION_CARE, 1) > 0;
        ldb_instance_ = new LdbInstance*[db_count_];
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i] = new LdbInstance(i, db_version_care, cache_ != NULL ? cache_[i % cache_count_] : NULL);
        }

        const char* bucket_index_strategy = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_BUCKET_INDEX_TO_INSTANCE_STRATEGY,
                                                                   BI_STRATEGY_HASH);
        bucket_indexer_ = BucketIndexer::new_bucket_indexer(bucket_index_strategy);

        log_warn("ldb storage engine construct count: %d, db version care: %s, cache count: %d, cache stat path: %s, bucket index strategy: %s",
                 db_count_, db_version_care ? "yes" : "no", cache_count_, cache_stat_path.c_str(),
                 bucket_index_strategy);

        return TAIR_RETURN_SUCCESS;
      }

      int LdbManager::init_cache(mdb_manager**& new_cache, int32_t count)
      {
        int ret = TAIR_RETURN_SUCCESS;
        std::string base_cache_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MDB_SHM_PATH,
                                                             "/mdb_shm_path");
        new_cache = new tair::mdb_manager*[count];
        memset(new_cache, 0, sizeof(new_cache));

        // init each cache
        int32_t i = 0;
        for (i = 0; i < count; ++i)
        {
          mdb_manager* cache = dynamic_cast<tair::mdb_manager*>(
            mdb_factory::create_embedded_mdb(append_num(base_cache_path, i).c_str()));
          if (NULL == cache)
          {
            log_error("init ldb memory cache fail, index: %d.", i);
            ret = TAIR_RETURN_FAILED;
            break;
          }
          else
          {
            new_cache[i] =cache;
          }
        }

        if (ret != TAIR_RETURN_SUCCESS && new_cache != NULL)
        {
          destroy_container(new_cache, i+1);
        }

        return ret;
      }

      int LdbManager::destroy()
      {
        LdbInstance** instance = ldb_instance_;
        ldb_instance_ = NULL;
        destroy_container(instance, db_count_);

        mdb_manager** cache = cache_;
        cache_ = NULL;
        destroy_container(cache, cache_count_);

        if (NULL != bucket_indexer_)
        {
          delete bucket_indexer_;
          bucket_indexer_ = NULL;   
        }

        while (using_head_ != NULL)
        {
          UsingLdbManager* current = using_head_;
          using_head_ = using_head_->next_;
          delete current;
        }

        if (remote_sync_logger_ != NULL)
        {
          delete remote_sync_logger_;
        }

        if (balancer_ != NULL)
        {
          delete balancer_;
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

      int LdbManager::direct_mupdate(int bucket_number, const tair_operc_vector& kvs)
      {
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else
        {
          rc = db_instance->direct_mupdate(bucket_number, kvs);
        }

        return rc;
      }

      int LdbManager::batch_put(int bucket_number, int area, tair::common::mput_record_vec* record_vec, bool version_care)
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

      int LdbManager::get(int bucket_number, data_entry& key, data_entry& value, bool stat)
      {
        log_debug("ldb::get");
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number, false);

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

      int LdbManager::get_range(int bucket_number, data_entry& key_start, data_entry& key_end, int offset, int limit, int type, std::vector<data_entry*>& result, bool &has_next)
      {
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number, false);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else 
        {
          PROFILER_BEGIN("db_instance get_range");
          rc = db_instance->get_range(bucket_number, key_start, key_end, offset, limit, type, result, has_next);
          log_debug("after get_range key_count:%lu", result.size());
          PROFILER_END();
        }
        return rc;
      }

      int LdbManager::del_range(int bucket_number, data_entry& key_start, data_entry& key_end, int offset, int limit, int type, std::vector<data_entry*>& result, bool &has_next)
      {
        int rc = TAIR_RETURN_SUCCESS;
        LdbInstance* db_instance = get_db_instance(bucket_number, false);

        if (db_instance == NULL)
        {
          log_error("ldb_bucket[%d] not exist", bucket_number);
          rc = TAIR_RETURN_FAILED;
        }
        else 
        {
          PROFILER_BEGIN("db_instance del_range");
          rc = db_instance->del_range(bucket_number, key_start, key_end, offset, limit, type, result, has_next);
          log_debug("after del_range key_count:%lu", result.size());
          PROFILER_END();
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

        for (int32_t i = 0; i < db_count_; ++i)
        {
          int tmp_ret = ldb_instance_[i]->clear_area(area);
          if (tmp_ret != TAIR_RETURN_SUCCESS)
          {
            ret = tmp_ret;
            log_error("clear area %d for instance %d fail.", area, i); // just continue
          }
        }

        // clear cache
        if (cache_ != NULL)
        {
          for (int32_t i = 0; i < cache_count_; ++i)
          {
            cache_[i]->clear(area);
          }
        }
        return ret;
      }

      bool LdbManager::init_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::init buckets: %lu", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        bool ret = false;

        if (1 == db_count_)
        {
          ret = ldb_instance_[0]->init_buckets(buckets);
        }
        else
        {
          BucketIndexer::INDEX_BUCKET_MAP index_map;
          bool updated = false;
          bucket_indexer_->sharding_bucket(db_count_, buckets, index_map, updated);

          if (updated && balancer_ != NULL)
          {
            balancer_->stop();
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            log_warn("instance %d own %lu buckets.", i, index_map[i].size());
            ret = ldb_instance_[i]->init_buckets(index_map[i]);
            if (!ret)
            {
              log_error("init buckets for db instance %d fail", i);
              break;
            }
          }
        }

        return ret;
      }

      void LdbManager::close_buckets(const std::vector<int>& buckets)
      {
        log_warn("ldb::close buckets: %lu", buckets.size());
        tbsys::CThreadGuard guard(&lock_);
        if (1 == db_count_)
        {
          ldb_instance_[0]->close_buckets(buckets);
        }
        else
        {
          BucketIndexer::INDEX_BUCKET_MAP index_map;
          bool updated = false;
          bucket_indexer_->sharding_bucket(db_count_, buckets, index_map, updated, true);

          if (updated && balancer_ != NULL)
          {
            balancer_->stop();
          }

          for (int32_t i = 0; i < db_count_; ++i)
          {
            ldb_instance_[i]->close_buckets(index_map[i]);
          }
        }

        // clear mdb cache
        if (cache_ != NULL)
        {
          for (int32_t i = 0; i < cache_count_; ++i)
          {
            cache_[i]->close_buckets(buckets);
          }
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
          // stop balance once starting migrate
          if (balancer_ != NULL)
          {
            balancer_->stop();
          }

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
          // @@ avoid to migrate data to one server too roughly, especially when several servers do
          // at the same time, just simply sleep some here
          if (migrate_wait_us_ > 0)
          {
            ::usleep(migrate_wait_us_);
          }
          ret = scan_ldb_->get_next_items(list);
          log_debug("get items %lu", list.size());
        }
        return ret;
      }

      void LdbManager::set_area_quota(int area, uint64_t quota)
      {
        // we consider set area quota to cache
        if (cache_ != NULL)
        {
          // round up
          uint64_t each_quota = (quota + cache_count_ - 1) / cache_count_;
          for (int32_t i = 0; i < cache_count_; ++i)
          {
            cache_[i]->set_area_quota(area, each_quota);
          }
        }
      }

      void LdbManager::set_area_quota(std::map<int, uint64_t>& quota_map)
      {
        if (cache_ != NULL)
        {
          // average area quota
          if (cache_count_ > 1)
          {
            for (std::map<int, uint64_t>::iterator it = quota_map.begin(); it != quota_map.end(); ++it)
            {
              it->second = (it->second + cache_count_ - 1) / cache_count_;
            }
          }

          for (int32_t i = 0; i < cache_count_; ++i)
          {
            cache_[i]->set_area_quota(quota_map);
          }
        }
      }

      int LdbManager::op_cmd(ServerCmdType cmd, std::vector<std::string>& params)
      {
        int ret = TAIR_RETURN_FAILED;

        tbsys::CThreadGuard guard(&lock_);

        // op cmd to instance
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ret = ldb_instance_[i]->op_cmd(cmd, params);
          if (ret != TAIR_RETURN_SUCCESS)
          {
            break;
          }
        }

        // op cmd in ldb_manager level shared by all instances
        // TODO: we really need one config center
        if (TAIR_RETURN_SUCCESS == ret)
        {
          switch (cmd)
          {
          case TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS:
          {
            if (params.empty())
            {
              log_error("set migrate wait time but no param.");
              ret = TAIR_RETURN_FAILED;
            }
            else
            {
              ret = do_set_migrate_wait(atoi(params[0].c_str()));
            }
            break;
          }
          case TAIR_SERVER_CMD_RELEASE_MEM:
          {
            ret = do_release_mem();
            break;
          }
          case TAIR_SERVER_CMD_SET_CONFIG:
          {
            if (params.size() < 2)
            {
              log_error("set config but invalid params size: %lu", params.size());
              ret = TAIR_RETURN_FAILED;
            }
            else if (params[0] == TAIR_PROFILER_THRESHOLD)
            {
              log_warn("set profiler threshold to %s", params[1].c_str());
              PROFILER_SET_THRESHOLD(atoi(params[1].c_str()));
            }
            break;
          }
          // fastdump cmd
          case TAIR_SERVER_CMD_RESET_DB:
          {
            ret = do_reset_db();
            break;
          }
          case TAIR_SERVER_CMD_START_BALANCE:
          {
            if (balancer_ == NULL)
            {
              balancer_ = new LdbBalancer(this);
            }
            balancer_->start();
            break;
          }
          case TAIR_SERVER_CMD_STOP_BALANCE:
          {
            if (balancer_ != NULL)
            {
              balancer_->stop();
            }
            else
            {
              log_error("balancer not start");
            }
            break;
          }
          case TAIR_SERVER_CMD_SET_BALANCE_WAIT_MS:
          {
            if (params.empty())
            {
              log_error("set balance wait time but no param.");
              ret = TAIR_RETURN_FAILED;
            }
            else if (balancer_ != NULL)
            {
              balancer_->set_wait_us(static_cast<int64_t>(atoi(params[0].c_str())) * 1000);
            }
            break;
          }
          // fastdump cmd end
          // do nothing cmd
          case TAIR_SERVER_CMD_PAUSE_GC:
          case TAIR_SERVER_CMD_RESUME_GC:
          case TAIR_SERVER_CMD_STAT_DB:
          {
            break;
          }
          default:
          {
            break;
          }
          }
        }
        return ret;
      }

      int LdbManager::do_set_migrate_wait(int32_t cmd_wait_ms)
      {
        static const int32_t MAX_MIGRATE_WAIT_MS = 60000; // 1min
        int ret = TAIR_RETURN_SUCCESS;
        if (cmd_wait_ms >= 0 && cmd_wait_ms < MAX_MIGRATE_WAIT_MS)
        {
          log_warn("set migrate wait ms %d", cmd_wait_ms);
          migrate_wait_us_ = cmd_wait_ms * 1000;
          ret = TAIR_RETURN_SUCCESS;
        }
        else
        {
          log_error("set migrate wait time but param is invalid: %d. max migrate wait ms: %d", cmd_wait_ms, MAX_MIGRATE_WAIT_MS);
          ret = TAIR_RETURN_FAILED;
        }
        return ret;
      }

      int LdbManager::do_reset_db()
      {
        // reset ldb cache
        std::string base_cache_back_path;
        mdb_manager** new_cache = NULL;
        LdbInstance** new_ldb_instance = NULL;

        int ret = do_reset_cache(new_cache, base_cache_back_path);

        if (TAIR_RETURN_SUCCESS == ret)
        {
          ret = do_reset_db_instance(new_ldb_instance, new_cache);
          if (TAIR_RETURN_SUCCESS == ret)
          {
            ret = do_switch_db(new_ldb_instance, new_cache, base_cache_back_path);
          }
        }

        return ret;
      }

      int LdbManager::do_reset_cache(mdb_manager**& new_cache, std::string& base_back_path)
      {
        if (cache_ == NULL)
        {
          return TAIR_RETURN_SUCCESS;
        }

        int ret = TAIR_RETURN_SUCCESS;
        // rotate using cache file
        static std::string sys_shm_path = std::string("/dev/shm/"); // just hard code
        std::string base_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_MDB_SHM_PATH,
                                                       "/mdb_shm_path");
        base_back_path = get_back_path(base_path.c_str());

        for (int32_t i = 0; i < cache_count_; ++i)
        {
          std::string cache_path = append_num(base_path, i); 
          std::string back_cache_path = append_num(base_back_path, i);
          if (::access((sys_shm_path + cache_path).c_str(), F_OK) != 0)
          {
            // just consider it's ok. maybe ::access fail because other reason.
            log_warn("resetdb but orignal cache path is not exist: %s, ignore it.", cache_path.c_str());
          }
          else if (::rename((sys_shm_path + cache_path).c_str(), (sys_shm_path + back_cache_path).c_str()) != 0)
          {
            log_error("resetdb cache %s to back cache %s fail. error: %s",
                      cache_path.c_str(), back_cache_path.c_str(), strerror(errno));
            ret = TAIR_RETURN_FAILED;
            break;
          }
        }

        if (ret == TAIR_RETURN_SUCCESS)
        {
          log_info("reinit new cache");
          ret = init_cache(new_cache, cache_count_);
        }

        return ret;
      }

      int LdbManager::do_reset_db_instance(LdbInstance**& new_ldb_instance, mdb_manager** new_cache)
      {
        int ret = TAIR_RETURN_SUCCESS;
        // init new ldb instance
        // get orignal buckets deployment
        log_info("reinit ldb instance");
        std::vector<int32_t>* tmp_buckets = new std::vector<int32_t>[db_count_];
        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i]->get_buckets(tmp_buckets[i]);
        }

        bool db_version_care = TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_DB_VERSION_CARE, 1) > 0;
        new_ldb_instance = new LdbInstance*[db_count_];

        int32_t i = 0;
        for (i = 0; i < db_count_; ++i)
        {
          new_ldb_instance[i] = new LdbInstance(i, db_version_care,
                                                new_cache != NULL ? new_cache[i % cache_count_] : NULL);
          log_debug("reinit instance %d own %lu buckets.", i, tmp_buckets[i].size());
          if (!new_ldb_instance[i]->init_buckets(tmp_buckets[i]))
          {
            log_error("resetdb reinit db fail. instance: %d", i);
            ret = TAIR_RETURN_FAILED;
            break;
          }
        }

        if (ret != TAIR_RETURN_SUCCESS && new_ldb_instance != NULL)
        {
          destroy_container(new_ldb_instance, i+1);
        }

        delete[] tmp_buckets;

        return ret;
      }

      int LdbManager::do_switch_db(LdbInstance** new_ldb_instance, mdb_manager** new_cache, std::string& base_cache_back_path)
      {
        int ret = TAIR_RETURN_SUCCESS;
        UsingLdbManager* new_using_mgr = new UsingLdbManager;
        new_using_mgr->ldb_instance_ = ldb_instance_;
        new_using_mgr->db_count_ = db_count_;
        new_using_mgr->cache_ = cache_;
        new_using_mgr->cache_count_ = cache_count_;
        new_using_mgr->base_cache_path_ = base_cache_back_path;
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
        return ret;
      }

      int LdbManager::do_release_mem()
      {
        last_release_time_ = time(NULL);
        return TAIR_RETURN_SUCCESS;
      }

      void LdbManager::maybe_exec_cmd()
      {
        // do something special left by op_cmd
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

#ifdef WITH_TCMALLOC
        // force to free memory to system,
        // 'cause freed memory may be in tcmalloc's free page list.
        static const int32_t RELEASE_LDB_MEM_INTERVAL_S = 60; // 60s
        if (last_release_time_ > 0 && time(NULL) - last_release_time_ > RELEASE_LDB_MEM_INTERVAL_S)
        {
          MallocExtension::instance()->ReleaseFreeMemory();
          log_warn("tcmalloc release memory to sys now");
          malloc_stats();
          last_release_time_ = 0;
        }
#endif
      }

      void LdbManager::get_stats(tair_stat* stat)
      {
        tbsys::CThreadGuard guard(&lock_);
        log_debug("ldbmanager get stats");

        for (int32_t i = 0; i < db_count_; ++i)
        {
          ldb_instance_[i]->get_stats(stat);
        }
        // lock hold, static is ok.
        // use cache stat's 
        static cache_stat ldb_cache_stat[TAIR_MAX_AREA_COUNT];
        if (cache_ != NULL || use_bloomfilter_)
        {
          if (cache_ != NULL)
          {
            // get total cache stat
            for (int32_t i = 0; i < cache_count_; ++i)
            {
              if (i == 0)
              {
                cache_[i]->raw_get_stats(ldb_cache_stat);
              }
              else
              {
                cache_[i]->raw_update_stats(ldb_cache_stat);
              }
            }
          }
          if (use_bloomfilter_)
          {
            // use cache to store bloom stat now temporarily.
            // TODO ..
            get_bloom_stats(ldb_cache_stat);
          }
          cache_stat_.save(ldb_cache_stat, TAIR_MAX_AREA_COUNT);
        }
        maybe_exec_cmd();
      }

      void LdbManager::set_bucket_count(uint32_t bucket_count)
      {
        if (this->bucket_count == 0)
        {
          this->bucket_count = bucket_count;
          if (cache_ != NULL)
          {
            for (int32_t i = 0; i < cache_count_; ++i)
            {
              // set cache (mdb_manager) bucket count
              cache_[i]->set_bucket_count(bucket_count);
            }
          }
        }
      }

      void LdbManager::get_index_map(BucketIndexer::INDEX_BUCKET_MAP& result)
      {
        bucket_indexer_->get_index_map(result);
      }

      int LdbManager::reindex_bucket(int32_t bucket, int32_t from, int32_t to)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (from >= db_count_ || to >= db_count_)
        {
          log_error("reindex bucket fail: invalid index. %d: %d => %d", bucket, from, to);
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          ret = bucket_indexer_->reindex(bucket, from, to);
          if (ret == TAIR_RETURN_SUCCESS)
          {
            // close bucket
            std::vector<int32_t> buckets(1, bucket);
            ldb_instance_[from]->close_buckets(buckets);
          }
        }
        return ret;
      }

      void LdbManager::pause_service(int32_t bucket)
      {
        // set to be frozen
        frozen_bucket_ = bucket;
      }

      void LdbManager::resume_service(int32_t bucket)
      {
        frozen_bucket_ = -1;
      }

      RecordLogger* LdbManager::get_remote_sync_logger()
      {
        // init-on-use
        if (NULL == remote_sync_logger_)
        {
          remote_sync_logger_ = new LdbRemoteSyncLogger(this);
        }
        return remote_sync_logger_;
      }

      LdbInstance* LdbManager::get_instance(int32_t index)
      {
        LdbInstance* ret = NULL;
        if (index < db_count_ && ldb_instance_ != NULL)
        {
          ret = ldb_instance_[index]; 
        }
        return ret;
      }

      LdbInstance* LdbManager::get_db_instance(const int bucket_number, bool write)
      {
        LdbInstance* ret = NULL;
        if (UNLIKELY(bucket_number == frozen_bucket_ && write))
        {
          // maybe wait for a while and try again
        }
        else if (LIKELY(NULL != ldb_instance_))
        {
          if (1 == db_count_)
          {
            // TODO: add init_ flag to remove exist() check
            ret = ldb_instance_[0]->exist(bucket_number) ? ldb_instance_[0] : NULL;
          }
          else
          {
            bool recheck = true;
            int index = bucket_indexer_->bucket_to_index(bucket_number, recheck);
            if (LIKELY(index >= 0))
            {
              ret = ldb_instance_[index];
              if (recheck)
              {
                ret = ret->exist(bucket_number) ? ret : NULL;
              }
            }
          }
        }
        return ret;
      }

    }
  }
}
