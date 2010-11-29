/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * kyotocabinet storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *
 */

#include "kdb_bucket.h"

namespace {
  const char* TAIR_KDB_SECTION = "kdb";
  const char* KDB_MAP_SIZE = "map_size";
  const uint64_t KDB_MAP_SIZE_DEFAULT = 10 * 1024 * 1024; // 10MB
  const char* KDB_BUCKET_SIZE = "bucket_size";
  const uint64_t KDB_BUCKET_SIZE_DEFAULT = 1048583ULL;
  const char* KDB_RECORD_ALIGN = "record_align";
  const uint64_t KDB_RECORD_ALIGN_DEFAULT = 128;

  const char* KDB_DATA_DIRECTORY = "data_dir";
}

namespace tair {
  namespace storage {
    namespace kdb {

      kdb_bucket::kdb_bucket()
      {
      }

      kdb_bucket::~kdb_bucket()
      {
        stop();
      }

      bool kdb_bucket::start(int bucket_number)
      {
        bool ret = true;
        const char* data_dir = TBSYS_CONFIG.getString(TAIR_KDB_SECTION, KDB_DATA_DIRECTORY, NULL);
        if (data_dir == NULL) {
          log_error("kdb data dir not config, item: %s.%s", TAIR_KDB_SECTION, KDB_DATA_DIRECTORY);
          ret  = false;
        }

        if (ret) {
          snprintf(filename, PATH_MAX_LENGTH, "%s/tair_kdb_%06d.dat", data_dir, bucket_number);
          uint64_t map_size = TBSYS_CONFIG.getInt(TAIR_KDB_SECTION, KDB_MAP_SIZE, KDB_MAP_SIZE_DEFAULT);
          uint64_t bucket_size = TBSYS_CONFIG.getInt(TAIR_KDB_SECTION, KDB_BUCKET_SIZE, KDB_BUCKET_SIZE_DEFAULT);
          uint64_t record_align = TBSYS_CONFIG.getInt(TAIR_KDB_SECTION, KDB_RECORD_ALIGN, KDB_RECORD_ALIGN_DEFAULT);

          ret = db.tune_map(map_size);
          if (!ret ) {
            print_db_error("set mmap size failed");
          }

          if (ret) {
            ret = db.tune_alignment(record_align);
            if (!ret) {
              print_db_error("set record alignment failed");
            }
          }

          if (ret) {
            ret = db.tune_options(kyotocabinet::HashDB::TLINEAR);
            if (!ret) {
              print_db_error("set option failed");
            }
          }

          if (ret) {
            ret = db.tune_buckets(bucket_size);
            if (!ret) {
              print_db_error("set bucket size failed");
            }
          }

          if (ret) {
            uint32_t mode = kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OCREATE;
            ret = db.open(filename, mode);
            if (!ret) {
              print_db_error("open kdb failed");
            }
          }
        }

        if (ret) {
          log_info("kdb [%d] opened", bucket_number);
        }

        return ret;
      }

      void kdb_bucket::stop()
      {
        if (!db.close()) {
          print_db_error("close kdb failed");
        }
      }

      int kdb_bucket::put(common::data_entry& key, common::data_entry& value, bool version_care, uint32_t expire_time)
      {
        return 0;
      }

      int kdb_bucket::get(common::data_entry& key, common::data_entry& value)
      {
        return 0;
      }

      int kdb_bucket::remove(common::data_entry& key, bool version_care)
      {
        return 0;
      }

      void kdb_bucket::destory()
      {
      }

      void kdb_bucket::get_stat(tair_stat* stat)
      {
      }

      bool kdb_bucket::is_item_expired()
      {
        return false;
      }
    }
  }
}
