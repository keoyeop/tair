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

#ifndef TAIR_STORAGE_LDB_BUCKET_H
#define TAIR_STORAGE_LDB_BUCKET_H

#include "leveldb/db.h"

#include "common/data_entry.hpp"
#include "ldb_define.hpp"
#include "bg_task.hpp"
#include "stat_manager.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbBucket
      {
      public:
        LdbBucket();
        ~LdbBucket();

        bool start(int bucket_number);
        void stop();
        void destroy();

        int put(int bucket_number, tair::common::data_entry& key, tair::common::data_entry& value, bool version_care, uint32_t expire_time);
        int get(int bucket_number, tair::common::data_entry& key, tair::common::data_entry& value);
        int remove(int bucket_number, tair::common::data_entry& key, bool version_care);

        bool begin_scan(int bucket_number);
        bool end_scan();
        bool get_next_item(item_data_info* &data, bool& still_have);
        void get_stat(tair_stat* stat);

      private:
        void sanitize_option(leveldb::Options& options);

      private:
        char db_path_[PATH_MAX];
        leveldb::DB* db_;
        leveldb::Iterator* scan_it_;
        std::string scan_end_key_;
        stat_manager stat_manager_;
        BgTask bg_task_;
      };
    }
  }
}
#endif
