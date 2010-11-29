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

#ifndef TAIR_KDB_BUCKET_H
#define TAIR_KDB_BUCKET_H

#include "storage/kdb/kyotocabinet/kchashdb.h"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"

namespace tair {
  namespace storage {
    namespace kdb {
      class kdb_bucket {
        const static int PATH_MAX_LENGTH = 1024;
        public:
          kdb_bucket();
          ~kdb_bucket();

          bool start(int bucket_number);
          void stop();

          int put(common::data_entry& key, common::data_entry& value, bool version_care, uint32_t expire_time);

          int get(common::data_entry& key, common::data_entry& value);

          int remove(common::data_entry& key, bool version_care);

          void destory(); // this will remove the data file

          void get_stat(tair_stat* stat);

        private:
          void print_db_error(const char* prefix);

        private:
          char filename[PATH_MAX_LENGTH];
          bool is_item_expired();
          kyotocabinet::HashDB db;
      };
    }
  }
}

#endif
