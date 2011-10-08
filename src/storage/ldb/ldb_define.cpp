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

#include "common/log.hpp"
#include "ldb_define.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      bool get_db_stat(leveldb::DB* db, std::string& value, const char* property)
      {
        bool ret = db != NULL && property != NULL;

        if (ret)
        {
          value.clear();

          char name[32];
          snprintf(name, sizeof(name), "leveldb.%s", property);
          std::string stat_value;

          if (!(ret = db->GetProperty(leveldb::Slice(std::string(name)), &stat_value)))
          {
            log_error("get db stats fail");
          }
          else
          {
            value += stat_value;
          }
        }
        return ret;
      }

      bool get_db_stat(leveldb::DB* db, int64_t& value, const char* property)
      {
        std::string str_value;
        bool ret = false;
        if ((ret = get_db_stat(db, str_value, property)))
        {
          value = atoll(str_value.c_str());
        }

        return ret;
      }

      int32_t get_level_num(leveldb::DB* db)
      {
        int64_t level = 0;
        get_db_stat(db, level, "levelnums");
        return static_cast<int32_t>(level);
      }

      bool get_level_range(leveldb::DB* db, int32_t level, std::string* smallest, std::string* largest)
      {
        bool ret = false;
        if (db != NULL)
        {
          ret = db->GetLevelRange(level, smallest, largest);
        }
        return ret;
      }
    }
  }
}
