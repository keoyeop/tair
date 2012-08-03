/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RecordLogger implementation of ldb
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H
#define TAIR_STORAGE_LDB_REMOTE_SYNC_LOGGER_H

#include "common/data_entry.hpp"
#include "common/record_logger.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      class LdbRemoteSyncLogReader
      {
      public:
        LdbRemoteSyncLogReader();
        ~LdbRemoteSyncLogReader();

        int start();
        int get_record(int32_t& type, tair::common::data_entry*& key, tair::common::data_entry*& value);
      };

      class LdbRemoteSyncLogger : public tair::common::RecordLogger
      {
      public:
        LdbRemoteSyncLogger();
        virtual ~LdbRemoteSyncLogger();

        int init();
        int add_record(int32_t index, int32_t type,
                       tair::common::data_entry* key, tair::common::data_entry* value);
        int get_record(int32_t index, int32_t& type,
                       tair::common::data_entry*& key, tair::common::data_entry*& value);

      private:
        LdbRemoteSyncLogReader* reader_;
      };

    }
  }
}
#endif
