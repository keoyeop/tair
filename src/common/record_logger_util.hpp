/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some common RecordLogger implementation
 *
 * Version: $Id: record_logger_util.hpp 28 2012-07-30 10:18:09Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_COMMON_RECORD_LOGGER_UTIL_H
#define TAIR_COMMON_RECORD_LOGGER_UTIL_H

#include "record_logger.hpp"

namespace tair
{
  namespace common
  {
    // Record logger wrapped on memory and file.
    // Memory part is accessed frequently and
    // records can be swapped in/out between memory and file.
    class MemFileRecordLogger : public tair::common::RecordLogger
    {
    public:
      MemFileRecordLogger();
      virtual ~MemFileRecordLogger();

      int init();
      int add_record(int32_t index, int32_t type,
                     data_entry* key, data_entry* value);
      int get_record(int32_t index, int32_t& type,
                     data_entry*& key, data_entry*& value);

    private:

    };

    // Record logger based on disk file
    class FileRecordLogger : public tair::common::RecordLogger
    {
    public:
      FileRecordLogger();
      virtual ~FileRecordLogger();

      int init();
      int add_record(int32_t index, int32_t type,
                     data_entry* key, data_entry* value);
      int get_record(int32_t index, int32_t& type,
                     data_entry*& key, data_entry*& value);

    private:

    };

  }
}

#endif
