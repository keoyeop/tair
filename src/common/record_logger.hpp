/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RecordLogger:
 *   Manage record logger(reader/writer).
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_COMMON_RECORD_LOGGER_H
#define TAIR_COMMON_RECORD_LOGGER_H

namespace tair
{
  namespace common
  {
    class data_entry;

    class RecordLogger
    {
    public:
      RecordLogger() : writer_count_(0), reader_count_(0) {}
      virtual ~RecordLogger() {}

      int get_writer_count() { return writer_count_; }
      int get_reader_count() { return reader_count_; }

      virtual int init() = 0;
      // add one record to logger. index indicates writer index.
      virtual int add_record(int32_t index, int32_t type,
                             data_entry* key, data_entry* value) = 0;
      // get NEXT avaliable record from logger. index indicates reader index.
      // key and type must be returned, while value is optional dependent on specified implementation.
      virtual int get_record(int32_t index, int32_t& type,
                             data_entry*& key, data_entry*& value) = 0;

    protected:
      int32_t writer_count_;
      int32_t reader_count_;
    };

  }
}
#endif
