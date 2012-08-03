/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some common RecordLogger implementation
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "data_entry.hpp"
#include "record_logger_util.hpp"

namespace tair
{
  namespace common
  {
    //////////////////////////////////
    // MemFileRecordLogger
    //////////////////////////////////
    MemFileRecordLogger::MemFileRecordLogger()
    {
      // one writer
      writer_count_ = 1;
      // one reader
      reader_count_ = 1;
    }

    MemFileRecordLogger::~MemFileRecordLogger()
    {
    }

    int MemFileRecordLogger::init()
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

    int MemFileRecordLogger::add_record(int32_t index, int32_t type,
                                        data_entry* key, data_entry* value)
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

    int MemFileRecordLogger::get_record(int32_t index, int32_t& type,
                                        data_entry*& key, data_entry*& value)
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

    //////////////////////////////////
    // FileRecordLogger
    //////////////////////////////////
    FileRecordLogger::FileRecordLogger()
    {
      // one writer
      writer_count_ = 1;
      // one reader
      reader_count_ = 1;
    }

    FileRecordLogger::~FileRecordLogger()
    {
    }

    int FileRecordLogger::init()
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

    int FileRecordLogger::add_record(int32_t index, int32_t type,
                                     data_entry* key, data_entry* value)
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

    int FileRecordLogger::get_record(int32_t index, int32_t& type,
                                     data_entry*& key, data_entry*& value)
    {
      int ret = TAIR_RETURN_SUCCESS;
      return ret;
    }

  }
}
