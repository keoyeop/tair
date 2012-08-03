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

#include "ldb_remote_sync_logger.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using tair::common::data_entry;

      //////////////////////////////////
      // LdbRemoteSyncLogReader
      //////////////////////////////////
      LdbRemoteSyncLogReader::LdbRemoteSyncLogReader()
      {
      }

      LdbRemoteSyncLogReader::~LdbRemoteSyncLogReader()
      {
      }

      int LdbRemoteSyncLogReader::start()
      {
        return TAIR_RETURN_SUCCESS;
      }

      int LdbRemoteSyncLogReader::get_record(int32_t& type, data_entry*& key, data_entry*& value)
      {
        int ret = TAIR_RETURN_SUCCESS;
        return ret;
      }

      //////////////////////////////////
      // LdbRemoteSyncLogger
      //////////////////////////////////
      LdbRemoteSyncLogger::LdbRemoteSyncLogger()
      {
        // we use leveldb's bin-log here where key/value has already been
        // added in function logic, so need no writer
        writer_count_ = 0;
        // several reader, each ldb instance has one reader(leveldb log reader)
        reader_count_ = 1;
        reader_ = new LdbRemoteSyncLogReader[reader_count_];
      }

      LdbRemoteSyncLogger::~LdbRemoteSyncLogger()
      {
        if (reader_ != NULL)
        {
          delete [] reader_;
        }
      }

      int LdbRemoteSyncLogger::init()
      {
        int ret = TAIR_RETURN_SUCCESS;
        return ret;
      }

      int LdbRemoteSyncLogger::add_record(int32_t index, int32_t type,
                                          data_entry* key, data_entry* value)
      {
        // no writer, do nothing.
        return TAIR_RETURN_SUCCESS;
      }

      int LdbRemoteSyncLogger::get_record(int32_t index, int32_t& type,
                                          data_entry*& key, data_entry*& value)
      {
        int ret = TAIR_RETURN_SUCCESS;
        if (index < 0 || index >= reader_count_)
        {
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          ret = reader_[index].get_record(type, key, value);
        }
        return ret;
      }

    }
  }
}
