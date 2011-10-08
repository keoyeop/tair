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

#ifndef TAIR_STORAGE_LDB_DEFINE_H
#define TAIR_STORAGE_LDB_DEFINE_H

#include "leveldb/db.h"

namespace leveldb
{
  class DB;
}

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      const static int LDB_EXPIRED_TIME_SIZE = sizeof(uint32_t);
      const static int LDB_KEY_BUCKET_NUM_SIZE = 3;
      const static int LDB_KEY_META_SIZE = LDB_KEY_BUCKET_NUM_SIZE + LDB_EXPIRED_TIME_SIZE;
      const static int MAX_BUCKET_NUMBER = (1 << 24) - 2;

      class LdbKey
      {
      public:
        LdbKey() : data_(NULL), data_size_(0), alloc_(false)
        {
        }

        LdbKey(const char* key_data, int32_t key_size, int32_t bucket_number, uint32_t expired_time = 0) :
          data_(NULL), data_size_(0), alloc_(false)
        {
          set(key_data, key_size, bucket_number, expired_time);
        }
        ~LdbKey()
        {
          free();
        }

        inline void set(const char* key_data, int32_t key_size, int32_t bucket_number, uint32_t expired_time)
        {
          if (key_data != NULL && key_size > 0)
          {
            free();
            data_size_ = key_size + LDB_KEY_META_SIZE;
            data_ = new char[data_size_];
            build_key_meta(data_, bucket_number, expired_time);
            memcpy(data_ + LDB_KEY_META_SIZE, key_data, key_size);
            alloc_ = true;
          }
        }
        inline void assign(char* data, const int32_t data_size)
        {
          free();
          data_ = data;
          data_size_ = data_size;
          alloc_ = false;
        }
        inline void free()
        {
          if (alloc_ && data_ != NULL)
          {
            delete data_;
            data_ = NULL;
            data_size_ = 0;
            alloc_ = false;
          }
        }
        inline char* data()
        {
          return data_;
        }
        inline int32_t size()
        {
          return data_size_;
        }
        inline char* key()
        {
          return data_ != NULL ? data_ + LDB_KEY_META_SIZE : NULL;
        }
        inline int32_t key_size()
        {
          return data_size_ > LDB_KEY_META_SIZE ? (data_size_ - LDB_KEY_META_SIZE) : 0;
        }

        static void build_key_meta(char* buf, int32_t bucket_number, uint32_t expired_time = 0)
        {
          // consistent key len SCAN_KEY_LEN
          // big-endian to use default bitewise comparator.
          // consider: varintint may be space-saved,
          // but user defined comparator need caculate datalen every time.

          // encode expired time
          encode_fixed32(buf, expired_time);
          encode_bucket_number(buf + LDB_EXPIRED_TIME_SIZE, bucket_number);
        }

        static void encode_bucket_number(char* buf, int bucket_number)
        {
          assert(bucket_number < MAX_BUCKET_NUMBER);
          for (int i = 0; i < LDB_KEY_BUCKET_NUM_SIZE; ++i)
          {
            buf[LDB_KEY_BUCKET_NUM_SIZE - i - 1] = (bucket_number >> (i*8)) & 0xFF;
          }
        }

        static int decode_bucket_number(const char* buf)
        {
          int bucket_number = 0;
          for (int i = 0; i < LDB_KEY_BUCKET_NUM_SIZE; ++i)
          {
            bucket_number |= buf[i] << (LDB_KEY_BUCKET_NUM_SIZE - i - 1);
          }
          assert(bucket_number < MAX_BUCKET_NUMBER);
          return bucket_number;
        }

        static void encode_fixed32(char* buf, uint32_t value)
        {
#if __BYTE_ORDER == __LITTLE_ENDIAN
          memcpy(buf, &value, sizeof(value));
#else
          buf[0] = value & 0xff;
          buf[1] = (value >> 8) & 0xff;
          buf[2] = (value >> 16) & 0xff;
          buf[3] = (value >> 24) & 0xff;
#endif           
        }

        static uint32_t decode_fixed32(const char* buf)
        {
          if (1)
          {
            // Load the raw bytes
            uint32_t result;
            memcpy(&result, buf, sizeof(result));  // gcc optimizes this to a plain load
            return result;
          }
          else
          {
            return ((static_cast<uint32_t>(buf[0]))
                    | (static_cast<uint32_t>(buf[1]) << 8)
                    | (static_cast<uint32_t>(buf[2]) << 16)
                    | (static_cast<uint32_t>(buf[3]) << 24));
          }
        }

      private:
        char* data_;
        int32_t data_size_;
        bool alloc_;
      };

      struct LdbItemMeta
      {
        LdbItemMeta() : flag_(0), version_(0), cdate_(0), mdate_(0) {}
        uint8_t  flag_;         // flag
        uint16_t version_;      // version
        uint32_t cdate_;        // create time
        uint32_t mdate_;        // modify time
        uint32_t edate_;        // expired time(for meta when get value. dummy with key)
        uint8_t  reserved_;     // just reserved
      };

      const int32_t LDB_ITEM_META_SIZE = sizeof(LdbItemMeta);

      class LdbItem
      {
      public:
        LdbItem() : meta_(), data_(NULL), data_size_(0), alloc_(false)
        {
        }
        ~LdbItem()
        {
          free();
        }

        // meta_ MUST already be set correctly
        void set(const char* value_data, const int32_t value_size)
        {
          if (value_data != NULL && value_size > 0)
          {
            free();
            data_size_ = value_size + LDB_ITEM_META_SIZE;
            data_ = new char[data_size_];
            memcpy(data_, &meta_, LDB_ITEM_META_SIZE);
            memcpy(data_ + LDB_ITEM_META_SIZE, value_data, value_size);
            alloc_ = true;
          }
        }
        void assign(char* data, const int32_t data_size)
        {
          free();
          data_ = data;
          data_size_ = data_size;
          meta_ = *(reinterpret_cast<LdbItemMeta*>(data_));
        }
        void free()
        {
          if (alloc_ && data_ != NULL)
          {
            delete data_;
            data_ = NULL;
            data_size_ = 0;
            alloc_ = false;
          }
        }
        inline char* data()
        {
          return data_;
        }
        inline int32_t size()
        {
          return data_size_;
        }
        inline char* value()
        {
          return NULL != data_ ? data_ + LDB_ITEM_META_SIZE : NULL;
        }
        inline int32_t value_size()
        {
          return data_size_ > LDB_ITEM_META_SIZE ? data_size_ - LDB_ITEM_META_SIZE : 0;
        }
        inline LdbItemMeta& meta()
        {
          return meta_;
        }

      private:
        LdbItemMeta meta_;
        char* data_;
        int32_t data_size_;
        bool alloc_;
      };

      bool get_db_stat(leveldb::DB* db, std::string& value, const char* property);
      bool get_db_stat(leveldb::DB* db, int64_t& value, const char* property);
      int32_t get_level_num(leveldb::DB* db);
      bool get_level_range(leveldb::DB* db, int32_t level, std::string* smallest, std::string* largest);
    }
  }
}
#endif
