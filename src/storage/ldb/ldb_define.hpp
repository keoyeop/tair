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

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      const static int LDB_KEY_META_SIZE = 3;
      const static int MAX_SCAN_KEY = (1 << 24 - 2);

      class LdbKey
      {
      public:
        LdbKey() : data_(NULL), data_size_(0), alloc_(false)
        {
        }

        LdbKey(const int32_t bucket_number, const char* key_data, const int32_t key_size)
        {
          set(bucket_number, key_data, key_size);
        }
        ~LdbKey()
        {
          free();
        }

        inline void set(const int32_t bucket_number, const char* key_data, const int32_t key_size)
        {
          if (key_data != NULL && key_size > 0)
          {
            free();
            data_size_ = key_size + LDB_KEY_META_SIZE;
            data_ = new char[data_size_];
            build_key_meta(bucket_number, data_);
            memcpy(data_ + LDB_KEY_META_SIZE, key_data, key_size);
            alloc_ = true;
          }
        }
        inline void assign(char* data, const int32_t data_size)
        {
          free();
          data_ = data;
          data_size_ = data_size;
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

        static void build_key_meta(const int32_t bucket_number, char* buf)
        {
          // consistent key len SCAN_KEY_LEN
          // maybe dynamic
          assert(bucket_number < MAX_SCAN_KEY);
          for (int i = 0; i < LDB_KEY_META_SIZE; ++i)
          {
            buf[LDB_KEY_META_SIZE - i - 1] = (bucket_number >> (i*8)) & 0xFF;
          }
        }

      private:
        char* data_;
        int32_t data_size_;
        bool alloc_;
      };

      struct LdbItemMeta
      {
        LdbItemMeta() : flag_(0), reserved_(0), version_(0), cdate_(0), mdate_(0), edate_(0) {}
        uint8_t  flag_;         // flag
        uint8_t  reserved_;     // just reserved
        uint16_t version_;      // version
        uint32_t cdate_;  // create time
        uint32_t mdate_;  // modify time
        uint32_t edate_;  // time when item is expired
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
        inline bool is_expired()
        {
          uint32_t expired_time = meta().edate_;
          return expired_time != 0 && expired_time < time(NULL);
        }

      private:
        LdbItemMeta meta_;
        char* data_;
        int32_t data_size_;
        bool alloc_;
      };

    }
  }
}
#endif
