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
#include "ldb_define.hpp"
#include "ldb_gc_factory.hpp"
#include "ldb_comparator.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      static const size_t LDB_COMPARE_SKIP_SIZE = LDB_EXPIRED_TIME_SIZE;
      LdbComparatorImpl::LdbComparatorImpl() : gc_(NULL)
      {
      }

      LdbComparatorImpl::LdbComparatorImpl(LdbGcFactory* gc) : gc_(gc)
      {
      }

      LdbComparatorImpl::~LdbComparatorImpl()
      {
      }

      const char* LdbComparatorImpl::Name() const
      {
        return "ldb.LdbComparator";
      }

      // skip expired time
      int LdbComparatorImpl::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
      {
        assert(a.size() > LDB_COMPARE_SKIP_SIZE && b.size() > LDB_COMPARE_SKIP_SIZE);
        const int min_len = (a.size() < b.size()) ? a.size() - LDB_COMPARE_SKIP_SIZE :
          b.size() - LDB_COMPARE_SKIP_SIZE;
        int r = memcmp(a.data() + LDB_COMPARE_SKIP_SIZE, b.data() + LDB_COMPARE_SKIP_SIZE, min_len);
        if (r == 0)
        {
          if (a.size() < b.size())
          {
            r = -1;
          }
          else if (a.size() > b.size())
          {
            r = +1;
          }
        }
        return r;
      }

      // skip expired time
      void LdbComparatorImpl::FindShortestSeparator(
        std::string* start,
        const leveldb::Slice& limit) const
      {
        // Find length of common prefix
        size_t min_length = std::min(start->size(), limit.size());
        assert(min_length > LDB_COMPARE_SKIP_SIZE);
        size_t diff_index = LDB_COMPARE_SKIP_SIZE;
        while ((diff_index < min_length) &&
               ((*start)[diff_index] == limit[diff_index]))
        {
          diff_index++;
        }

        if (diff_index >= min_length) {
          // Do not shorten if one string is a prefix of the other
        }
        else
        {
          uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
          if (diff_byte < static_cast<uint8_t>(0xff) &&
              diff_byte + 1 < static_cast<uint8_t>(limit[diff_index]))
          {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            assert(Compare(*start, limit) < 0);
          }
        }
      }

      void LdbComparatorImpl::FindShortSuccessor(std::string* key) const
      {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = LDB_COMPARE_SKIP_SIZE; i < n; i++)
        {
          const uint8_t byte = (*key)[i];
          if (byte != static_cast<uint8_t>(0xff))
          {
            (*key)[i] = byte + 1;
            key->resize(i+1);
            return;
          }
        }
        // *key is a run of 0xffs.  Leave it alone.
      }

      bool LdbComparatorImpl::ShouldDrop(const char* key, int64_t sequence, uint32_t now) const
      {
        assert(key != NULL);
        // key format
        //     expired_time  fixed32
        //     bucket_number 3bytes
        //     area_number   2bytes
        //     user_key      ...

        bool drop = false;
        const char* pkey = key;
        uint32_t expired_time = LdbKey::decode_fixed32(pkey);
        fprintf(stderr, "exptime %u <> %u\n", expired_time, now);
        if (expired_time > 0 && expired_time < (now > 0 ? now : time(NULL))) // 1. check expired time
        {
          drop = true;
        }
        else
        {
          pkey += LDB_EXPIRED_TIME_SIZE;

          // To decode as later as possible, check empty and check need_gc separately, so need lock here.
          tbsys::CThreadGuard guard(&gc_->lock_);
          fprintf(stderr, "bucket size: %zd area: %zd\n", gc_->gc_buckets_.size(), gc_->gc_areas_.size());
          if (!gc_->gc_buckets_.empty()) // 2. check if bucket is valid
          {
            drop = gc_->need_gc(LdbKey::decode_bucket_number(pkey), sequence, gc_->gc_buckets_);
          }
          if (!drop && !gc_->gc_areas_.empty()) // 3. check if area is valid
          {
            pkey += LDB_KEY_BUCKET_NUM_SIZE;
            // area decode, consensus with data_entry
            drop = gc_->need_gc(pkey[1] << 8 | pkey[0], sequence, gc_->gc_areas_);
          }
        }

        return drop;
      }

      const leveldb::Comparator* LdbComparator(LdbGcFactory* gc)
      {
        static const LdbComparatorImpl ldb_comparator(gc);
        return &ldb_comparator;
      }

    }
  }
}
