/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb gc bucket or area.
 * Synchronization guaranteed, cause add/check may happen in different place,
 * outside synchronization can't be got.
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_GC_FACTORY_H
#define TAIR_STORAGE_LDB_GC_FACTORY_H

#include <stdint.h>
#include <ext/hash_map>

#include <tbsys.h>

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      struct GcNode
      {
        GcNode() {}
        GcNode(int64_t sequence, uint32_t when)
          : sequence_(sequence), when_(when) {}
        int64_t sequence_;
        uint32_t when_;
      };

      // area/bucket ==> GcNode (maybe set)
      typedef __gnu_cxx::hash_map<int32_t, GcNode> GC_MAP;
      typedef GC_MAP::iterator GC_MAP_ITER;

      // TODO: save
      class LdbGcFactory
      {
      public:
        LdbGcFactory();
        ~LdbGcFactory();
        friend class LdbComparatorImpl; // convient use

        int add_bucket(int32_t bucket_number, int64_t sequence, uint32_t when);
        int add_area(int32_t area, int64_t sequence, uint32_t when);

        // erase strategy:
        //   1. last_compact_round_over_time > gc_add_time
        // maybe: 
        //   2. bucket_range not in upper level
        //   3. area item count
        void finish(uint32_t last_gc_start_time);

        void finish_gc(uint32_t last_gc_start_time, GC_MAP& container);
        bool need_gc(int32_t key, int64_t sequence, GC_MAP& container);

      private:
        tbsys::CThreadMutex lock_;
        GC_MAP gc_buckets_;
        GC_MAP gc_areas_;
      };
    }
  }
}

#endif
