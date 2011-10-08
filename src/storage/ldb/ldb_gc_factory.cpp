/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb gc bucket or area
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/log.hpp"
#include "ldb_gc_factory.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      LdbGcFactory::LdbGcFactory()
      {
      }

      LdbGcFactory::~LdbGcFactory()
      {
      }

      int LdbGcFactory::add_bucket(int32_t bucket_number, int64_t sequence, uint32_t when)
      {
        tbsys::CThreadGuard guard(&lock_);
        gc_buckets_[bucket_number] = GcNode(sequence, when);
        log_debug("add gc bucket: %d, seq: %d, when: %u, size: %d", bucket_number, sequence, when, gc_buckets_.size());
        return TAIR_RETURN_SUCCESS;
      }

      int LdbGcFactory::add_area(int32_t area, int64_t sequence, uint32_t when)
      {
        tbsys::CThreadGuard guard(&lock_);
        gc_areas_[area] = GcNode(sequence, when);
        log_debug("add gc area: %d, seq: %d, when: %u, size: %d", area, sequence, when, gc_areas_.size());
        return TAIR_RETURN_SUCCESS;
      }

      void LdbGcFactory::finish(uint32_t last_gc_start_time)
      {
        if (last_gc_start_time > 0)
        {
          tbsys::CThreadGuard guard(&lock_);
          log_debug("before finish gc. gc_buckets: %d, gc_areas: %d", gc_buckets_.size(), gc_areas_.size());
          finish_gc(last_gc_start_time, gc_buckets_);
          finish_gc(last_gc_start_time, gc_areas_);
          log_debug("after finish gc. gc_buckets: %d, gc_areas: %d", gc_buckets_.size(), gc_areas_.size());
        }
      }

      // user should hold lock_
      void LdbGcFactory::finish_gc(uint32_t last_gc_start_time, GC_MAP& container)
      {
        std::vector<int32_t> erase_keys;
        for (GC_MAP_ITER it = container.begin(); it != container.end(); ++it)
        {
          // add gc before gc start, then this gc round already gc all what you want (TODO: memtable consider);
          if (it->second.when_ < last_gc_start_time)
          {
            erase_keys.push_back(it->first);
          }
        }
        for (std::vector<int32_t>::iterator it = erase_keys.begin(); it != erase_keys.end(); ++it)
        {
          log_debug("gc finish %d", *it);
          container.erase(*it);
        }
      }

      // user should hold lock_
      bool LdbGcFactory::need_gc(int32_t key, int64_t sequence, GC_MAP& container)
      {
        bool need_gc = false;
        GC_MAP_ITER it = container.find(key);
        fprintf(stderr, "drop bucket/area %d, seq %lld <> %lld\n", key, sequence, it != container.end() ? it->second.sequence_ : -1);
        if (it != container.end() && it->second.sequence_ >= sequence)
        {
          need_gc = true;
        }
        return need_gc;
      }

    }
  }
}
