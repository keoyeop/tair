/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage compact manager
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_BG_TASK_H
#define TAIR_STORAGE_LDB_BG_TASK_H

#include "Timer.h"

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
      class LdbInstance;
      class LdbCompact;

      class LdbCompactTask : public tbutil::TimerTask
      {
        typedef struct
        {
          std::string start_key_;
          std::string end_key_;
        } ScanKey;

      public:
        LdbCompactTask();
        virtual ~LdbCompactTask();
        virtual void runTimerTask();

        bool init(LdbInstance* db);
        void stop();
        void reset();

      private:
        bool is_compact_time();
        bool need_compact();
        bool should_compact();
        void do_compact();

        void compact_for_gc();
        void compact_gc(GcType gc_type, bool& all_done);
        void compact_for_expired();

        void build_scan_key(GcType type, int32_t key, std::vector<ScanKey>& scan_keys);

      private:
        bool stop_;
        LdbInstance* db_;
        int32_t min_time_hour_;
        int32_t max_time_hour_;
        // largest filenumber in this task round
        uint64_t round_largest_filenumber_;
        tbsys::CThreadMutex lock_;
        bool is_compacting_;
      };
      typedef tbutil::Handle<LdbCompactTask> LdbCompactTaskPtr;

      class BgTask
      {
      public:
        BgTask();
        ~BgTask();

        bool start(LdbInstance* db);
        void stop();
        void restart();

      private:
        bool init_compact_task(LdbInstance* db);
        void stop_compact_task();

      private:
        tbutil::TimerPtr timer_;
        LdbCompactTaskPtr compact_task_;
      };
    }
  }
}
#endif
