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

#include "common/define.hpp"
#include "common/log.hpp"
#include "ldb_instance.hpp"
#include "ldb_define.hpp"
#include "bg_task.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      ////////// LdbCompactTask
      LdbCompactTask::LdbCompactTask()
        : db_(NULL), min_time_(0), max_time_(0), is_compacting_(false), paused_(false)
      {
      }

      LdbCompactTask::~LdbCompactTask()
      {
      }

      void LdbCompactTask::runTimerTask()
      {
        if (should_compact())
        {
          do_compact();
          is_compacting_ = false;
        }
      }

      bool LdbCompactTask::init(LdbInstance* db)
      {
        bool ret = db != NULL;
        if (ret)
        {
          db_ = db;

          const char* time_range = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_COMPACT_RANGE, "2-7");
          if (!get_time_range(time_range, min_time_, max_time_))
          {
            log_warn("config compact hour range error: %s, use default 2-7", time_range);
          }
          log_info("compact hour range: %d - %d", min_time_, max_time_);
        }
        else
        {
          log_error("leveldb not init");
        }
        return ret;
      }

      void LdbCompactTask::pause()
      {
        paused_ = true;        
      }

      void LdbCompactTask::resume()
      {
        paused_ = false;
      }

      bool LdbCompactTask::should_compact()
      {
        tbsys::CThreadGuard guard(&lock_);
        bool ret = !paused_ && !is_compacting_ && is_compact_time() && need_compact();
        if (ret)
        {
          is_compacting_ = true;
        }
        return ret;
      }

      bool LdbCompactTask::need_compact()
      {
        // // TODO: check load io, etc. .. maybe
        return true;
      }

      // leveldb compact has its's own mutex
      void LdbCompactTask::do_compact()
      {
        // Compact should consider gc (bucket/area) items and expired item.
        // 1. Gc's priority is highest.
        //    Rules:
        //    1). we compare current leveldb smallest filenumber with
        //        gc's file_number to determine whether this file need compact.
        //        Only files whose file_number <= smallest gc's file_number need compact.
        //    2). After Rule-1, if gc_buckets is not empty, cause buckets number matters to key range,
        //        so first compact files whose range is in gc_buckets' range.
        //    3). If Rule-2 run and gc-buckets is over, gc area. Based on key format, area is matter to range someway,
        //        compact gc-areas.
        // 2. Expired items has nothing to do with range and filenumber, so if we want to compact over all expired
        //    items, we can do nothing except compacting the whole db(that's expensive). Maybe some statics can
        //    join in the strategy.
        compact_for_gc();
        compact_for_expired();
      }

      void LdbCompactTask::compact_for_gc()
      {
        if (!db_->gc_factory()->empty())
        {
          bool all_done = false;
          compact_gc(GC_BUCKET, all_done);
          if (all_done)
          {
            compact_gc(GC_AREA, all_done);
          }
        }
      }

      void LdbCompactTask::compact_for_expired()
      {
        // TODO
      }

      void LdbCompactTask::compact_gc(GcType gc_type, bool& all_done)
      {
        log_debug("compact gc %d", gc_type);
        db_->gc_factory()->try_evict();
        all_done = db_->gc_factory()->empty(gc_type);

        if (!all_done)          // not evict all
        {
          GcNode gc_node = db_->gc_factory()->pick_gc_node(gc_type);
          std::string start_key, end_key;
          DUMP_GCNODE(info, gc_node, "pick gc node, type: %d", gc_type);
          if (gc_node.key_ < 0)
          {
            all_done = true;
          }
          else
          {
            switch (gc_type) {
            case GC_BUCKET:
              LdbKey::build_scan_key(gc_node.key_, start_key, end_key);
              break;
            case GC_AREA:
              LdbKey::build_scan_key_with_area(gc_node.key_, start_key, end_key);
              break;
            default:
              log_error("invalid compact gc type: %d", gc_type);
              break;
            }
            uint32_t start_time = time(NULL);
            DUMP_GCNODE(info, gc_node, "compact for gc type: %d, start: %u", gc_type, start_time);

            leveldb::Slice comp_start(start_key), comp_end(end_key);
            leveldb::Status status = db_->db()->
              CompactRangeSelfLevel(gc_node.file_number_, &comp_start, &comp_end);

            if (!status.ok())
            {
              DUMP_GCNODE(error, gc_node, "compact for gc fail. type: %d, error: %s",
                          gc_type, status.ToString().c_str());
            }
            else
            {
              DUMP_GCNODE(info, gc_node, "compact for gc success. type: %d, cost: %u",
                          gc_type, (time(NULL) - start_time));
              db_->gc_factory()->remove(gc_node, gc_type);
              // may can evict some
              db_->gc_factory()->try_evict();
              all_done = db_->gc_factory()->empty(gc_type);
            }
          }
        }
      }

      bool LdbCompactTask::is_compact_time()
      {
        return (min_time_ != max_time_) && is_in_range(min_time_, max_time_);
      }

      bool LdbCompactTask::get_time_range(const char* str, int32_t& min, int32_t& max)
      {
        bool ret = str != NULL;

        if (ret)
        {
          int32_t tmp_min = 0, tmp_max = 0;
          char buf[32];
          char* max_p = strncpy(buf, str, sizeof(buf));
          char* min_p = strsep(&max_p, "-~");

          if (min_p != NULL && min_p[0] != '\0')
          {
            tmp_min = atoi(min_p);
          }
          if (max_p != NULL && max_p[0] != '\0')
          {
            tmp_max = atoi(max_p);
          }

          if ((ret = tmp_min >= 0 && tmp_max >= 0))
          {
            min = tmp_min;
            max = tmp_max;
          }
        }

        return ret;
      }

      bool LdbCompactTask::is_in_range(int32_t min, int32_t max)
      {
        time_t t = time(NULL);
        struct tm *tm = localtime((const time_t*) &t);
        bool reverse = false;
        if (min > max)
        {
          std::swap(min, max);
          reverse = true;
        }
        bool in_range = tm->tm_hour >= min && tm->tm_hour <= max;
        return reverse ? !in_range : in_range;
      }


      ////////// BgTask
      BgTask::BgTask() : timer_(0), compact_task_(0)
      {

      }

      BgTask::~BgTask()
      {
        stop();
      }

      bool BgTask::start(LdbInstance* db)
      {
        bool ret = db != NULL;
        if (ret && timer_ == 0)
        {
          timer_ = new tbutil::Timer();
          ret = init_compact_task(db);
        }
        return ret;
      }

      void BgTask::stop()
      {
        if (timer_ != 0)
        {
          stop_compact_task();
          timer_ = 0;
        }
      }

      void BgTask::pause()
      {
        log_error("pause bgtask");
        if (compact_task_ != 0)
        {
          compact_task_->pause();
        }
      }

      void BgTask::resume()
      {
        log_error("resume bgtask");
        if (compact_task_ != 0)
        {
          compact_task_->resume();
        }
      }

      bool BgTask::init_compact_task(LdbInstance* db)
      {
        bool ret = false;
        if (timer_ != 0)
        {
          compact_task_ = new LdbCompactTask();
          if ((ret = compact_task_->init(db)))
          {
            ret =
              timer_->scheduleRepeated(compact_task_, tbutil::Time::seconds(
                                         TBSYS_CONFIG.getInt(TAIRLDB_SECTION, LDB_CHECK_COMPACT_INTERVAL, 60))) == 0;
            log_info("schedule compact task %d", ret);
          }
          else
          {
            log_error("init compact task fail");
          }
        }
        return ret;
      }

      void BgTask::stop_compact_task()
      {
        if (timer_ != 0 && compact_task_ != 0)
        {
          timer_->cancel(compact_task_);
          compact_task_ = 0;
        }
      }
    }
  }
}
