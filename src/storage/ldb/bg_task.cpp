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
#include "ldb_bucket.hpp"
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
        : db_(NULL), min_time_(0), max_time_(0),
          is_compacting_(false), last_compact_round_over_time_(0), should_compact_level_(0)
      {
      }

      LdbCompactTask::~LdbCompactTask()
      {
      }

      void LdbCompactTask::runTimerTask()
      {
        log_debug("run compact timer task");
        if (should_compact())
        {
          do_compact();
        }
      }

      bool LdbCompactTask::init(LdbBucket* db)
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

      bool LdbCompactTask::should_compact()
      {
        tbsys::CThreadGuard guard(&lock_);
        return !is_compacting_ && is_compact_time() && can_compact();
      }

      bool LdbCompactTask::can_compact()
      {
        // TODO: check load io, etc. .. maybe
        return true;
      }

      // leveldb compact has its's own mutex
      void LdbCompactTask::do_compact()
      {
        log_debug("do compact now level: %d, last time: %u, is_compacting: %d", should_compact_level_, last_compact_round_over_time_, is_compacting_);
        // level count does not change once db started.
        static int32_t level_num = get_level_num(db_->db()) - 1;

        bool can_compact = false;
        {
          tbsys::CThreadGuard guard(&lock_);
          can_compact = !is_compacting_;
          if (!is_compacting_)
          {
            is_compacting_ = true;
          }
        }

        if (can_compact)
        {
          if (0 == last_compact_round_over_time_ && 0 == should_compact_level_) // first compact
          {
            last_compact_round_over_time_ = time(NULL);
          }

          std::string range_smallest, range_largest;
          while (should_compact_level_ < level_num)
          {
            if (!get_level_range(db_->db(), should_compact_level_, &range_smallest, &range_largest))
            {
              log_error("compact fail when get level [%d] range.", should_compact_level_);
              break;
            }
            else if (!range_smallest.empty() && !range_largest.empty()) // can compact
            {
              // compact each level.
              // TODO: Test compaction's load to tune other strategy
              uint32_t start_time = time(NULL);
              log_debug("compact [%d] start %u", should_compact_level_, start_time);
              db_->db()->CompactRange(should_compact_level_, range_smallest, range_largest);
              log_debug("compact [%d] end cost: %u", should_compact_level_, time(NULL) - start_time);

              ++should_compact_level_;
              break;
            }
            else
            {
              log_debug("current level [%d] has no file.", should_compact_level_);
              ++should_compact_level_;
            }
          }

          if (should_compact_level_ >= level_num)
          {
            // finish today
            should_compact_level_ = -1;
            uint32_t save_last_compact_round_over_time = last_compact_round_over_time_;
            last_compact_round_over_time_ = time(NULL);
            // finish gc
            db_->gc_factory()->finish(save_last_compact_round_over_time);
          }

          is_compacting_ = false;
        }
      }

      bool LdbCompactTask::is_compact_time()
      {
        bool ret = false;
        if (!(ret = (should_compact_level_ >= 0)) && (time(NULL) - last_compact_round_over_time_ > COMPACT_PAUSE_TIME))
        {
          ret = true;
          should_compact_level_ = 0;
        }

        if (ret)
        {
          ret = is_in_range(min_time_, max_time_);
        }
        return ret;
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

      bool BgTask::start(LdbBucket* db)
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

      bool BgTask::init_compact_task(LdbBucket* db)
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
