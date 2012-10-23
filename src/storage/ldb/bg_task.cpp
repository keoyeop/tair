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
        : stop_(false), db_(NULL), min_time_hour_(0), max_time_hour_(0),
          round_largest_filenumber_(0), is_compacting_(false)
      {
      }

      LdbCompactTask::~LdbCompactTask()
      {
        stop();
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

          const char* time_range = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_COMPACT_GC_RANGE, "2-7");
          if (!util::time_util::get_time_range(time_range, min_time_hour_, max_time_hour_))
          {
            log_warn("config compact hour range error: %s, use default 2-7", time_range);
          }
          log_info("compact hour range: %d - %d", min_time_hour_, max_time_hour_);
        }
        else
        {
          log_error("leveldb not init");
        }
        return ret;
      }

      void LdbCompactTask::stop()
      {
        stop_ = true;
      }

      void LdbCompactTask::reset()
      {
        log_warn("reset compact task");
        round_largest_filenumber_ = 0;
      }

      bool LdbCompactTask::should_compact()
      {
        tbsys::CThreadGuard guard(&lock_);
        bool ret = !stop_ && db_->gc_factory()->can_gc() && !is_compacting_ && is_compact_time() && need_compact();
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
        if (db_->gc_factory()->can_gc() && !db_->gc_factory()->empty())
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
        // db_->gc_factory()->try_evict();
        all_done = db_->gc_factory()->empty(gc_type);

        if (!all_done)          // not evict all
        {
          GcNode gc_node = db_->gc_factory()->pick_gc_node(gc_type);
          DUMP_GCNODE(info, gc_node, "pick gc node, type: %d", gc_type);
          if (gc_node.key_ < 0)
          {
            all_done = true;
            log_warn("[%d] gc all done, type: %d", db_->index(), gc_type);
          }
          else
          {
            uint32_t start_time = time(NULL);
            // get scan key to gc
            std::vector<ScanKey> gc_scan_keys;
            build_scan_key(gc_type, gc_node.key_, gc_scan_keys);

            if (round_largest_filenumber_ <= 0)
            {
              // new task round, reset largest_filenumber_,
              // 'cause in one new task round, every file whose filenumber is large than round_largest_filenumber
              // is generated after gced(ShouldDrop() filter)
              get_db_stat(db_->db(), round_largest_filenumber_, "largest-filenumber");
              log_warn("[%d] new task round, round_largest_filenumber: %"PRI64_PREFIX"u",
                       db_->index(), round_largest_filenumber_);
            }

            uint64_t limit_filenumber = std::max(round_largest_filenumber_, gc_node.file_number_);

            DUMP_GCNODE(warn, gc_node,
                        "[%d] gc type: %d, count: %d, start time: %s, limit filenumber: %"PRI64_PREFIX"u",
                        db_->index(), gc_type, gc_scan_keys.size(),
                        tair::util::time_util::time_to_str(start_time).c_str(), limit_filenumber);

            size_t i = 0;
            for (i = 0; !stop_ && db_->gc_factory()->can_gc() && i < gc_scan_keys.size(); ++i)
            {
              leveldb::Slice comp_start(gc_scan_keys[i].start_key_), comp_end(gc_scan_keys[i].end_key_);
              leveldb::Status status = db_->db()->
                CompactRangeSelfLevel(limit_filenumber, &comp_start, &comp_end);

              if (!status.ok())
              {
                DUMP_GCNODE(error, gc_node, "[%d] gc fail. type: %d, error: %s",
                            db_->index(), gc_type, status.ToString().c_str());
                break;
              }
              // just have a rest..
              ::sleep(1);
            }

            if (i >= gc_scan_keys.size()) // all is ok
            {
              DUMP_GCNODE(warn, gc_node, "[%d] gc success. type: %d, cost: %u",
                          db_->index(), gc_type, (time(NULL) - start_time));
              db_->gc_factory()->remove(gc_node, gc_type);
              // // may can evict some
              // db_->gc_factory()->try_evict();
              all_done = db_->gc_factory()->empty(gc_type);
              if (all_done)
              {
                log_warn("[%d] gc all done, type: %d", db_->index(), gc_type);
              }
            }
          }
        }
      }

      void LdbCompactTask::build_scan_key(GcType type, int32_t key, std::vector<ScanKey>& scan_keys)
      {
        switch (type)
        {
        case GC_BUCKET:
        {
          ScanKey scan_key;
          LdbKey::build_scan_key(key, scan_key.start_key_, scan_key.end_key_);
          scan_keys.push_back(scan_key);
          break;
        }
        case GC_AREA:
        {
          // build each scan key with one bucket with area
          std::vector<int32_t> buckets;
          db_->get_buckets(buckets);
          for (size_t i = 0; i < buckets.size(); ++i)
          {
            ScanKey scan_key;
            LdbKey::build_scan_key_with_area(buckets[i], key, scan_key.start_key_, scan_key.end_key_);
            scan_keys.push_back(scan_key);
          }
          break;
        }
        default:
          log_error("[%d] invalid gc type to build scan key, type: %d, key: %d", db_->index(), type, key);
          break;
        }
      }

      bool LdbCompactTask::is_compact_time()
      {
        return (min_time_hour_ != max_time_hour_) && util::time_util::is_in_range(min_time_hour_, max_time_hour_);
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
          timer_->destroy();
          timer_ = 0;
        }
      }

      void BgTask::restart()
      {
        if (compact_task_ != 0)
        {
          compact_task_->reset();
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
          compact_task_->stop();
          compact_task_ = 0;
        }
      }
    }
  }
}
