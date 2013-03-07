/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ldb balance bucket to instance
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include <tbsys.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb/options.h"

#include "ldb_manager.hpp"
#include "ldb_instance.hpp"
#include "ldb_balancer.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      //////////////////// LdbBalancer::Balancer
      LdbBalancer::Balancer::Balancer(LdbBalancer* owner, LdbManager* manager) :
        owner_(owner), manager_(manager)
      {
      }

      LdbBalancer::Balancer::~Balancer()
      {
        owner_->finish(this);
      }

      void LdbBalancer::Balancer::run(tbsys::CThread *thread, void *arg)
      {
        pthread_detach(pthread_self());

        BucketIndexer::INDEX_BUCKET_MAP index_map;
        // get current bucket index map
        manager_->get_index_map(index_map);
        // get unit to be balanced
        std::vector<Unit> units;
        try_balance(index_map, units);
        if (!units.empty())
        {
          // do_balance() process may be interrupted,
          // so we need continue to balance later
          do_balance(units);
        }
        delete this;
      }

      void LdbBalancer::Balancer::try_balance(const BucketIndexer::INDEX_BUCKET_MAP& index_map, std::vector<Unit>& result_units)
      {
        result_units.clear();

        int32_t bucket_count = 0;
        for (BucketIndexer::INDEX_BUCKET_MAP::const_iterator it = index_map.begin();
             it != index_map.end();
             ++it)
        {
          bucket_count += it->second.size();
        }

        int32_t average = bucket_count / index_map.size();
        int32_t remainder = bucket_count % index_map.size();

        BucketIndexer::INDEX_BUCKET_MAP result_index_map;

        // first pull out
        for (BucketIndexer::INDEX_BUCKET_MAP::const_iterator it = index_map.begin();
             it != index_map.end();
             ++it)
        {
          const std::vector<int32_t>& buckets = it->second;
          std::vector<int32_t>::const_iterator bit = buckets.begin();
          int32_t more = buckets.size() - average;

          if (more > 0)
          {
            if (remainder > 0)
            {
              remainder--;
              more--;
            }
            if (more > 0)
            {
              // this bucket need balance later
              for (int i = 0; i < more; ++i)
              {
                // pull out the more
                result_units.push_back(Unit(*bit++, it->first, 0));
              }
            }
          }

          // add out-of-balance ones to result index map
          result_index_map[it->first] = std::vector<int32_t>(bit, buckets.end());
        }


        if (result_units.empty())
        {
          log_warn("NO NEED balance, input: %s", BucketIndexer::to_string(index_map).c_str());
        }
        else
        {
          // second push in
          std::vector<Unit>::iterator uit = result_units.begin();
          for (BucketIndexer::INDEX_BUCKET_MAP::iterator it = result_index_map.begin();
               it != result_index_map.end();
               ++it)
          {
            int32_t less = average - it->second.size();
            if (less >= 0)
            {
              if (remainder > 0)
              {
                less++;
                remainder--;
              }

              if (less > 0)
              {
                // balance some
                for (int32_t i = 0; i < less; ++i, ++uit)
                {
                  uit->to_ = it->first;
                  it->second.push_back(uit->bucket_);
                }
              }
            }
          }

          std::string units_str;
          for (std::vector<Unit>::iterator it = result_units.begin(); it != result_units.end(); ++it)
          {
            units_str.append(it->to_string());
          }

          log_warn("NEED balance. input: %s, output: %d, balance units: %s",
                   BucketIndexer::to_string(index_map).c_str(),
                   BucketIndexer::to_string(result_index_map).c_str(),
                   units_str.c_str());
        }
      }

      bool LdbBalancer::Balancer::do_balance(const std::vector<Unit>& units)
      {
        int ret = TAIR_RETURN_SUCCESS;
        for (std::vector<Unit>::const_iterator it = units.begin();
             it != units.end() && !_stop;
             ++it)
        {
          log_warn("start one balance: %s", it->to_string().c_str());
          ret = do_one_balance(it->bucket_,
                               manager_->get_instance(it->from_),
                               manager_->get_instance(it->to_));
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("balance one bucket fail: %s", it->to_string().c_str());
            break;
          }
        }

        return ret != TAIR_RETURN_SUCCESS ? false : true;
      }

      int LdbBalancer::Balancer::do_one_balance(int32_t bucket, LdbInstance* from, LdbInstance* to)
      {
        leveldb::DB* from_db = from->db();
        leveldb::DB* to_db = to->db();

        int32_t batch_size = 0;
        int64_t item_count = 0, data_size = 0;
        leveldb::WriteBatch batch;

        leveldb::WriteOptions write_options;
        write_options.sync = false;
        leveldb::Status status;

        // process
        typedef enum
        {
          // balancing is doing(data is moving)
          DOING = 0,
          // data moving is almost over, propose to switch
          PROPOSING,
          // all done, commit over
          COMMIT,
        } Process;

        // input data iterator
        LdbBucketDataIter data_it(bucket, from_db);
        data_it.seek_to_first();

        // here we go...
        Process process = DOING;
        while (process != COMMIT && !_stop)
        {
          if (!data_it.valid())
          {
            if (process == DOING)
            {
              // we got the last record, just pause write for switch.
              // Actually, data_it chases the normal write request,
              // we just expect data_it is faster.
              // TODO: stop write earlier if we are tired
              //       of this chasing game(already be closer enough eg.).
              manager_->pause_service(bucket);
              // try to get the possible remaining data
              data_it.next();
              // now, we are proposing to be done
              process = PROPOSING;
            }
            else if (process == PROPOSING)
            {
              // Now, all data has been got really,
              // write last batch if existed
              if (batch_size > 0)
              {
                // trigger last write
                batch_size = MAX_BATCH_SIZE + 1;
              }
              // all done
              process = COMMIT;
            }
          }

          if (data_it.valid())
          {
            leveldb::Slice& key = data_it.key();
            leveldb::Slice& value = data_it.value();

            batch_size += key.size() + value.size();
            data_size += key.size() + value.size();
            ++item_count;

            if (value.empty())
            {
              batch.Delete(key);
            }
            else
            {
              batch.Put(key, value);
            }

            data_it.next();
          }

          // batch over
          if (batch_size > MAX_BATCH_SIZE)
          {
            status = to_db->Write(write_options, &batch, bucket);
            if (!status.ok())
            {
              log_error("write batch fail: %s", status.ToString().c_str());
              // we will break
              break;
            }
            batch_size = 0;
          }
        }

        int ret = status.ok() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;

        if (process >= COMMIT && ret == TAIR_RETURN_SUCCESS)
        {
          to_db->ForceCompactMemTable();
          ret = manager_->reindex_bucket(bucket, from->index(), to->index());
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("reindex bucket %d fail, ret: %d", bucket, ret);
          }
        }

        if (process >= PROPOSING)
        {
          // has paused, must resume
          manager_->resume_service(bucket);
        }

        log_warn("balance bucket: %d, itemcount: %"PRI64_PREFIX"d, datasize: %"PRI64_PREFIX"d, suc: %s",
                 bucket, item_count, data_size, ret == TAIR_RETURN_SUCCESS ? "yes" : "no");

        return ret;
      }

      //////////////////// LdbBalancer
      LdbBalancer::LdbBalancer(LdbManager* manager) :
        manager_(manager), balancer_(NULL)
      {}

      LdbBalancer::~LdbBalancer()
      {
        if (balancer_ != NULL)
        {
          balancer_->stop();
          // we need wait for balancer finish
          // we can't use wait() `cause balancer_
          // has detached itself
          while (balancer_ != NULL)
          {
            ::usleep(20);
          }
        }
      }

      void LdbBalancer::start()
      {
        if (balancer_ != NULL)
        {
          log_warn("balance already in process");
        }
        else
        {
          balancer_ = new Balancer(this, manager_);
          balancer_->start();
          log_warn("balance start.");
        }
      }

      void LdbBalancer::stop()
      {
        if (balancer_ == NULL)
        {
          log_warn("no balance in process");
        }
        else
        {
          balancer_->stop();
        }
      }

      void LdbBalancer::finish(Balancer* balancer)
      {
        if (balancer == balancer_)
        {
          log_warn("balance finish.");
          balancer_ = NULL;
        }
      }

    }
  }
}
