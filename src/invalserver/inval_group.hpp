/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: inval_group.hpp 28 2012-12-20 05:18:09 fengmao.pj@taobao.com $
 *
 * Authors:
 *   fengmao <fengmao.pj@taobao.com>
 *
 */
#ifndef INVAL_GROUP_H
#define INVAL_GROUP_H
#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "log.hpp"
#include "base_packet.hpp"
#include <string>
#include <queue>
#include "inval_request_packet_wrapper.hpp"
#include "inval_processor.hpp"
namespace tair {
  class tair_client_impl;
  class InvalRetryThread;
  class TairGroup {
  public:
    TairGroup(uint64_t master, uint64_t slave, const std::string &group_name, tair_client_impl *tair_client);
    ~TairGroup();

    //inval_server commit the request, invoked in inval_server's funcation named `handlePacketQueue.
    void commit_request(SharedInfo *shared, bool merged, bool need_return_packet);

    //retry_thread commit the request, invoked in retry_thread's funcation named `run.
    inline void commit_request(data_entry * key, SharedInfo *shared)
    {
      SingleWrapper *wrapper = new SingleWrapper(this, shared, key);
      do_commit_request(wrapper);
    }

    //retry_thread commit the request, invoked in retry_thread's funcation named `run.
    inline void commit_request(tair_dataentry_set * keys, SharedInfo *shared)
    {
      MultiWrapper *wrapper = new MultiWrapper(this, shared, keys);
      do_commit_request(wrapper);
    }

    inline void inc_uninvokeded_callback_count()
    {
      atomic_inc(&uninvoked_callback_count);
    }

    inline void dec_uninvoked_callback_count()
    {
      atomic_dec(&uninvoked_callback_count);
    }

    inline int get_uninvoked_callback_count()
    {
      return atomic_read(&uninvoked_callback_count);
    }

    inline int get_request_timeout_count()
    {
      return atomic_read(&request_timeout_count);
    }

    inline void inc_request_timeout_count()
    {
      atomic_inc(&request_timeout_count);
    }

    inline void reset_request_timout_count()
    {
      atomic_set(&request_timeout_count, 0);
    }

    inline tair_client_impl* get_tair_client()
    {
      return tair_client;
    }

    //sample the data indicated the status of tair group's healthy.
    void sampling();

  protected:
    bool is_healthy();
    void do_commit_request(PacketWrapper* wrapper);
    inline void cache_request(PacketWrapper *wrapper, bool need_return_packet)
    {
        wrapper->set_needed_return_packet(need_return_packet);
        wrapper->inc_request_delay_count();
        wrapper->set_request_status(RETRY_COMMIT);

        if (wrapper->dec_and_return_reference_count(1) == 0)
        {
          REQUEST_PROCESSOR.end_request(wrapper);
        }
    }
  protected:
    atomic_t uninvoked_callback_count;
    atomic_t uninvoked_callback_count_average;

    atomic_t request_timeout_count;
    atomic_t request_timeout_count_average;

    atomic_t healthy;
    enum
    {
      HEALTHY = 0,
      SICK = 1
    };

    //tair client
    tair_client_impl *tair_client;
    InvalRetryThread *retry_thread;

    //group
    std::string group_name;
    uint64_t master;
    uint64_t slave;

    //data of group's healthy
    static const int DATA_ITEM_SIZE;
    std::vector<atomic_t> uninvoked_callback_sampling_data;
    std::vector<atomic_t> timeout_count_sampling_data;
    int head;
    int tail;
  };
}
#endif
