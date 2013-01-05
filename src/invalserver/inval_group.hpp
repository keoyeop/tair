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
  class TairGroup {
  public:
    TairGroup(uint64_t master, uint64_t slave, const std::string &group_name, tair_client_impl *tair_client);
    ~TairGroup();

    //inval_server commit the request, invoked in inval_server's funcation named `handlePacketQueue.
    void commit_request(SharedInfo *shared, bool merged, bool need_return_packet);

    //retry_thread commit the request, invoked in retry_thread's funcation named `run.
    void retry_commit_request(PacketWrapper *wrapper, bool merged);

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

    inline void reset_request_timeout_count()
    {
      atomic_set(&request_timeout_count, 0);
    }

    inline tair_client_impl* get_tair_client()
    {
      return tair_client;
    }

    //sample the data indicated the status of tair group's healthy.
    void sampling();

    inline void set_uninvoked_callback_count_limit(int limit)
    {
      atomic_set(&uninvoked_callback_count_limit, limit);
    }

    inline void set_request_timeout_count_limit(int limit)
    {
      atomic_set(&request_timeout_count_limit, limit);
    }
  protected:
    inline bool is_healthy()
    {
      return atomic_read(&healthy) == HEALTHY;
    }
    void do_retry_commit_request(PacketWrapper* wrapper);

    typedef void (RequestProcessor::*PROC_FUNC_T) (PacketWrapper *wrapper);
    void do_process_unmerged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet);
    void do_process_merged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet);

    void process_commit_request(PROC_FUNC_T func, SharedInfo *shared, bool merged, bool need_return_packet);
  protected:
    //tair client
    tair_client_impl *tair_client;

    //group
    std::string group_name;
    uint64_t master;
    uint64_t slave;

    //data of group's healthy
    //record the accumulated count of callback functions, that wait to be invoked.
    atomic_t uninvoked_callback_count;
    //the limit value of `uninvoked_callback_count.
    atomic_t uninvoked_callback_count_limit;

    //record the accumulated count of timeout for some time.
    atomic_t request_timeout_count;
    //the limit value of `request_timeout_count.
    atomic_t request_timeout_count_limit;

    //default value of the limit.
    enum
    {
      DEFAULT_UNINVOKED_CALLBACK_COUNT_LIMIT = 1000000,
      DEFAULT_REQUEST_TIMEOUT_COUNT_LIMIT = 100000
    };

    atomic_t healthy;
    enum
    {
      HEALTHY = 0,
      SICK = 1
    };
    static const size_t DATA_ITEM_SIZE;
    //sampling the data of `uninvoked_callback_count.
    std::vector<atomic_t> uninvoked_callback_sampling_data;
    //sampling the data of `request_timeout_count.
    std::vector<atomic_t> request_timeout_sampling_data;
    int tail;
  };
}
#endif
