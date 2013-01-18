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
namespace tair {
  class tair_client_impl;
  struct SharedInfo;
  class PacketWrapper;
  class RequestProcessor;
  class TairGroup {
  public:
    TairGroup(const std::string &cluster_name, uint64_t master, uint64_t slave,
        const std::string &group_name);
    ~TairGroup();

    //inval_server commit the request
    void commit_request(SharedInfo *shared, bool merged, bool need_return_packet);

    inline tair_client_impl* get_tair_client()
    {
      return tair_client;
    }

    //sample the data indicated the status of tair group's healthy.
    void sampling(bool connected);

    inline const std::string& get_cluster_name()
    {
      return cluster_name;
    }

    inline const std::string& get_group_name()
    {
      return group_name;
    }

    inline bool is_healthy()
    {
      return atomic_read(&failed_count) < atomic_read(&max_failed_count);
    }
    inline void failed()
    {
      atomic_inc(&failed_count);
    }
    inline void successed()
    {
      if (atomic_sub_return(2, &failed_count) <= 0)
        atomic_set(&failed_count, 0);
    }
    inline void set_max_failed_count(int max_count)
    {
      atomic_set(&failed_count, max_count);
    }

    inline bool is_connected()
    {
      return connected;
    }
    inline void disconnected()
    {
      connected = false;
    }
    inline void set_client(tair_client_impl *client)
    {
      if (client == NULL)
        connected = false;
      else
      {
        tair_client = client;
        connected = true;
      }
    }
  protected:

    typedef void (RequestProcessor::*PROC_FUNC_T) (PacketWrapper *wrapper);
    void do_process_unmerged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet);
    void do_process_merged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet);

    void process_commit_request(PROC_FUNC_T func, SharedInfo *shared, bool merged, bool need_return_packet);
  protected:
    //tair client
    tair_client_impl *tair_client;

    //group
    std::string group_name;
    std::string cluster_name;
    uint64_t master;
    uint64_t slave;

    atomic_t failed_count;
    const static int DEFAULT_MAX_FAILED_COUNT = 10;
    atomic_t max_failed_count;
    bool connected;
  };
}
#endif
