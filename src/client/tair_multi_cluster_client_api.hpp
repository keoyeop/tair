/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_multi_cluster_client_api.hpp 690 2012-04-09 02:09:34Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */
#ifndef TAIR_CLIENT_MULTI_CLUSTER_CLIENT_API_H
#define TAIR_CLIENT_MULTI_CLUSTER_CLIENT_API_H

namespace tair
{
  class cluster_info_updater;
  class cluster_handler_manager;

  using tair::common::data_entry;
  class tair_multi_cluster_client_api
  {
  public:
    tair_multi_cluster_client_api();
    ~tair_multi_cluster_client_api();

    bool startup(const char* master_addr, const char* slave_addr, const char* group_name);

    int get(int area, const data_entry& key, data_entry*& data);
#if 1
    int put(int area, const data_entry& key, const data_entry& data, int expire, int version, bool fill_cache = true);
    int remove(int area, const data_entry& key);
    int incr(int area, const data_entry& key, int count, int* ret_count, int init_value = 0, int expire = 0);
    int decr(int area, const data_entry& key, int count, int* ret_count, int init_value = 0, int expire = 0);
    int add_count(int area, const data_entry& key, int count, int* ret_count, int init_value = 0);
#endif
    void set_update_interval(int32_t interval_ms);
    void set_timeout(int32_t timeout_ms);
    void set_log_level(const char* level);
    void close();

  private:
    cluster_info_updater* updater_;
    cluster_handler_manager* handler_mgr_;
  };
}
#endif
