/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/log.hpp"

#include "cluster_info_updater.hpp"
#include "cluster_handler_manager.hpp"
#include "tair_multi_cluster_client_api.hpp"

namespace tair
{
  tair_multi_cluster_client_api::tair_multi_cluster_client_api()
  {
    updater_ = new cluster_info_updater();
    handler_mgr_ = new bucket_shard_cluster_handler_manager();
  }

  tair_multi_cluster_client_api::~tair_multi_cluster_client_api()
  {
    delete updater_;
    delete handler_mgr_;
  }

  bool tair_multi_cluster_client_api::startup(const char* master_addr, const char* slave_addr, const char* group_name)
  {
    if (NULL == master_addr || group_name == NULL)
    {
      log_error("invalid master address/group_name");
      return false;
    }

    updater_->init(handler_mgr_, master_addr, slave_addr, group_name);
    bool urgent;
    int ret = updater_->update_cluster_info(urgent);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("update cluster info fail. ret: %d", ret);
      return false;
    }

    updater_->start();

    return true;
  }

  int tair_multi_cluster_client_api::get(int area, const data_entry& key, data_entry*& data)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ONE_CLUSTER_HANDLER_OP(ret, key, get(area, key, data));
    return ret;
  }

    int tair_multi_cluster_client_api::put(int area, const data_entry& key, const data_entry& data,
                                         int expire, int version, bool fill_cache)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, put(area, key, data, expire, version, fill_cache));
    return ret;
  }

  int tair_multi_cluster_client_api::remove(int area, const data_entry& key)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, remove(area, key));
    return ret;
  }

  int tair_multi_cluster_client_api::incr(int area, const data_entry& key, int count, int* ret_count,
                                          int init_value, int expire)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, add_count(area, key, count, ret_count, init_value, expire));
    return ret;
  }

  int tair_multi_cluster_client_api::decr(int area, const data_entry& key, int count, int* ret_count,
                                          int init_value, int expire)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, add_count(area, key, (-count), ret_count, init_value, expire));
    return ret;
  }

  int tair_multi_cluster_client_api::add_count(int area, const data_entry& key, int count, int* ret_count, int init_value)
  {
    int ret = TAIR_RETURN_SERVER_CAN_NOT_WORK;
    ALL_CLUSTER_HANDLER_OP(ret, add_count(area, key, count, ret_count, init_value));
    return ret;
  }

  void tair_multi_cluster_client_api::set_timeout(int timeout_ms)
  {
    handler_mgr_->set_timeout(timeout_ms);
  }

  void tair_multi_cluster_client_api::close()
  {
    updater_->stop();
    handler_mgr_->close();
  }

}
