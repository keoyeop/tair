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

#include "cluster_info_updater.hpp"

namespace tair
{
  cluster_info_updater::cluster_info_updater() :
    inited_(false), interval_s_(DEFAULT_CLUSTER_UPDATE_INTERVAL_S), stopped_(false),
    version_(0), manager_(NULL)
  {
  }

  cluster_info_updater::~cluster_info_updater()
  {
    stop();
    wait();
  }

  void cluster_info_updater::init(cluster_handler_manager* manager,
                             const char* master_cs_addr, const char* slave_cs_addr, const char* group_name,
                             int64_t interval_s)
  {
    if (NULL != manager && NULL != master_cs_addr && NULL != group_name)
    {
      master_handler_.init(master_cs_addr, slave_cs_addr, group_name);
      this->manager_ = manager;
    }
  }

  void cluster_info_updater::run(tbsys::CThread*, void*)
  {
    static const int32_t FAIL_UPDATE_CLUSTER_INFO_INTERVAL_S = 1;
    static const int32_t URGENT_UPDATE_CLUSTER_INFO_INTERVAL_S = 1;

    int ret;
    bool urgent = false;
    int32_t interval = interval_s_;
    while (!_stop)
    {
      ret = update_cluster_info(urgent);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        interval = FAIL_UPDATE_CLUSTER_INFO_INTERVAL_S;
        log_error("update cluster info fail: %d. retry after %d(s).", interval, ret);
      }
      else if (urgent)
      {
        interval = URGENT_UPDATE_CLUSTER_INFO_INTERVAL_S;
        log_warn("urgent condition, maybe all cluster are dead. retry after %d(s)", interval);
      }
      TAIR_SLEEP(stopped_, interval);
      interval = interval_s_;
    }
  }

  int cluster_info_updater::update_cluster_info(bool& urgent)
  {
    if (!inited_ && !(inited_ = master_handler_.start()))
    {
      return TAIR_RETURN_FAILED;
    }

    urgent = false;
    CLUSTER_INFO_LIST cluster_infos;
    uint32_t new_version = 0;
    int ret = retrieve_cluster_info(cluster_infos, new_version);
    // version change, do update
    if (TAIR_RETURN_SUCCESS == ret && new_version > version_)
    {
      urgent = manager_->update(cluster_infos);
      version_ = new_version;
    }

    return ret;
  }

  int cluster_info_updater::retrieve_cluster_info(CLUSTER_INFO_LIST& cluster_infos, uint32_t& new_version)
  {
    CONFIG_MAP config_map;
    int ret = master_handler_.retrieve_server_config(config_map, new_version, false);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      return ret;
    }

    // need no update
    if (new_version <= version_)
    {
      return ret;
    }

    const static char* CLUSTER_INFO_CONFIG_KEY = "groups";
    const static char* CLUSTER_INFO_CONFIG_VALUE_DELIMITER = " ,";

    std::vector<std::string> clusters;
    ret = parse_config(config_map, CLUSTER_INFO_CONFIG_KEY, CLUSTER_INFO_CONFIG_VALUE_DELIMITER, clusters);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("invalid cluster info config");
    }
    else
    {
      for (std::vector<std::string>::const_iterator it = clusters.begin(); it != clusters.end(); ++it)
      {
        // we have only one cs config now
        cluster_infos.push_back(cluster_info(master_handler_.get_cluster_info().master_cs_addr_,
                                             master_handler_.get_cluster_info().slave_cs_addr_,
                                             (*it)));
      }
    }
    return ret;
  }

}
