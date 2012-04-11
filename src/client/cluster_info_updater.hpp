/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: cluster_info_updater.hpp 690 2012-04-09 02:09:34Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_CLIENT_CLUSTER_INFO_UPDATER_H
#define TAIR_CLIENT_CLUSTER_INFO_UPDATER_H

#include <tbsys.h>

#include "cluster_handler_manager.hpp"

namespace tair
{
  static const int64_t DEFAULT_CLUSTER_UPDATE_INTERVAL_S = 30;

  class cluster_info_updater : public tbsys::CDefaultRunnable
  {
  public:
    cluster_info_updater();
    ~cluster_info_updater();

    void run(tbsys::CThread* thread, void* arg);

    void init(cluster_handler_manager* manager,
              const char* master_cs_addr, const char* slave_cs_addr, const char* group_name,
              int64_t interval_s = DEFAULT_CLUSTER_UPDATE_INTERVAL_S);
    int update_cluster_info(bool& urgent);

  private:
    int retrieve_cluster_info(CLUSTER_INFO_LIST& cluster_infos, uint32_t& new_version);

  private:
    bool inited_;
    int64_t interval_s_;
    bool stopped_;
    uint32_t version_;
    cluster_handler master_handler_;
    cluster_handler_manager* manager_;
  };
}

#endif
