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
  class cluster_updater;
  class cluster_handler_manager;

// It will update cluster infos dynamically in this bucket_shard_cluster_handler_manager implementation,
// to avoid wild cluster_handler crash when concurrent using and updating,
// we use reference count to protect cluster_handler from unexpected destructing.
// But this is not common function interface of handler_manager, so here is
// an ugly macro encapsulation.
// Actually, it is expected to use handler_manager like this way:
//
//     cluster_handler* handler = handler_mgr_->pick_handler(key);
//     return NULL == handler ? TAIR_RETURN_SERVER_CAN_NOT_WORK :
//       handler->get_client()->get(xxxxx);

// effect op(read) over one cluster handler that is picked based on key
#define ONE_CLUSTER_HANDLER_OP(ret, key, op)                            \
    do {                                                                \
      handlers_node* node = (handlers_node*)(handler_mgr_->current());  \
      node->ref();                                                      \
      cluster_handler* handler = node->pick_handler(key);               \
      ret = (NULL == handler) ? TAIR_RETURN_SERVER_CAN_NOT_WORK : handler->get_client()->op; \
      node->unref();                                                    \
    } while (0)

// effect op(write) over all cluster handler synchronously,
// break once one fail. There is no ANY data consistency guaranteed.
// TODO.
#define ALL_CLUSTER_HANDLER_OP(ret, op)                                 \
  do {                                                                  \
    handlers_node* node = (handlers_node*)(handler_mgr_->current());    \
    node->ref();                                                        \
    int32_t handler_count = node->get_handler_count();                  \
    for (int32_t i = 0; i < handler_count; ++i)                         \
    {                                                                   \
      cluster_handler* handler = node->pick_handler(i);                 \
      ret = (NULL == handler) ? TAIR_RETURN_SERVER_CAN_NOT_WORK : handler->get_client()->op; \
      if (ret != TAIR_RETURN_SUCCESS)                                   \
      {                                                                 \
        break;                                                          \
      }                                                                 \
    }                                                                   \
    if (ret != TAIR_RETURN_SUCCESS)                                     \
    {                                                                   \
      ret = TAIR_RETURN_PARTIAL_SUCCESS;                                \
    }                                                                   \
    node->unref();                                                      \
  } while (0)


  class tair_multi_cluster_client_api
  {
  public:
    tair_multi_cluster_client_api();
    ~tair_multi_cluster_client_api();

    bool startup(const char* master_addr, const char* slave_addr, const char* group_name);

    int get(int area, const data_entry &key, data_entry*& data);
    int put(int area, const data_entry &key, const data_entry &data, int expire, int version, bool fill_cache = true);
    int remove(int area, const data_entry &key);
    int incr(int area, const data_entry& key, int count, int *ret_count, int init_value = 0, int expire = 0);
    int decr(int area, const data_entry& key, int count, int *ret_count, int init_value = 0, int expire = 0);
    int add_count(int area, const data_entry &key, int count, int *ret_count, int init_value = 0);
    void set_timeout(int timeout_ms);
    void close();

  private:
    cluster_info_updater* updater_;
    cluster_handler_manager* handler_mgr_;
  };
}
#endif
