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
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "tair_client_api.hpp"
#include "i_tair_client_impl.hpp"
#include "tair_client_api_impl.hpp"
#include "tair_multi_cluster_client_impl.hpp"

namespace tair {

  static i_tair_client_impl* new_tair_client(const char *master_addr,const char *slave_addr,const char *group_name);
  static TairClusterType get_cluster_type_by_config(tbsys::STR_STR_MAP& config_map);

  /*-----------------------------------------------------------------------------
   *  tair_client_api
   *-----------------------------------------------------------------------------*/

  tair_client_api::tair_client_api() : impl(NULL)
  {
  }

  tair_client_api::~tair_client_api()
  {
    if (impl != NULL)
    {
      delete impl;
    }
  }

  bool tair_client_api::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    bool ret = false;
    impl = new_tair_client(master_addr, slave_addr, group_name);
    if (NULL == impl)
    {
      log_error("init tair client fail.");
    }
    else
    {
      ret = impl->startup(master_addr,slave_addr,group_name);
    }
    return ret;
  }

  void tair_client_api::close()
  {
    impl->close();
  }

  int tair_client_api::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expire,
      int version,
      bool fill_cache)
  {
    return impl->put(area,key,data,expire,version, fill_cache);
  }

  int tair_client_api::mput(int area,
      const tair_client_kv_map& kvs,
      int& fail_request,
      bool compress)
  {
    return impl->mput(area, kvs, fail_request, compress);
  }

  int tair_client_api::get(int area,
      const data_entry &key,
      data_entry*& data)
  {
    return impl->get(area,key,data);
  }

  int tair_client_api::mget(int area,
      vector<data_entry *> &keys,
      tair_keyvalue_map& data)
  {
    return impl->mget(area,keys,data);
  }

  int tair_client_api::remove(int area,
      const data_entry &key)
  {
    return impl->remove(area,key);
  }

  int tair_client_api::mdelete(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
  }

  int tair_client_api::minvalid(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
  }


  int tair_client_api::incr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {
    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl->add_count(area,key,count,ret_count,init_value,expire);

  }
  int tair_client_api::decr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {

    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl->add_count(area,key,-count,ret_count,init_value,expire);

  }



  int tair_client_api::add_count(int area,
      const data_entry &key,
      int count,
      int *ret_count,
      int init_value /*= 0*/)
  {

    return impl->add_count(area,key,count,ret_count,init_value);

  }

  //int tair_client_api::removeArea(int area)
  //{
  //      return impl->removeArea(area);
  //}
  //
  //int tair_client_api::dumpArea(std::set<DumpMetaInfo>& info)
  //{
  //      return impl->dumpArea(info);
  //}


#if 0
#undef ADD_ITEMS
#undef GET_ITEMS_FUNC

#define ADD_ITEMS(type)                                                 \
  int tair_client_api::add_items(int area,const data_entry &key,type data,int version,int expired,int max_count)

#define GET_ITEMS_FUNC(FuncName,type)                                   \
  int tair_client_api::FuncName(int area, const data_entry &key, int offset, int count, type result)

#define ADD_ITEMS_FUNC_IMPL(type,element_type)                          \
  ADD_ITEMS(type)                                                      \
  {                                                                    \
    return impl->add_items(area,key,data.begin(),data.end(),version,expired,max_count,element_type); \
  }

  ADD_ITEMS_FUNC_IMPL(vector<int>&,ELEMENT_TYPE_INT);
  ADD_ITEMS_FUNC_IMPL(vector<int64_t>&,ELEMENT_TYPE_LLONG);
  ADD_ITEMS_FUNC_IMPL(vector<double>&,ELEMENT_TYPE_DOUBLE);
  ADD_ITEMS_FUNC_IMPL(vector<std::string>&,ELEMENT_TYPE_STRING);


#define GET_ITEMS_FUNC_IMPL(FuncName,type,element_type)                 \
  GET_ITEMS_FUNC(FuncName,type)                                        \
  {                                                                    \
    return impl->FuncName(area,key,offset,count,result,element_type); \
  }

#define GET_ITEMS_FUNC_IMPL_INT32(FuncName,type)                        \
  GET_ITEMS_FUNC(FuncName,type)                                        \
  {                                                                    \
    vector<int64_t> tmp_result;                                       \
    int ret = impl->FuncName(area,key,offset,count,tmp_result,ELEMENT_TYPE_INT); \
    if(ret < 0){                                                      \
      return ret;                                                    \
    }                                                                 \
    copy(tmp_result.begin(),tmp_result.end(),std::back_inserter(result)); \
    return ret;                                                       \
  }

  GET_ITEMS_FUNC_IMPL_INT32(get_items,std::vector<int>&);
  GET_ITEMS_FUNC_IMPL(get_items,std::vector<int64_t>&,ELEMENT_TYPE_LLONG)
    GET_ITEMS_FUNC_IMPL(get_items,std::vector<double>&,ELEMENT_TYPE_DOUBLE)
    GET_ITEMS_FUNC_IMPL(get_items,std::vector<std::string>&,ELEMENT_TYPE_STRING)

    GET_ITEMS_FUNC_IMPL_INT32(get_and_remove,std::vector<int>&);
  GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<int64_t>&,ELEMENT_TYPE_LLONG)
    GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<double>&,ELEMENT_TYPE_DOUBLE)
    GET_ITEMS_FUNC_IMPL(get_and_remove,std::vector<std::string>&,ELEMENT_TYPE_STRING)


    int tair_client_api::remove_items(int area,
        const data_entry &value,
        int offset,
        int count)
    {
      return impl->remove_items(area,value,offset,count);
    }

  int tair_client_api::get_items_count(int area,const data_entry& key)
  {
    return impl->get_items_count(area,key);
  }

#endif

  void tair_client_api::set_log_level(const char* level)
  {
    TBSYS_LOGGER.setLogLevel(level);
  }

  void tair_client_api::set_timeout(int timeout)
  {
    impl->set_timeout(timeout);
  }

  const char *tair_client_api::get_error_msg(int ret)
  {
    return impl->get_error_msg(ret);
  }

  uint32_t tair_client_api::get_bucket_count() const
  {
    return impl->get_bucket_count();
  }
  uint32_t tair_client_api::get_copy_count() const
  {
    return impl->get_copy_count();
  }
  void tair_client_api::get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const
  {
    return impl->get_server_with_key(key,servers);
  }

  int tair_client_api::get_group_status(vector<string> &group, vector<string> &status)
  {
    return impl->op_cmd_to_cs(TAIR_SERVER_CMD_GET_GROUP_STATUS, &group, &status);
  }
  int tair_client_api::get_tmp_down_server(vector<string> &group, vector<string> &down_servers)
  {
    return impl->op_cmd_to_cs(TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER, &group, &down_servers);
  }
  int tair_client_api::set_group_status(const char *group, const char *status)
  {
    int ret = TAIR_RETURN_FAILED;
    if (group != NULL && status != NULL)
    {
      std::vector<std::string> params;
      params.push_back(group);
      params.push_back(status);
      ret = impl->op_cmd_to_cs(TAIR_SERVER_CMD_SET_GROUP_STATUS, &params, NULL);
    }
    return ret;
  }
  int tair_client_api::reset_server(const char* group, std::vector<std::string>* dss)
  {
    int ret = TAIR_RETURN_FAILED;
    if (group != NULL)
    {
      std::vector<std::string> params;
      params.push_back(group);
      if (dss != NULL)
      {
        for (vector<string>::iterator it = dss->begin(); it != dss->end(); ++it)
        {
          params.push_back(*it);
        }
      }
      ret = impl->op_cmd_to_cs(TAIR_SERVER_CMD_RESET_DS, &params, NULL);
    }
    return ret;
  }
  int tair_client_api::flush_mmt(const char* group, const char* ds_addr)
  {
    int ret = TAIR_RETURN_FAILED;
    if (group != NULL)
    {
      ret = impl->op_cmd_to_ds(TAIR_SERVER_CMD_FLUSH_MMT, group, NULL, NULL, ds_addr);
    }
    return ret;
  }
  int tair_client_api::reset_db(const char* group, const char* ds_addr)
  {
    int ret = TAIR_RETURN_FAILED;
    if (group != NULL)
    {
      ret = impl->op_cmd_to_ds(TAIR_SERVER_CMD_RESET_DB, group, NULL, NULL, ds_addr);
    }
    return ret;
  }

  i_tair_client_impl* new_tair_client(const char *master_addr,
                                      const char *slave_addr,
                                      const char *group_name)
  {
    i_tair_client_impl* ret_client_impl = NULL;

    tair_client_impl client;
    client.set_can_mock(true);
    if (!client.startup(master_addr, slave_addr, group_name))
    {
      log_error("startup to get cluster type fail.");
      return NULL;
    }
    tbsys::STR_STR_MAP config_map;
    uint32_t version = 0;
    int ret = client.retrieve_server_config(false, config_map, version);
    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("retrieve_server_config fail, ret: %d", ret);
      return NULL;
    }

    TairClusterType type = get_cluster_type_by_config(config_map);
    switch (type)
    {
    case TAIR_CLUSTER_TYPE_SINGLE_CLUSTER:
    {
      log_warn("Tair cluster is SINGLE CLUSTER TYPE, init tair_client_impl");
      ret_client_impl = new tair_client_impl();
      break;
    }
    case TAIR_CLUSTER_TYPE_MULTI_CLUSTER:
    {
      log_warn("Tair cluster is MULTI CLUSTER TYPE, init tair_multi_cluster_client_impl");
      ret_client_impl = new tair_multi_cluster_client_impl();
      break;
    }
    default :
    {
      log_error("unsupported cluster type, can NOT init tair client. type: %d", type);
      break;
    }
    }
    return ret_client_impl;
  }

  TairClusterType get_cluster_type_by_config(tbsys::STR_STR_MAP& config_map)
  {
    // we consider cluster type by TAIR_MULTI_GROUPS now.
    // Just consider SINGLE_CLUSTER and MULTI_CLUSTER now.
    static const char* cluster_type_config = TAIR_MULTI_GROUPS;

    if (config_map.find(cluster_type_config) != config_map.end())
    {
      return TAIR_CLUSTER_TYPE_MULTI_CLUSTER;
    }
    else
    {
      return TAIR_CLUSTER_TYPE_SINGLE_CLUSTER;
    }
  }
} /* tair */
