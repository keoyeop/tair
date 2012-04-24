/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair client api interface
 * Version: $Id: i_tair_client_impl.hpp 690 2012-04-09 02:09:34Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef I_TAIR_CLIENT_IMPL_H
#define I_TAIR_CLIENT_IMPL_H

#include <map>
#include <vector>
#include <string>

#include "define.hpp"
#include "data_entry.hpp"

namespace tair
{
  using tair::common::data_entry;
  class i_tair_client_impl
  {
  public:
    i_tair_client_impl() {};
    virtual ~i_tair_client_impl() {};

    virtual bool startup(const char* master_addr, const char* slave_addr, const char* group_name) = 0;
    virtual void close() = 0;
    virtual int get(int area, const data_entry& key, data_entry*& data) = 0;
    virtual int put(int area, const data_entry& key, const data_entry& data,
                    int expire, int version, bool fill_cache = true) = 0;
    virtual int remove(int area, const data_entry& key) = 0;
    virtual int add_count(int area, const data_entry& key, int count, int* ret_count,
                          int init_value = 0, int expire_time = 0) = 0;
    virtual int mget(int area, std::vector<data_entry *> &keys, tair::common::tair_keyvalue_map& data) = 0;
    virtual int mput(int area, const tair::common::tair_client_kv_map& kvs, int& fail_request, bool compress) = 0;
    virtual int mdelete(int area, std::vector<data_entry *> &keys) = 0;

    virtual void set_log_level(const char* level)
    { TBSYS_LOGGER.setLogLevel(level); }
    virtual void set_timeout(int32_t timeout_ms) = 0;
    virtual uint32_t get_bucket_count() const = 0;
    virtual uint32_t get_copy_count() const = 0;

    virtual void get_server_with_key(const data_entry& key,std::vector<std::string>& servers)
    { return; }
    virtual int op_cmd_to_cs(ServerCmdType cmd, std::vector<std::string>* params, std::vector<std::string>* ret_values)
    { return TAIR_RETURN_NOT_SUPPORTED; }

    virtual int op_cmd_to_ds(ServerCmdType cmd, const char* group, std::vector<std::string>* params,
                             std::vector<std::string>* ret_values, const char* dest_server_addr = NULL)
    { return TAIR_RETURN_NOT_SUPPORTED; }

    const char* get_error_msg(int ret)
    {
      std::map<int,std::string>::const_iterator it = i_tair_client_impl::errmsg_.find(ret);
      return it != i_tair_client_impl::errmsg_.end() ? it->second.c_str() : "unknow";
    }

  public:
    static const std::map<int, std::string> errmsg_;
  };
}

#endif
