/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_api.hpp 1245 2013-06-06 10:34:29Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */

#include <tair_mc_client_api.hpp>
#include <data_entry.hpp>

#include <vector>
#include <string>

#ifndef TEST_API
#define TEST_API

using tair::tair_mc_client_api;
using tair::data_entry;
using tair::tair_keyvalue_map;
using std::vector;
using std::string;

enum
{
   max_key_size = 1024,
   max_value_size = 1048576
};


class tair_mc_client_test_api;
typedef bool (tair_mc_client_test_api::*func_ptr_type)();
struct func
{
   func_ptr_type func_ptr;
   string func_name;
};

class tair_mc_client_test_api
{
   
public:
   tair_mc_client_test_api();
   ~tair_mc_client_test_api();
   const vector<func>& get_all_func();
   bool startup(const string& data_id);
   void close();

   bool test_put();
   bool test_get();
   bool test_mget();
   bool test_remove();
   bool test_invalid();

private:   
   int get_key_pos();
   int get_value_pos();
   
private:
   tair_mc_client_api client;
   char key_str[max_key_size];
   char value_str[max_value_size];

   vector<func>  all_func;
};

#endif

