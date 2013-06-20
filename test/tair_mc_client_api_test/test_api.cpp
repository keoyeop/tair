/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_api.cpp 1245 2013-06-06 11:16:29Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */
#include "test_api.hpp"
#include "define.hpp"

#include <time.h>
#include <stdlib.h>
#include <string.h>

tair_mc_client_test_api::tair_mc_client_test_api()
{
   srand((unsigned int)time(NULL));
   int len = 'z' - '!';
   for (int i = 0; i < max_key_size; ++i)
      key_str[i] = (rand() % len) + '!';
   for (int i = 0; i < max_value_size; ++i)
      value_str[i] = (rand() % len) + '!';

   func tmp;
   tmp.func_ptr = &tair_mc_client_test_api::test_put; tmp.func_name= "put";
   all_func.push_back(tmp);
   tmp.func_ptr = &tair_mc_client_test_api::test_get; tmp.func_name= "get";
   all_func.push_back(tmp);
   tmp.func_ptr = &tair_mc_client_test_api::test_mget; tmp.func_name= "mget";
   all_func.push_back(tmp);
   tmp.func_ptr = &tair_mc_client_test_api::test_remove; tmp.func_name= "remove";
   all_func.push_back(tmp);
}

tair_mc_client_test_api::~tair_mc_client_test_api(){}

const vector<func>& tair_mc_client_test_api::get_all_func()
{
   return all_func;
}

bool tair_mc_client_test_api::startup(const string& data_id)
{
   client.set_timeout(3000);

#ifdef WITH_COMPRESS   
   client.set_compress_threshold(512);
   client.set_compress_type(TAIR_SNAPPY_COMPRESS);
#endif
   
   client.set_log_level("INFO");
   return client.startup(data_id);
}

void tair_mc_client_test_api::close()
{
   client.close();
}

bool tair_mc_client_test_api::test_put()
{
   int key_pos = get_key_pos();
   int value_pos = get_value_pos();
   data_entry key(key_str, key_pos, false);
   data_entry value(value_str, value_pos, false);
   
   client.remove(0, key);
   if (client.put(0,key, value, 0, 0, true) != 0)
      return false;
   data_entry *ret_value;
   if (client.get(0, key, ret_value) != 0)
       return false;

   bool res = (strncmp(value.get_data(), ret_value->get_data(), ret_value->get_size()) == 0);
   delete ret_value;
   return res;
}

bool tair_mc_client_test_api::test_get()
{
   int key_pos = get_key_pos();
   int value_pos = get_value_pos();
   data_entry key(key_str, key_pos, false);
   data_entry value(value_str, value_pos, false);
   
   client.remove(0, key);
   if (client.put(0,key, value, 0, 0, true) != 0)
      return false;
   data_entry *ret_value;
   if (client.get(0, key, ret_value) != 0)
       return false;

   bool res = (strncmp(value.get_data(), ret_value->get_data(), ret_value->get_size()) == 0);
   delete ret_value;
   return res;
}

bool tair_mc_client_test_api::test_mget()
{
   int key_pos = get_key_pos();
   int value_pos = get_value_pos();
   data_entry key0(key_str, key_pos, false);
   data_entry value0(value_str, value_pos, false);
   key_pos = get_key_pos();
   value_pos = get_value_pos();
   data_entry key1(key_str, key_pos, false);
   data_entry value1(value_str, value_pos, false);
   key_pos = get_key_pos();
   value_pos = get_value_pos();
   data_entry key2(key_str, key_pos, false);
   data_entry value2(value_str, value_pos, false);

   client.remove(0, key0);
   client.remove(0, key1);
   client.remove(0, key2);

   if (client.put(0, key0, value0, 0, 0, true) != TAIR_RETURN_SUCCESS)
      return false;
   if (client.put(0, key1, value1, 0, 0, true) != TAIR_RETURN_SUCCESS)
      return false;

   vector<data_entry*> keys;
   keys.push_back(&key0);
   keys.push_back(&key1);
   keys.push_back(&key2);

   tair_keyvalue_map ret_map;
   int rc = client.mget(0, keys, ret_map);
   for(tair_keyvalue_map::iterator it = ret_map.begin(); it != ret_map.end();)
   {
      tair_keyvalue_map::iterator tmp = it++;
      delete tmp->first;
      delete tmp->second;
   }
   ret_map.clear();
   if (rc != TAIR_RETURN_PARTIAL_SUCCESS)
       return false;

   // all in 
   if (client.put(0, key2, value2, 0, 0, true) != TAIR_RETURN_SUCCESS)
      return false;

   rc = client.mget(0, keys, ret_map) ;
   for(tair_keyvalue_map::iterator it = ret_map.begin(); it !=ret_map.end(); )
   {
      tair_keyvalue_map::iterator tmp = it++;
      delete tmp->first;
      delete tmp->second;
   }
   if (!((rc == TAIR_RETURN_SUCCESS) || (rc == TAIR_RETURN_PARTIAL_SUCCESS)))
       return false;
   return true;
}

bool tair_mc_client_test_api::test_remove()
{
   int key_pos = get_key_pos();
   int value_pos = get_value_pos();
   data_entry key(key_str, key_pos, false);
   data_entry value(value_str, value_pos, false);
   
   if (client.put(0,key, value, 0, 0, true) != 0)
      return false;
   data_entry *ret_value;
   if (client.get(0, key, ret_value) != 0)
       return false;

   bool res = (strncmp(value.get_data(), ret_value->get_data(), ret_value->get_size()) == 0);
   delete ret_value;
   ret_value = NULL;
   if (!res)
      return false;
//**************************
   if (client.remove(0, key) != 0)
      return false;
   if (client.get(0, key, ret_value) == 0)
   {
      delete ret_value;
      ret_value = NULL;
      return false;
   }
   return true;
}

bool tair_mc_client_test_api::test_invalid()
{
   int key_pos = get_key_pos();
   int value_pos = get_value_pos();
   data_entry key(key_str, key_pos, false);
   data_entry value(value_str, value_pos, false);
   
   if (client.put(0,key, value, 0, 0, true) != 0)
      return false;
   data_entry *ret_value;
   if (client.get(0, key, ret_value) != 0)
       return false;

   bool res = (strncmp(value.get_data(), ret_value->get_data(), ret_value->get_size()) == 0);
   delete ret_value;
   ret_value = NULL;
   if (!res)
      return false;
//**************************
   if (client.invalidate(0, key, true) != 0)
      return false;
   if (client.get(0, key, ret_value) == 0)
   {
      delete ret_value;
      ret_value = NULL;
      return false;
   }
   return true;
}

int tair_mc_client_test_api::get_key_pos()
{
   int range = max_key_size / 2;
   int pos = (rand() % range) + range;
   return pos;
}

int tair_mc_client_test_api::get_value_pos()
{
   int range = max_value_size / 2;
   int pos = (rand() % range) + range;
   return pos;
}

