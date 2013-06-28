/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_mc_client_api.hpp 1245 2013-05-13 17:09:47Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */
#ifndef CLUSTER_CONTROLLER
#define CLUSTER_CONTROLLER

#define TAIR_DEFAULT_TIMEOUT 2000

#include "define.hpp"
#include "tair_client_api.hpp"

#include <map>
#include <tr1/memory>

namespace tair
{
   using std::tr1::shared_ptr;
   class tair_client_api;

   struct cluster_info
   {
      string master;
      string slave;
      string group;
      int32_t read_weight;
      int32_t write_weight;
      map<string, string> options;
   };

   struct localcache_info
   {
      int32_t ns;
      int32_t cap;
      int32_t ttl;
   };

   struct tair_client_wrapper
   {
      int32_t read_weight;
      int32_t write_weight;
      tair_client_api delegate;
   };

   typedef shared_ptr<tair_client_wrapper> tair_client_wrapper_sptr;
   typedef vector<tair_client_wrapper_sptr> client_vector;
   typedef shared_ptr<client_vector>  client_vector_sptr;
   
   typedef shared_ptr<cluster_info> cluster_info_sptr;
   typedef vector<cluster_info_sptr> cluster_info_vector;

   typedef shared_ptr<localcache_info> localcache_info_sptr;
   typedef vector<localcache_info_sptr> localcache_info_vector;

   class cluster_controller
   {
   public:
      cluster_controller();
      ~cluster_controller();
      void close();
      bool update_config(const string& json);
#ifdef WITH_COMPRESS
      void set_compress_type(TAIR_COMPRESS_TYPE type);
      void set_compress_threshold(int theshold);
#endif
      void setup_cache(int area, size_t capacity);
      void setup_cache(int area, size_t capacity, uint64_t expire_time);
      void set_timeout_ms(int a_timeout_ms);
      void set_randread(bool rand_flag);
      client_vector_sptr choose_client_wrapper_for_write()
      {
         return write_cluster;
      }
      tair_client_wrapper_sptr choose_client_wrapper_for_read();
      
   private:
      bool update_config_cluster(const cluster_info_vector& cluster_config);
      void update_config_localcache(const localcache_info_vector& localcache_config);
      
   private:
      int timeout_ms;

      client_vector_sptr all_cluster;
      client_vector_sptr read_cluster;
      client_vector_sptr write_cluster;

      const static int32_t MAX_WEIGHT = 10000;
   };

}

#endif

