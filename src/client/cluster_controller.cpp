/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_mc_client_api.hpp 1245 2013-05-13 17:42:28Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */

#include "cluster_controller.hpp"
#include<json/json.h>

#include <stdlib.h>
#include <time.h>

namespace tair
{
   
   cluster_controller::cluster_controller(): timeout_ms(TAIR_DEFAULT_TIMEOUT)
   {
      srand((unsigned)time(NULL));

      //用户在tair::tair_mc_client_api::startup() 返回false后, 调用put()、get()会导致程序core掉，
      //开始时，在 all_cluster中放一个未初始化的tair_client_api的对象，防止在上述情况下程序core掉。
      all_cluster.reset(new client_vector());
      read_cluster.reset(new client_vector());
      write_cluster.reset(new client_vector());
      tair_client_wrapper_sptr  tair_client_non_init(new tair_client_wrapper());
      all_cluster->push_back(tair_client_non_init);
      read_cluster->push_back(tair_client_non_init);
      write_cluster->push_back(tair_client_non_init);
   }

   cluster_controller::~cluster_controller()
   {
   }

   bool cluster_controller::update_config(const string& json)
   {
      //启用严格模式，让非法的json解析时直接返回false，不自动容错
      Json::Features f = Json::Features::strictMode();
      Json::Reader reader(f);
      Json::Value value;

      if (!reader.parse(json, value))
      {
         TBSYS_LOG(ERROR, "json parse the diamond config info failed");
         return false;
      }

      cluster_info_vector cluster_config;
      localcache_info_vector localcache_config;

      //parse cluster config
      if (value.type() != Json::objectValue)
      {
         TBSYS_LOG(ERROR, "diamond config info format is invalid, json node \"root\" type should be objectValue");
         return false;
      }
      
      Json::Value clusters = value["clusters"];
      if (clusters.type() != Json::arrayValue)
      {
         TBSYS_LOG(ERROR, "diamond config info format is invalid, json node \"clusters\" type should be arrayValue");
         return false;
      }
      for (unsigned int i = 0; i < clusters.size(); ++i)
      {
         cluster_info_sptr one_cluster_config(new cluster_info);
         // address infomation
         if (clusters[i].type() != Json::objectValue)
         {
            TBSYS_LOG(ERROR, "diamond config info format is invalid, "
                      "the array element of \"clusters\" type should be objectValue");
            return false;
         }
         Json::Value address = clusters[i]["address"];
         if (address.empty())
         {
            TBSYS_LOG(ERROR, "diamond information miss tair configserver address");
            return false;
         }
         one_cluster_config->master = address["master"].asString();
         one_cluster_config->slave = address["slave"].asString();
         one_cluster_config->group = address["group"].asString();
         // readWeight infomation
         Json::Value readWeight = clusters[i]["readWeight"]; //readWeight
         if (readWeight.empty())
         {
            TBSYS_LOG(ERROR, "diamond config info miss readWeight");
            return false;
         }
         one_cluster_config->read_weight = atoi(readWeight.asString().c_str());
         if ((one_cluster_config->read_weight < 0) || (one_cluster_config->read_weight > MAX_WEIGHT))
         {
            TBSYS_LOG(ERROR, "diamond config info readWeight invalid value, < 0 or > %d", MAX_WEIGHT);
            return false;
         }
         //writeWeight information
         Json::Value writeWeight = clusters[i]["writeWeight"];
         if (writeWeight.empty())
         {
            TBSYS_LOG(ERROR, "diamond config info miss writeWeight");
            return false;
         }
         one_cluster_config->write_weight = atoi(writeWeight.asString().c_str());
         if ((one_cluster_config->write_weight < 0) || (one_cluster_config->write_weight > MAX_WEIGHT))
         {
            TBSYS_LOG(ERROR, "diamond config info writeWeight invalid value, < 0 or > %d", MAX_WEIGHT);
            return false;
         }

         cluster_config.push_back(one_cluster_config);
      }
      // update controller's tair configserver
      if (!update_config_cluster(cluster_config))
         return false;

      //parse localecache config
      Json::Value localcache = value["localcache"];
      if (!localcache.empty())
      {
         if (localcache.type() != Json::arrayValue)
         {
            TBSYS_LOG(ERROR, "diamond config info format is invalid, json node \"localcache\" type should be arrayValue");
            return false;
         }
         for (unsigned int i = 0; i < localcache.size(); ++i)
         {
            if (localcache[i].type() != Json::objectValue)
            {
               TBSYS_LOG(ERROR, "diamond config info format is invalid, "
                         "the array element of \"localcache\" type should be objectValue");
               return false;
            }
            localcache_info_sptr one_localcache_config(new localcache_info);
            Json::Value ns = localcache[i]["ns"];
            if (ns.empty())
            {
               TBSYS_LOG(ERROR, "diamond config info format is invalid, localcache miss \"ns\" info");
               return false;
            }
            one_localcache_config->ns  = atoi(ns.asString().c_str());
            if (one_localcache_config->ns <0 || one_localcache_config->ns > TAIR_MAX_AREA_COUNT)
            {
               TBSYS_LOG(ERROR, "diamond config info localcache \"ns\" value is invalid, < 0 or > %d", TAIR_MAX_AREA_COUNT);
               return false;
            }

            Json::Value cap = localcache[i]["cap"];
            if (cap.empty())
            {
               TBSYS_LOG(ERROR, "diamond config info format is invalid, localcache miss \"cap\" info");
               return false;
            }
            one_localcache_config->cap = atoi(cap.asString().c_str());
            if (one_localcache_config->cap <= 0)
            {
               TBSYS_LOG(ERROR, "diamond config info localcache \"cap\" value is invalid, <= 0");
               return false;
            }

            Json::Value ttl = localcache[i]["ttl"];
            if (ttl.empty())
            {
               TBSYS_LOG(ERROR, "diamond config info format is invalid, localcache miss \"ttl\" info");
               return false;
            }
            one_localcache_config->ttl = atoi(ttl.asString().c_str());
            if (one_localcache_config->ttl <= 0)
            {
               TBSYS_LOG(ERROR, "diamond config info localcache \"ttl\" value is invalid, <= 0");
               return false;
            }
               
            localcache_config.push_back(one_localcache_config);
         }
         //update controller's client localcache config
         update_config_localcache(localcache_config);
      }

      return true;
   }

   void cluster_controller::close()
   {
      client_vector_sptr temp_all_cluster = all_cluster;
      if (!temp_all_cluster.get())
         return ;
      for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it)
         (*it)->delegate.close();
   }

#ifdef WITH_COMPRESS
   void cluster_controller::set_compress_type(TAIR_COMPRESS_TYPE type)
   {
      tair_client_api::set_compress_type(type);
   }

   void cluster_controller::set_compress_threshold(int threshold)
   {
      tair_client_api::set_compress_threshold(threshold);
   }
#endif

   void cluster_controller::set_timeout_ms(int a_timeout_ms)
   {
      timeout_ms = a_timeout_ms;
      client_vector_sptr temp_all_cluster = all_cluster;
      if (!temp_all_cluster.get())
         return ;
      for (client_vector::iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it)
         (*it)->delegate.set_timeout(timeout_ms);
   }

   void cluster_controller::set_randread(bool rand_flag)
   {
      client_vector_sptr temp_read_cluster = read_cluster;
      if(!temp_read_cluster.get())
         return;
      for (client_vector::iterator it = temp_read_cluster->begin(); it != temp_read_cluster->end(); ++it)
         (*it)->delegate.set_randread(rand_flag);
   }

   //   inline client_vector* cluster_controller::choose_client_wrapper_for_write() {
   //      return write_cluster;
   //   }

   // 在调用之前，初始化阶段 可以保证 read_cluster != NULL, 且 所指向数组非空。
   tair_client_wrapper_sptr cluster_controller::choose_client_wrapper_for_read()
   {
      client_vector_sptr temp_read_cluster = read_cluster;
      // 绝大部分都是单集群
      if (LIKELY(temp_read_cluster->size() == 1))
         return (*temp_read_cluster)[0];
      int sum_read_weight = (temp_read_cluster->back())->read_weight;
      int random = rand() % sum_read_weight;
      client_vector::const_iterator it;
      for (it = temp_read_cluster->begin(); it != temp_read_cluster->end(); ++it)
         if (random < (*it)->read_weight)
            return *it;

      /**flow must not come here**/
      TBSYS_LOG(ERROR, "There is fatal error, there is no tair cluster to read key/value");
      return (*temp_read_cluster)[0];
   }

   
   struct great_compare_read
   {
      bool operator()(const tair_client_wrapper_sptr& a, const tair_client_wrapper_sptr& b)
         {
            return a->read_weight > b->read_weight;
         }
   } sort_read;

   struct great_compare_write
   {
      bool operator()(const tair_client_wrapper_sptr& a, const tair_client_wrapper_sptr& b)
         {
            return a->write_weight > b->write_weight;
         }
   } sort_write;

   bool cluster_controller::update_config_cluster(const cluster_info_vector& cluster_config)
   {
      client_vector_sptr temp_all_cluster(new client_vector());
      client_vector_sptr temp_read_cluster(new client_vector());
      client_vector_sptr temp_write_cluster(new client_vector());
      for (cluster_info_vector::const_iterator it = cluster_config.begin(); it != cluster_config.end(); ++it)
      {
         tair_client_wrapper_sptr  tair_client(new tair_client_wrapper());
         tair_client->delegate.set_timeout(timeout_ms);
         if (!tair_client->delegate.startup((*it)->master.c_str(), (*it)->slave.c_str(), (*it)->group.c_str()))
         {
            TBSYS_LOG(ERROR, "connect to config server failed, master=%s, slave=%s, group=%s",
                      (*it)->master.c_str(), (*it)->slave.c_str(), (*it)->group.c_str());
            return false;
            break;
         }

         tair_client->read_weight = (*it)->read_weight;
         if (tair_client->read_weight > 0)
            temp_read_cluster->push_back(tair_client);
         tair_client->write_weight = (*it)->write_weight;
         if (tair_client->write_weight > 0)
            temp_write_cluster->push_back(tair_client);
         temp_all_cluster->push_back(tair_client);
      }// end of for

      if (temp_read_cluster->empty())
      {
         TBSYS_LOG(ERROR, "There is no tair cluster to read key/value");
         return false;
      }
      if (temp_write_cluster->empty())
      {
         TBSYS_LOG(ERROR, "There is no tair cluster to write key/value");
         return false;
      }

      sort(temp_read_cluster->begin(), temp_read_cluster->end(), sort_read);
      for(unsigned i = 1; i < temp_read_cluster->size(); ++i)
         (*temp_read_cluster)[i]->read_weight += (*temp_read_cluster)[i-1]->read_weight;
      sort(temp_write_cluster->begin(), temp_write_cluster->end(), sort_write);

      read_cluster = temp_read_cluster;
      write_cluster = temp_write_cluster;
      all_cluster = temp_all_cluster;

      TBSYS_LOG(INFO, "the tair clusters is loaded successfully");

      return true;
   }

   void cluster_controller::update_config_localcache(const localcache_info_vector& localcache_config)
   {
      client_vector_sptr temp_all_cluster = all_cluster;
      for (client_vector::const_iterator it = temp_all_cluster->begin(); it != temp_all_cluster->end(); ++it)
      {
         for (localcache_info_vector::const_iterator config = localcache_config.begin();
              config != localcache_config.end(); ++config)
         {
            (*it)->delegate.setup_cache((*config)->ns, (*config)->cap, (*config)->ttl);
         }
      }
   }

}
