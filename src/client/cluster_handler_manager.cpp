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

#include "cluster_handler_manager.hpp"

namespace tair
{
  //////////////////////////// cluster_handler ////////////////////////////
  cluster_handler::cluster_handler() : client_(NULL), index_(-1), down_servers_(NULL), down_buckets_(NULL)
  {
  }

  cluster_handler::~cluster_handler()
  {
    if (client_ != NULL)
    {
      client_->close();
      delete client_;
    }
    clear_update_temp();
  }

  void cluster_handler::init(const char* master_cs_addr, const char* slave_cs_addr, const char* group_name)
  {
    info_.master_cs_addr_.assign(master_cs_addr);
    info_.slave_cs_addr_.assign(NULL == slave_cs_addr ? master_cs_addr : slave_cs_addr);
    info_.group_name_.assign(group_name);
  }

  void cluster_handler::init(const cluster_info& info)
  {
    info_ = info;
  }

  bool cluster_handler::start(int32_t timeout_ms)
  {
    if (client_ != NULL)
    {
      client_->close();
      delete client_;
    }
    client_ = new tair_client_impl();
    client_->set_timeout(timeout_ms);
    return client_->startup(info_.master_cs_addr_.c_str(), info_.slave_cs_addr_.c_str(), info_.group_name_.c_str());
  }

  int cluster_handler::retrieve_server_config(CONFIG_MAP& config_map, uint32_t& version, bool update)
  {
    return client_->retrieve_server_config(config_map, version, update);
  }

  void cluster_handler::get_buckets_by_server(uint64_t server_id, DOWN_BUCKET_LIST& buckets)
  {
    return client_->get_buckets_by_server(server_id, buckets);
  }

  //////////////////////////// handlers_node ////////////////////////////
  handlers_node::handlers_node()
    : prev_(NULL), next_(NULL), timeout_ms_(DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS),
      handler_map_(NULL), handlers_(NULL), extra_bucket_map_(NULL)
  {
    atomic_set(&ref_, 0);
  }

  handlers_node::handlers_node(CLUSTER_HANDLER_MAP* handler_map, CLUSTER_HANDLER_LIST* handlers, BUCKET_INDEX_MAP* extra_bucket_map)
    : prev_(NULL), next_(NULL), handler_map_(handler_map), handlers_(handlers), extra_bucket_map_(extra_bucket_map)
  {
    atomic_set(&ref_, 0);
  }

  handlers_node::~handlers_node()
  {
    clear(false);
  }

  void handlers_node::update(const CLUSTER_INFO_LIST& cluster_infos, const handlers_node& diff_handlers_node)
  {
    CLUSTER_HANDLER_MAP_ITER_LIST has_down_server_handlers;

    // construct new handler_map
    construct_handler_map(cluster_infos, diff_handlers_node, has_down_server_handlers);
    // construct new handlers
    construct_handlers();
    // construct new extra_bucket_map
    construct_extra_bucket_map(has_down_server_handlers);
  }

  cluster_handler* handlers_node::pick_handler(const data_entry& key)
  {
    if (bucket_count_ <= 0)
    {
      return NULL;
    }

    int bucket = util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % bucket_count_;
    int index = bucket_to_handler_index(bucket);
    return index >= 0 ? (*handlers_)[index] : NULL;
  }

  cluster_handler* handlers_node::pick_handler(int32_t index)
  {
    return (index < 0 || index > (int32_t)handlers_->size()) ? NULL : (*handlers_)[index];
  }

  void handlers_node::clear(bool force)
  {
    // not remove from list here for simplicity
    if (handler_map_ != NULL)
    {
      for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end();)
      {
        cluster_handler* handler = it->second;
        ++it;
        if (force || it->second->get_index() >= 0)
        {
          delete handler;
        }
      }
      delete handler_map_;
      handler_map_ = NULL;
    }
    if (handlers_ != NULL)
    {
      delete handlers_;
      handlers_ = NULL;
    }
    if (extra_bucket_map_ != NULL)
    {
      delete extra_bucket_map_;
      extra_bucket_map_ = NULL;
    }
  }
  // we reuse cluster_handler, so cluster_handler should be marked whether is should be cleared later
  void handlers_node::mark_clear(const handlers_node& diff_handlers_node)
  {
    CLUSTER_HANDLER_MAP* cur_handler_map = diff_handlers_node.handler_map_;
    for (CLUSTER_HANDLER_MAP::const_iterator it = handler_map_->begin(); it != handler_map_->end();)
    {
      if (cur_handler_map->find(it->first) == cur_handler_map->end())
      {
        cluster_handler* handler = it->second;
        ++it;
        // this handler has not been at service
        handler->set_index(-1);
      }
      else
      {
        ++it;
      }
    }
  }

  void handlers_node::clear_update_temp()
  {
    for (CLUSTER_HANDLER_MAP::iterator it = handler_map_->begin(); it != handler_map_->end();)
    {
      cluster_handler* handler = it->second;
      ++it;
      handler->clear_update_temp();
    }
  }

  void handlers_node::construct_handler_map(const CLUSTER_INFO_LIST& cluster_infos,
                                            const handlers_node& diff_handlers_node,
                                            CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers)
  {
    handler_map_->clear();
    if (cluster_infos.empty())
    {
      return;
    }

    int ret = TAIR_RETURN_SUCCESS;
    cluster_handler* handler = NULL;
    bool new_handler = false;

    for (CLUSTER_INFO_LIST::const_iterator info_it = cluster_infos.begin(); info_it != cluster_infos.end(); ++info_it)
    {
      // already operate
      if (handler_map_->find(*info_it) != handler_map_->end())
      {
        continue;
      }

      CLUSTER_HANDLER_MAP::const_iterator handler_it = diff_handlers_node.handler_map_->find(*info_it);
      if (handler_it != diff_handlers_node.handler_map_->end())
      {
        new_handler = false;
        // reuse handler
        handler = handler_it->second;
      }
      else
      {
        new_handler = true;
        handler = new cluster_handler();
        handler->init(*info_it);

        // be tolerable to one cluster's down
        if ((ret = handler->start(timeout_ms_)) != TAIR_RETURN_SUCCESS)
        {
          log_error("start cluster handler fail. ret: %d, info: %s", ret, info_it->debug_string().c_str());
          delete handler;
          continue;
        }
      }

      int32_t new_bucket_count = handler->get_client()->get_bucket_count();
      // multi-cluster must have same bucket count
      if (bucket_count_ > 0 && bucket_count_ != new_bucket_count)
      {
        log_error("bucket count conflict, ignore this cluster. expect %d but %d, info: %s",
                  bucket_count_, new_bucket_count, info_it->debug_string().c_str());
        // must be a new handler
        assert(new_handler);
        delete handler;
      }

      // update bucket count
      if (bucket_count_ <= 0)
      {
        bucket_count_ = new_bucket_count;
      }

      CONFIG_MAP config_map;
      uint32_t new_version;
      // update server table
      ret = handler->retrieve_server_config(config_map, new_version, true);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_error("retrieve cluster config fail. ret: %d, info: %s", ret, info_it->debug_string().c_str());
        // be tolerable to one cluster's down
        if (new_handler)
        {
          delete handler;
        }
        continue;
      }

      // TODO: move to common
      static const char* CLUSTER_STATUS_CONFIG_KEY = "status";
      static const char* CLUSTER_STATUS_ON = "on";
      static const char* CLUSTER_DOWN_SERVER_CONFIG_KEY = "down_server";
      static const char* CLUSTER_DOWN_SERVER_CONFIG_VALUE_DELIMITER = "; ";

      // check get cluster status
      std::string status;
      parse_config(config_map, CLUSTER_STATUS_CONFIG_KEY, status);
      // cluster status not ON.
      // no status config means OFF.
      if (strcasecmp(status.c_str(), CLUSTER_STATUS_ON) != 0)
      {
        log_info("cluster OFF: %s", info_it->debug_string().c_str());

        if (new_handler)
        {
          delete handler;
        }
        continue;
      }

      // this handler will at service
      CLUSTER_HANDLER_MAP::iterator new_handler_it =
        handler_map_->insert(CLUSTER_HANDLER_MAP::value_type(*info_it, handler)).first;

      // check cluster servers
      std::vector<std::string> down_servers;
      parse_config(config_map, CLUSTER_DOWN_SERVER_CONFIG_KEY, CLUSTER_DOWN_SERVER_CONFIG_VALUE_DELIMITER, down_servers);

      if (!down_servers.empty())
      {
        DOWN_SERVER_LIST* down_server_ids = new DOWN_SERVER_LIST();
        for (std::vector<std::string>::const_iterator it = down_servers.begin(); it != down_servers.end(); ++it)
        {
          down_server_ids->push_back(tbsys::CNetUtil::strToAddr(it->c_str(), 0));
        }

        handler->set_down_servers(down_server_ids);
        has_down_server_handlers.push_back(new_handler_it);
      }
    }
  }

  void handlers_node::construct_handlers()
  {
    handlers_->clear();
    if (handler_map_->empty())
    {
      return;
    }

    int32_t i = 0;
    for (CLUSTER_HANDLER_MAP::const_iterator it = handler_map_->begin(); it != handler_map_->end(); ++it)
    {
      handlers_->push_back(it->second);
      it->second->set_index(i);
    }
  }

  void handlers_node::construct_extra_bucket_map(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers)
  {
    extra_bucket_map_->clear();
    // no down servers, no down buckets, no extra special bucket sharding
    if (has_down_server_handlers.empty())
    {
      return;
    }

    CLUSTER_HANDLER_MAP_ITER_LIST has_down_bucket_handlers;
    // collect all down buckets by down servers
    collect_down_bucket(has_down_server_handlers, has_down_bucket_handlers);
    // sharding all down buckets
    shard_down_bucket(has_down_bucket_handlers);
  }

  void handlers_node::collect_down_bucket(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers,
                                          CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers)
  {
    has_down_bucket_handlers.clear();
    if (has_down_server_handlers.empty())
    {
      return;
    }

    for (CLUSTER_HANDLER_MAP_ITER_LIST::iterator handler_it_it = has_down_server_handlers.begin();
         handler_it_it != has_down_server_handlers.end(); ++handler_it_it)
    {
      cluster_handler* handler = (*handler_it_it)->second;
      DOWN_SERVER_LIST* down_servers = handler->get_down_servers();
      if (NULL == down_servers || down_servers->empty())
      {
        continue;
      }

      DOWN_BUCKET_LIST* down_buckets = new DOWN_BUCKET_LIST();
      for (DOWN_SERVER_LIST::const_iterator server_it = down_servers->begin(); server_it != down_servers->end(); ++server_it)
      {
        handler->get_buckets_by_server(*server_it, *down_buckets);
      }

      // handler->clear_update_temp() will delete
      handler->set_down_buckets(down_buckets);

      if (!down_buckets->empty())
      {
        has_down_bucket_handlers.push_back(*handler_it_it);
      }
    }
  }

  void handlers_node::shard_down_bucket(const CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers)
  {
    extra_bucket_map_->clear();
    if (has_down_bucket_handlers.empty())
    {
      return;
    }

    // one bucket has at most handler_map_.size() shard choice.
    std::vector<int32_t> indexs(handler_map_->size());

    // iterator all down buckets to find handler that can service it.
    // we prefer skiping bucket's orignal cluster handler(exclude) to
    // sorting all down buckets then finding from all cluster handlers.
    for (CLUSTER_HANDLER_MAP_ITER_LIST::const_iterator handler_it_it = has_down_bucket_handlers.begin();
         handler_it_it != has_down_bucket_handlers.end(); ++handler_it_it)
    {
      DOWN_BUCKET_LIST* down_buckets = (*handler_it_it)->second->get_down_buckets();
      if (down_buckets == NULL || down_buckets->empty())
      {
        continue;
      }

      for (DOWN_BUCKET_LIST::const_iterator bucket_it = down_buckets->begin(); bucket_it != down_buckets->end(); ++bucket_it)
      {
        // already operate
        if (extra_bucket_map_->find(*bucket_it) != extra_bucket_map_->end())
        {
          continue;
        }

        get_handler_index_of_bucket(*bucket_it, (*handler_it_it)->first, indexs);

        (*extra_bucket_map_)[*bucket_it] = indexs.empty() ? -1 :        // this bucket has no handler service
          indexs[hash_bucket(*bucket_it) % indexs.size()];                     // hash sharding to one handler
      }
    }
  }

  void handlers_node::get_handler_index_of_bucket(int32_t bucket, const cluster_info& exclude, std::vector<int32_t>& indexs)
  {
    indexs.clear();
    for (CLUSTER_HANDLER_MAP::const_iterator handler_it = handler_map_->begin();
         handler_it != handler_map_->end(); ++handler_it)
    {
      // bucket is from exclude, no need check, skip directly
      if (handler_it->first == exclude)
      {
        continue;
      }

      DOWN_BUCKET_LIST* down_buckets = handler_it->second->get_down_buckets();
      // bucket is not down in this cluster handler
      if (NULL == down_buckets || down_buckets->empty() || down_buckets->find(bucket) == down_buckets->end())
      {
        indexs.push_back(handler_it->second->get_index());
      }
    }
  }

  int32_t handlers_node::bucket_to_handler_index(int32_t bucket)
  {
    // no alive cluster
    if (handlers_->empty())
    {
      return -1;
    }

    // check whether this bucket is in special condition
    // (eg. one cluster's ds is up after down, but before reset)
    if (!extra_bucket_map_->empty())
    {
      BUCKET_INDEX_MAP::const_iterator it = extra_bucket_map_->find(bucket);
      if (it != extra_bucket_map_->end())
      {
        return it->second;
      }
    }

    // hash sharding bucket to cluster
    return hash_bucket(bucket) % handlers_->size();
  }


  //////////////////////////// bucket_shard_cluster_handler_manager ////////////////////////////
  bucket_shard_cluster_handler_manager::bucket_shard_cluster_handler_manager()
  {
    current_ = new handlers_node(new CLUSTER_HANDLER_MAP(), new CLUSTER_HANDLER_LIST(), new BUCKET_INDEX_MAP());
    current_->ref();
    using_head_ = new handlers_node();
  }

  bucket_shard_cluster_handler_manager::~bucket_shard_cluster_handler_manager()
  {
    // clear all cluster handler
    current_->clear(true);
    if (current_ != NULL)
    {
      delete current_;
    }

    // clear using handlers node
    if (using_head_ != NULL)
    {
      handlers_node* prev_node = using_head_;
      handlers_node* node = using_head_;
      while (node->next_ != NULL)
      {
        node = node->next_;
        delete prev_node;
        prev_node = node;
      }
      if (prev_node != NULL)
      {
        delete prev_node;
      }
    }
  }

  bool bucket_shard_cluster_handler_manager::update(const CLUSTER_INFO_LIST& cluster_infos)
  {
    // mutex update
    tbsys::CThreadGuard guard(&lock_);
    bool urgent = false;
    handlers_node* new_handlers_node =
      new handlers_node(new CLUSTER_HANDLER_MAP(),
                        new CLUSTER_HANDLER_LIST(),
                        new BUCKET_INDEX_MAP());
    new_handlers_node->update(cluster_infos, *current_);

    if (new_handlers_node->handler_map_->empty())
    {
      log_warn("NO alive cluster on service.");
      urgent = true;
    }

    handlers_node* old_handlers_node = current_;
    // ref
    new_handlers_node->ref();
    // clear update temp data
    new_handlers_node->clear_update_temp();
    // update
    current_ = new_handlers_node;

    // just expect to skip the occurrence between getting current_ and current_->ref()
    ::usleep(200);
    // mark clear out-of-service cluster handler
    old_handlers_node->mark_clear(*current_);
    add_to_using_node_list(old_handlers_node);
    cleanup_using_node_list();

    return urgent;
  }

  cluster_handler* bucket_shard_cluster_handler_manager::pick_handler(const data_entry& key)
  {
    return current_->pick_handler(key);
  }

  cluster_handler* bucket_shard_cluster_handler_manager::pick_handler(int32_t index)
  {
    return current_->pick_handler(index);
  }

  void bucket_shard_cluster_handler_manager::close()
  {

  }

  // lock_ hold
  void bucket_shard_cluster_handler_manager::add_to_using_node_list(handlers_node* node)
  {
    if (NULL == node)
    {
      return;
    }
    node->prev_ = using_head_;
    node->next_ = using_head_->next_;
    if (using_head_->next_ != NULL)
    {
      using_head_->next_->prev_ = node;
    }
    using_head_->next_ = node;
    node->unref();
  }

  // lock hold
  void bucket_shard_cluster_handler_manager::cleanup_using_node_list()
  {
    if (using_head_ != NULL && using_head_->next_ != NULL)
    {
      handlers_node* node = using_head_->next_;
      while (node != NULL)
      {
        if (node->get_ref() <= 0)
        {
          node->prev_->next_ = node->next_;
          if (node->next_ != NULL)
          {
            node->next_->prev_ = node->prev_;
          }
          delete node;
        }
        node = node->next_;
      }
    }
  }

  //////////////////////////// utility function ////////////////////////////

  // use integer hash to optimize uniform condition compared to silly mod sharding
  int32_t hash_bucket(int32_t h)
  {
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >> 10);
    h += (h << 3);
    h ^= (h >> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >> 16);
    // return h;
  }

  int parse_config(const CONFIG_MAP& config_map, const char* key, std::string& value)
  {
    CONFIG_MAP::const_iterator it = config_map.find(key);
    if (it == config_map.end())
    {
      return TAIR_RETURN_FAILED;
    }
    value.assign(it->second);
    return TAIR_RETURN_SUCCESS;
  }

  int parse_config(const std::map<std::string, std::string>& config_map,
                   const char* key, const char* delim, std::vector<std::string>& values)
  {
    std::map<std::string, std::string>::const_iterator it = config_map.find(key);
    if (it == config_map.end())
    {
      return TAIR_RETURN_FAILED;
    }
    else
    {
      split_str(it->second.c_str(), delim, values);
    }

    return values.empty() ? TAIR_RETURN_FAILED : TAIR_RETURN_SUCCESS;
  }

  void split_str(const char* str, const char* delim, std::vector<std::string>& values)
  {
    if (NULL == str)
    {
      return;
    }
    if (NULL == delim)
    {
      values.push_back(str);
    }
    else
    {
      int32_t delim_len = strlen(delim);
      const char* last_pos = str;
      const char* pos = NULL;
      while ((pos = strstr(last_pos, delim)) != NULL)
      {
        values.push_back(std::string(last_pos, pos - last_pos));
        last_pos += delim_len;
      }

      if (str == last_pos)
      {
        values.push_back(std::string(str));
      }
    }
  }

}
