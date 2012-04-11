/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: cluster_handler_manager.hpp 690 2012-04-09 02:09:34Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_CLIENT_CLUSTER_HANDLER_MANAGER_H
#define TAIR_CLIENT_CLUSTER_HANDLER_MANAGER_H

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)
#include <unordered_map>
#else
#include <tr1/unordered_map>
namespace std {
  using tr1::hash;
  using tr1::unordered_map;
}
#endif

#include <tbsys.h>
#include "tair_client_api_impl.hpp"

namespace tair
{
  struct cluster_info
  {
    cluster_info()
    {}
    cluster_info(const std::string& m_cs_addr, const std::string& s_cs_addr, const std::string& g_name)
      : master_cs_addr_(m_cs_addr), slave_cs_addr_(s_cs_addr), group_name_(g_name)
    {}

    inline bool operator==(const cluster_info& info) const
    {
      // cluster are separated by group_name now.
      return group_name_ == info.group_name_;
    }

    inline std::string debug_string() const
    {
      return std::string("[ ") + master_cs_addr_ + " | " + slave_cs_addr_ + " | " + group_name_ + " ]";
    }

    std::string master_cs_addr_;
    std::string slave_cs_addr_;
    std::string group_name_;
  };

  struct hash_cluster_info
  {
    size_t operator() (const cluster_info& info) const
    {
      // cluster are separated by group_name now.
      return std::hash<std::string>()(info.group_name_);
    }
  };

  class cluster_handler;

  typedef std::vector<cluster_info> CLUSTER_INFO_LIST;
  typedef std::unordered_map<cluster_info, cluster_handler*, hash_cluster_info> CLUSTER_HANDLER_MAP;
  typedef std::vector<cluster_handler*> CLUSTER_HANDLER_LIST;
  typedef std::vector< CLUSTER_HANDLER_MAP::iterator > CLUSTER_HANDLER_MAP_ITER_LIST;
  typedef std::vector<uint64_t> DOWN_SERVER_LIST;
  typedef std::set<int32_t> DOWN_BUCKET_LIST;
  typedef std::unordered_map<int32_t, int32_t> BUCKET_INDEX_MAP;
  typedef tbsys::STR_STR_MAP CONFIG_MAP;

  static const int32_t DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS = 2000;

  class cluster_handler
  {
  public:
    cluster_handler();
    ~cluster_handler();

    void init(const char* master_cs_addr, const char* slave_cs_addr, const char* group_name);
    void init(const cluster_info& info);
    bool start(int32_t timeout_ms = DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS);
    int retrieve_server_config(CONFIG_MAP& config_map, uint32_t& version, bool update = true);
    void get_buckets_by_server(uint64_t server_id, DOWN_BUCKET_LIST& buckets);

    inline tair_client_impl* get_client() const
    {
      return client_;
    }
    inline const cluster_info& get_cluster_info() const
    {
      return info_;
    }

    // convenient use when update
    inline void set_index(int32_t index)
    {
      index_ = index;
    }
    inline int32_t get_index() const
    {
      return index_;
    }
    inline void set_down_servers(DOWN_SERVER_LIST* servers)
    {
      down_servers_ = servers;
    }
    inline DOWN_SERVER_LIST* get_down_servers()
    {
      return down_servers_;
    }
    inline void set_down_buckets(DOWN_BUCKET_LIST* buckets)
    {
      down_buckets_ = buckets;
    }
    inline DOWN_BUCKET_LIST* get_down_buckets()
    {
      return down_buckets_;
    }
    inline void clear_update_temp()
    {
      if (down_servers_ != NULL)
      {
        delete down_servers_;
        down_servers_ = NULL;
      }
      if (down_buckets_ != NULL)
      {
        delete down_buckets_;
        down_buckets_ = NULL;
      }
    }

  private:
    // use tair_client_impl to shadow modification from public tair_client_api user
    tair_client_impl* client_;
    cluster_info info_;

    // for convenient use when update
    int32_t index_;
    DOWN_SERVER_LIST* down_servers_;
    DOWN_BUCKET_LIST* down_buckets_;
  };

  class cluster_handler_manager
  {
  public:
    cluster_handler_manager() : timeout_ms_(DEFAULT_CLUSTER_HANDLER_TIMEOUT_MS) {};
    virtual ~cluster_handler_manager() {};

    virtual bool update(const CLUSTER_INFO_LIST& cluster_infos) = 0;
    virtual void* current() = 0;
    virtual cluster_handler* pick_handler(const data_entry& key) = 0;
    virtual cluster_handler* pick_handler(int32_t index) = 0;
    virtual void close() = 0;

    void set_timeout(int32_t timeout_ms)
    {
      if (timeout_ms > 0)
      {
        timeout_ms_ = timeout_ms;
      }
    }

  protected:
    int32_t timeout_ms_;
  private:
    cluster_handler_manager(const cluster_handler_manager&);
  };

  class handlers_node
  {
  public:
    handlers_node();
    explicit handlers_node(CLUSTER_HANDLER_MAP* handler_map,
                           CLUSTER_HANDLER_LIST* handlers,
                           BUCKET_INDEX_MAP* extra_bucket_map);
    ~handlers_node();

    void update(const CLUSTER_INFO_LIST& cluster_infos, const handlers_node& diff_handlers_node);
    cluster_handler* pick_handler(const data_entry& key);
    cluster_handler* pick_handler(int32_t index);
    void clear(bool force);
    void mark_clear(const handlers_node& diff_handlers_node);
    void clear_update_temp();

    inline int32_t get_handler_count()
    {
      return handlers_->size();
    }
    inline void ref()
    {
      atomic_inc(&ref_);
    }
    inline void unref()
    {
      atomic_dec(&ref_);
      if (atomic_read(&ref_) <= 0)
      {
        // we collect handlers_node by list,
        // delete() will occur updating list concurrently,
        // just gc some memory here.
        clear(false);
      }
    }
    inline int32_t get_ref()
    {
      return atomic_read(&ref_);
    }
    inline void set_timeout(int32_t timeout_ms)
    {
      if (timeout_ms > 0)
      {
        timeout_ms_ = timeout_ms;
      }
    }

  private:
    void construct_handler_map(const CLUSTER_INFO_LIST& cluster_infos,
                               const handlers_node& diff_handlers_node,
                               CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers);
    void construct_handlers();
    void construct_extra_bucket_map(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers);
    void collect_down_bucket(CLUSTER_HANDLER_MAP_ITER_LIST& has_down_server_handlers,
                             CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers);
    void shard_down_bucket(const CLUSTER_HANDLER_MAP_ITER_LIST& has_down_bucket_handlers);
    void get_handler_index_of_bucket(int32_t bucket, const cluster_info& exclude, std::vector<int32_t>& indexs);

    int32_t bucket_to_handler_index(int32_t bucket);

  private:
    handlers_node(const handlers_node&);

  public:
    atomic_t ref_;
    handlers_node* prev_;
    handlers_node* next_;

    // cluster handler timeout
    int32_t timeout_ms_;
    // multi-cluster must have same bucket count
    int32_t bucket_count_;

    // to lookup handler by cluster_info when update
    CLUSTER_HANDLER_MAP* handler_map_;
    // to lookup handler by index of sharding when pick
    CLUSTER_HANDLER_LIST* handlers_;
    // to lookup handler index in handlers_ by bucket when pick
    BUCKET_INDEX_MAP* extra_bucket_map_;
  };

  // shard bucket to clusters
  class bucket_shard_cluster_handler_manager : public cluster_handler_manager
  {
  public:
    bucket_shard_cluster_handler_manager();
    virtual ~bucket_shard_cluster_handler_manager();

    void* current()
    {
      return current_;
    }

    bool update(const CLUSTER_INFO_LIST& cluster_infos);
    cluster_handler* pick_handler(const data_entry& key);
    cluster_handler* pick_handler(int32_t index);
    void close();

  private:
    void add_to_using_node_list(handlers_node* node);
    void cleanup_using_node_list();

  private:
    // mutex for update
    tbsys::CThreadMutex lock_;
    handlers_node* current_;
    // avoid wild handler crash when concurrent using and updating(delete, etc),
    // handlers_node has reference count to be self-responsible to destruction.
    handlers_node* using_head_;
  };

  extern int32_t hash_bucket(int32_t bucket);
  extern int parse_config(const CONFIG_MAP& config_map, const char* key, std::string& value);
  extern int parse_config(const CONFIG_MAP& config_map,
                          const char* key, const char* delim, std::vector<std::string>& values);
  extern void split_str(const char* str, const char* delim, std::vector<std::string>& values);
}
#endif
