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
#ifndef TAIR_CLIENT_API
#define TAIR_CLIENT_API

#include <vector>
#include <map>

#include "define.hpp"
#include "data_entry.hpp"
#include "data_entry_local_cache.h"
#include "inval_stat.hpp"

namespace tair {

  class i_tair_client_impl;
  using namespace std;
  using namespace tair::common;

  class tair_client_api
  {
  public:
    tair_client_api();
    ~tair_client_api();

    // capability, cache size
    // expire, entry expire time, unit ms
    // now, just support cache read
    void setup_cache(int area, size_t capacity = 30);

    data_entry_local_cache* get_local_cache(int area)
    {
      if (area < 0 || area >= TAIR_MAX_AREA_COUNT)
        return NULL;
      return cache_impl[area];
    }

    static const int MAX_ITEMS = 65535;
    static const int ALL_ITEMS = MAX_ITEMS + 1;

    /**
     * @brief connect to tair cluster
     *
     * @param master_addr     master configserver addr [ip:port],default port is 5198
     * @param slave_addr      slave configserver addr [ip:port]
     * @param group_name      group_name
     *
     * @return true -- success,false -- fail
     */
    bool startup(const char *master_addr,const char *slave_addr,const char *group_name);

    /**
     * @brief connect to data server
     *
     * @param server_addr		data server addr [ip:port],default port is 5191
     *
     * @return true -- success,false -- fail
     */
    bool directup(const char *server_addr);

    /**
     * @brief connect to tair cluster
     *
     * @param master_cfgsvr master configserver ip:port
     * @param slave_cfgsvr  master configserver ip:port
     * @param group_name    group name
     *
     * @return  true -- success,false -- fail
     */
    // bool startup(uint64_t master_cfgsvr,uint64_t slave_cfgsvr,const char *group_name);

    /**
     * @brief close tairclient,release resource
     */
    void close();

    /**
     * @brief put data to tair
     *
     * @param area     namespace
     * @param key      key
     * @param data     data
     * @param expire   expire time,realtive time
     * @param version  version,if you don't care the version,set it to 0
     * @param fill_cache whether fill cache when put(only meaningful when server support embedded cache).
     *
     * @return 0 -- success, otherwise fail,you can use get_error_msg(ret) to get more information.
     */
    int put(int area,
        const data_entry &key,
        const data_entry &data,
        int expire,
        int version,
        bool fill_cache = true);

    /**
     * @brief get data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     * @param data    data,a pointer,the caller must release the memory.
     *
     * @return 0 -- success, otherwise fail.
     */
    int get(int area,
        const data_entry &key,
        data_entry*& data);

    /**
     * @brief get multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    keys list, vector type
     * @param data    data,data hash map,the caller must release the memory.
     *
     * @return 0 -- success, TAIR_RETURN_PARTIAL_SUCCESS -- partly success, otherwise fail.
     */
    int mget(int area,
        const vector<data_entry *> &keys,
        tair_keyvalue_map& data);

    /**
     * @brief  remove data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     *
     * @return  0 -- success, otherwise fail.
     */
    int remove(int area,
        const data_entry &key);


    /**
     * @brief delete data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int invalidate(int area, const data_entry &key, const char *groupname);

    /**
     * @brief delete data from tair cluster
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int invalidate(int area, const data_entry &key);

    /**
     * @brief delete data with prefix key from tair cluster
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_invalidate(int area, const data_entry &key, const char *groupname);

    /**
     * @brief delete data with prefix key from tair cluster
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_invalidate(int area, const data_entry &key);

    /**
     * @brief hide data
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int hide_by_proxy(int area, const data_entry &key, const char* groupname);

    /**
     * @brief hide data
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int hide_by_proxy(int area, const data_entry &key);

    /**
     * @brief hide data with prefix key
     *
     * @param area    namespace
     * @param key     key
     * @param groupname    group's name.
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_hide_by_proxy(int area, const data_entry &key, const char *groupname);

    /**
     * @brief hide data with prefix key
     *
     * @param area    namespace
     * @param key     key
     *
     * @return TAIR_RETURN_SUCCESS -- success, otherwise fail.
     */
    int prefix_hide_by_proxy(int area, const data_entry &key);

    /**
     * @brief to hide one item
     * @param area: namespace
     * @param key: key to hide
     */
    int hide(int area, const data_entry &key);

    /**
     * @brief to get one hidden item
     * @param area: namespace
     * @param key: key to get
     * @param value: ptr holding the value returned
     */
    int get_hidden(int area, const data_entry &key, data_entry *&value);
    /**
     * @brief to get one item with prefix key
     * @param area: namespace
     * @param pkey: primary key
     * @param skey: secondary key
     * @param value: ptr holding the value returned
     */
    int prefix_get(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    /**
     * @brief to put one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary key & secondary key
     * @param value: value to put
     * @param expire&version: expire & version
     */
    int prefix_put(int area, const data_entry &pkey, const data_entry &skey,
        const data_entry &value, int expire, int version);
    /**
     * @brief to hide one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary & secondary key
     */
    int prefix_hide(int area, const data_entry &pkey, const data_entry &skey);

    /**
     * @brief like prefix_hide, but with multiple skeys
     * @param pkey: primary key
     * @param skey_set: set of skeys
     * @param key_code_map: return code of each failed key, would be cleared before filled
     * @return success, partial success, or others
     */
    int prefix_hides(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    /**
     * @brief to get one hidden item with prefix key
     * @param pkey & skey: primary & secondary key
     * @param value: to receive the result
     */
    int prefix_get_hidden(int area, const data_entry &pkey, const data_entry &skey, data_entry *&value);

    /**
     * @brief to remove one item with prefix key
     * @param area: namespace
     * @param pkey & skey: primary & secondary key
     */
    int prefix_remove(int area, const data_entry &pkey, const data_entry &skey);

    /**
     * @brief like prefix_remove, but with multiple skeys
     * @param pkey: primary key
     * @param skey_set: set of skeys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    int prefix_removes(int area, const data_entry &pkey, const tair_dataentry_set &skey_set, key_code_map_t &key_code_map);

    /**
     * @brief 
     * @param pkey: primary key for range query
     * @param start_key: start skey to seek, seek from the first skey if equals ""
     * @param end_key: end skey to seek, seek to the last skey if equals ""
     * @param offset: skip first N skey
     * @param limit: limit response less than N skey
     * @param values: result skey with ascending sortting, free memory by user.
     * @param type: CMD_RANGE_ALL means get key&value response, 
     *              CMD_RANGE_VALUE_ONL means get only values,
     *              CMD_RANGE_KEY_ONL means get only keys.
     * @return TAIR_RETURN_SUCCESS -- success, <0 -- fail, or TAIR_HAS_MORE_DATA -- limit by package size(1M default).
     */
    int get_range(int area, const data_entry &pkey, const data_entry &start_key, const data_entry &end_key, 
        int offset, int limit, vector<data_entry *> &values, short type=CMD_RANGE_ALL);

    /**
     * @brief remove multiple items, which were merged with the same prefix key
     * @param mkey_set: set of merged keys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    //!NOTE called by invalid server
    int removes(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    /**
     * @brief hide multiple items, which were merged with the same prefix key
     * @param mkey_set: set of merged keys
     * @param key_code_map: return code of each failed skey, would be cleared before filled
     * @return success, partial success, or others
     */
    //!NOTE called by invalid server
    int hides(int area, const tair_dataentry_set &mkey_set, key_code_map_t &key_code_map);

    /**
     * @brief remove multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    key list, vector type
     *
     * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
     */
    int mdelete(int area,
        const vector<data_entry*> &key);
    /**
     * @brief invalid multi  data from tair cluster
     *
     * @param area    namespace
     * @param keys    key list, vector type
     *
     * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
     */
    int minvalid(int area,
        vector<data_entry*> &key);
    /**
     * @brief incr
     *
     * @param area            namespace
     * @param key             key
     * @param count           add (count) to original value.
     * @param retCount                the latest value
     * @param expire          expire time
     * @param initValue               initialize value
     *
     * @return 0 -- success, otherwise fail.
     */
    int incr(int area,
        const data_entry& key,
        int count,
        int *ret_count,
        int init_value = 0,
        int expire = 0);
    // opposite to incr
    int decr(int area,
        const data_entry& key,
        int count,
        int *ret_count,
        int init_value = 0,
        int expire = 0);

    /**
     *
     * ******this method is deprecated,don't use******
     *
     * @brief   add count
     *
     * @param area            namespace
     * @param key             key
     * @param count           add (count) to original value.
     * @param retCount                the latest value
     * @param initValue               the initialize value
     *
     * @return  0 -- success ,otherwise fail.
     */
    int add_count(int area,
        const data_entry &key,
        int count,
        int *ret_count,
        int init_value = 0);

    /**
     * @brief set count to key's value, ignore whether this key exists or is not
     *        count type.
     *
     * @param area            namespace
     * @param key             key
     * @param count           set count value
     * @param expire          expire time
     * @param version         version
     *
     * @return 0 -- success, otherwise fail.
     */
    int set_count(int area, const data_entry& key, int count, int expire = 0, int version = 0);

    /**
     * @brief Once a key is locked, it can NOT be updated(put/incr/decr), can only
     *        be get or delete. A locked key can be unlocked(unlock)
     *
     * @param area            namespace
     * @param key             key
     *
     * @return TAIR_RETURN_SUCCESS -- success, TAIR_RETURN_LOCK_EXIST -- fail: already locked
     *         otherwise -- fail
     */
    int lock(int area, const data_entry& key);

    /**
     * @brief opposite to lock
     *
     * @param area            namespace
     * @param key             key
     *
     * @return TAIR_RETURN_SUCCESS -- success,
     *         TAIR_RETURN_LOCK_NOT_EXIST -- fail: key is not locked
     *         otherwise -- fail
     */
    int unlock(int area, const data_entry& key);

    /**
     * @brief set the expire time
     *
     * @param area     namespace
     * @param key      key
     * @param expire   expire time,realtive time
     *
     * @return 0 -- success, otherwise fail.
     */
    int expire(int area,
        const data_entry& key,
        int expire);

#if 0
    /**
     *
     * items :  just support four types: int32_t int64_t double string
     *
     * @brief  add items
     *
     * @param area            namespace
     * @param key             key
     * @param data            data,vector<type>
     * @param version         version,the same as put
     * @param expired         expire time ,the same as put
     * @param max_count               how many items do you want to save
     *
     * @return  0 -- success, otherwise fail
     */
#define ADD_ITEMS(type)                                                 \
    int add_items(int area,const data_entry &key,type data,int version,int expired,int max_count=500)

    ADD_ITEMS(std::vector<int>&);
    ADD_ITEMS(std::vector<int64_t>&);
    ADD_ITEMS(std::vector<double>&);
    ADD_ITEMS(std::vector<std::string>&);


    /**
     * @brief get items
     *
     * @param area            namespace
     * @param key             key
     * @param offset          item's offset
     * @param count           the item's count you want to get,must > 0,
     *                                iff you want to get all items,set count to ALL_ITEMS;
     * @param std::vector       result,you should know the item's type
     *
     * @return  0 -- success, otherwise fail
     */
#define GET_ITEMS_FUNC(FuncName,type)                                   \
    int FuncName(int area, const data_entry &key, int offset, int count, type result)

    GET_ITEMS_FUNC(get_items,std::vector<int>&);
    GET_ITEMS_FUNC(get_items,std::vector<int64_t>&);
    GET_ITEMS_FUNC(get_items,std::vector<double>&);
    GET_ITEMS_FUNC(get_items,std::vector<std::string>&);

    /**
     * @brief get and remove items
     *
     * @param area            namespace
     * @param key             key
     * @param offset          item's offset
     * @param count           the item's count you want to get
     *                                if count >= really items count,this key will be deleted.
     * @param std::vector       result,you should know the item's type
     *
     * @return  0 -- success, otherwise fail
     */

    GET_ITEMS_FUNC(get_and_remove,std::vector<int>&);
    GET_ITEMS_FUNC(get_and_remove,std::vector<int64_t>&);
    GET_ITEMS_FUNC(get_and_remove,std::vector<double>&);
    GET_ITEMS_FUNC(get_and_remove,std::vector<std::string>&);

    /**
     * @brief remove items
     *
     * @param area            namespace
     * @param key             key
     * @param offset          the offset of item
     * @param count           the count of item that you want to remove
     *
     * @return  0 -- success, otherwise -- fail
     */
    int remove_items(int area,
        const data_entry &key,
        int offset,
        int count);

    /**
     * @brief get item count
     *
     * @param area            namespace
     * @param key             key
     *
     * @return  if ret > 0,ret is item's count,else failed.
     */
    int get_items_count(int area,const data_entry& key);

#endif

    /**
     * @brief set log level
     *
     * @param 
     */
    void set_log_level(const char* level);

    /**
     * @brief set log file
     *
     * @param 
     */
    void set_log_file(const char* log_file);

    /**
     * @brief set timout of each method
     *
     * @param timeout
     */
    void set_timeout(int timeout);

    /**
     * @brief set read from random server
     *
     * @param rand_flag: true means read from all replicas.
     */
    void set_randread(bool rand_flag);

#ifdef WITH_COMPRESS
    /**
     * @brief set the compress algorithm type
     *
     * @param type: the type of compress algorithm, 0 means zlib
     */
    void set_compress_type(TAIR_COMPRESS_TYPE type);

    /**
     * @brief set the compress threshold
     *
     * @param threshold: the threshold of compress, if the size of data is lower than
     *                   this value, skip the compress method. the unit is byte.
     */
    void set_compress_threshold(int threshold);
#endif

    /**
     * @brief get bucket count of tair cluster
     *
     * @return  bucket count
     */
    uint32_t get_bucket_count() const;

    /**
     * @brief get copy count of tair cluster
     *
     * @return  copy count
     */
    uint32_t get_copy_count() const;

    const char *get_error_msg(int ret);

    void get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const;

    void query_from_configserver(uint32_t query_type, const std::string &groupname, std::map<std::string, std::string> &out, uint64_t serverId);

    uint32_t get_config_version() const;

    int64_t ping(uint64_t server_id);

    /*** Following interface is specially useful for multi-cluster (FastDump, etc.). ***/
    /**
     ** @brief batch put data to tair
     ** @ NOTE: This interface is specially for dump data to server
     **         in some specific condition(FastDump etc.).
     **         Do NOT use it only when you konw what you're doing.
     **
     ** @param area     namespace
     ** @param kvs      key && value
     ** @param fail_request fail_keys
     ** @param compress whether compress data
     **
     ** @return 0 -- success, otherwise fail,you can use get_error_msg(ret) to get more information.
     **/

    int mput(int area,
        const tair_client_kv_map& kvs,
        int& fail_request,
        bool compress = true);

    /**
     * @param group: group names of which you wanna know the status
     * @param status: group statuses, in the format of 'group_1: group_status=on'
     */
    int get_group_status(vector<string>& group, vector<string>& status);

    /**
     * @param group: group names of which you wanna know the tmp down server
     * @param down_servers: return tmp down server, in the format of 'group_1: tmp_down_server=xx'
     */
    int get_tmp_down_server(vector<string>& group, vector<string>& down_servers);

    /**
     * @param group: group name
     * @param status: on/off
     */
    int set_group_status(const char* group, const char* status);

    /**
     * @param group: group to reset
     * @param dss  : if not NULL, then reset ds whose address is specified in `dss
     */
    int reset_server(const char* group, vector<string>* dss = NULL);

    /**
     * @param ds_addr  : if not NULL, then only flush ds whose address is `ds_addr
     */
    int flush_mmt(const char* ds_addr = NULL);
    /**
     * @param group: group to reset db
     * @param ds_addr  : if not NULL, then only reset ds whose address is `ds_addr
     */
    int reset_db(const char* ds_addr = NULL);

    /**
     * @brief get invalid servers of tair cluster
     *
     * @param inval_server_list  all the invalid servers of the cluster.
     *
     * return TAIR_RETURN_SUCCESS, etherwise failed.
     */
    int retrieve_invalidserver(vector<uint64_t> &inval_server_list);

    /**
     * @brief send the command the the invalid_server, indicated by @param invalid_server_id,
     * to re-do the request.
     *
     * @param invalid_server_id  the id of the invalid servers .
     *
     * return TAIR_RETURN_SUCCESS, etherwise failed.
     */
    int retry_all(uint64_t invalid_server_id);

    /**
     * @brief send the command to all the invalid server(s), to re-do the request.
     *
     * return TAIR_RETURN_SUCCESS -success, TAIR_RETURN_SUCCESS -partially success, etherwise failed.
     */
    int retry_all();

    /**
     * @brief get the statistics data from the invalid server, indicated by @param invalid_server_id.
     *
     * @param invalid_server_id  the id of the invalid servers .
     * @param stat  statistics data from the invalid server.
     *
     * return TAIR_RETURN_SUCCESS, etherwise failed.
     */
    int query_from_invalidserver(uint64_t invalid_server_id, inval_stat_data_t* &stat);

    /**
     * @brief get the statistics data from  all the invalid server(s).
     *
     * @param stat  map ={<invalid server id, statistics data>}. statistics data from the invalid server.
     *
     * return TAIR_RETURN_SUCCESS -success, TAIR_RETURN_SUCCESS -partially success, etherwise failed.
     */
    int query_from_invalidserver(std::map<uint64_t, inval_stat_data_t*> &stats);

protected:
    void invalid_local_cache(int area, const std::vector<data_entry*> &key);

    int32_t timeout_ms;
    i_tair_client_impl *impl;
    typedef data_entry_local_cache cache_type;
    cache_type *cache_impl[TAIR_MAX_AREA_COUNT];
  };
}
#endif
