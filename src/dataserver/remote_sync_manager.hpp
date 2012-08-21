/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RemoteSyncManager:
 *   Manage synchronization between local and remote cluster.
 *   Data synchronization is based on record logger interface, so different
 *   user should implement its own record logger(reader/writer).
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H
#define TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H

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

#include "common/tair_atomic.hpp"
#include "client/tair_client_api_impl.hpp"

class tair::common::RecordLogger;

namespace tair
{
  class tair_client_impl;
  class tair_manager;
  class ClusterHandler;

  typedef std::unordered_map<std::string, ClusterHandler*> CLUSTER_HANDLER_MAP;

  // cluster conf and client handler
  class ClusterHandler
  {
  public:
    ClusterHandler() : client_(NULL)
    {}
    ~ClusterHandler()
    {
      if (client_ != NULL)
      {
        delete client_;
      }
    }

    inline bool conf_inited() 
    {
      return !master_cs_addr_.empty();
    }
    inline int init_conf(std::vector<std::string>& conf)
    {
      int ret = TAIR_RETURN_SUCCESS;
      if (conf.size() == 4)
      {
        master_cs_addr_ = conf[0];
        slave_cs_addr_ = conf[1];
        group_name_ = conf[2];
        timeout_ms_ = atoi(conf[3].c_str());

        char buf[128];
        int size = snprintf(buf, sizeof(buf), "%"PRI64_PREFIX"d;%"PRI64_PREFIX"d;%s",
                            tbsys::CNetUtil::strToAddr(master_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT),
                            tbsys::CNetUtil::strToAddr(slave_cs_addr_.c_str(), TAIR_CONFIG_SERVER_DEFAULT_PORT),
                            group_name_.c_str());
        info_.assign(buf, size);
      }
      else
      {
        ret = TAIR_RETURN_FAILED;  
      }
      return ret;
    }
    inline int start()
    {
      if (client_ == NULL)
      {
        client_ = new tair_client_impl();
        client_->set_timeout(timeout_ms_);
      }

      return client_->startup(master_cs_addr_.c_str(), slave_cs_addr_.c_str(), group_name_.c_str()) ?
        TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED;
    }
    inline tair_client_impl* client()
    {
      return client_;
    }
    inline std::string info()
    {
      return info_;
    }
    inline std::string debug_string()
    {
      char tmp[10];
      snprintf(tmp, sizeof(tmp), "%d", timeout_ms_);
      return std::string("[") + master_cs_addr_ + "," + slave_cs_addr_ + "," + group_name_ + "," + tmp + "]";
    }

  private:
    tair_client_impl* client_;
    std::string master_cs_addr_;
    std::string slave_cs_addr_;
    std::string group_name_;
    int32_t timeout_ms_;
    std::string info_;
  };

  class RemoteSyncManager : public tbsys::CDefaultRunnable
  {
    // failed record stuff
    typedef struct FailRecord
    {
      FailRecord(common::data_entry* key, std::string cluster_info, int code)
        : key_(key), cluster_info_(cluster_info), code_(code) {}
      FailRecord(const char* data, int32_t size)
      {
        uint32_t per_size = tair::util::coding_util::decode_fixed32(data);
        cluster_info_.assign(data + sizeof(int32_t), per_size);
        data += per_size + sizeof(int32_t);
        per_size = tair::util::coding_util::decode_fixed32(data);
        // outer will delete this
        key_ = new data_entry(data + sizeof(int32_t), per_size, true);
        data += per_size + sizeof(int32_t);
        code_ = tair::util::coding_util::decode_fixed32(data);
      }
      ~FailRecord()
      {
      }

      const char* encode()
      {
        if (encode_data_.empty())
        {
          char buf[sizeof(int32_t)];
          encode_data_.reserve(key_->get_size() + cluster_info_.size() + 3 * sizeof(int32_t));
          tair::util::coding_util::encode_fixed32(buf, cluster_info_.size());
          encode_data_.append(buf, sizeof(buf));
          encode_data_.append(cluster_info_.data(), cluster_info_.size());
          tair::util::coding_util::encode_fixed32(buf, key_->get_size());
          encode_data_.append(buf, sizeof(buf));
          encode_data_.append(key_->get_data(), key_->get_size());
          tair::util::coding_util::encode_fixed32(buf, code_);
          encode_data_.append(buf, sizeof(buf));
        }
        return encode_data_.data();
      }
      int32_t encode_size()
      {
        return encode_data_.size();
      }

      common::data_entry* key_;
      std::string cluster_info_;
      int code_;
      std::string encode_data_;
    } FailRecord;

    // avoid dummy data_entry copy, use ref for aync concurrent use.
    typedef struct DataEntryWrapper
    {
    public:
      DataEntryWrapper(data_entry* entry)
        : entry_(entry), ref_(1) {}
      inline data_entry* entry()
      {
        return entry_;
      }
      inline uint32_t ref()
      {
        return tair::common::atomic_inc(&ref_);
      }
      inline uint32_t unref()
      {
        uint32_t ret = tair::common::atomic_dec(&ref_);
        if (ret <= 0)
        {
          delete this;
        }
        return ret;
      }

    private:
      ~DataEntryWrapper()
      {
        if (entry_ != NULL)
        {
          delete entry_;
        }
      }
      common::data_entry* entry_;
      volatile uint32_t ref_;
    } DataEntryWrapper;

    // remote synchronization callback arg
    typedef struct CallbackArg
    {
      CallbackArg(RecordLogger* fail_logger, TairRemoteSyncType type,
                  DataEntryWrapper* key, ClusterHandler* handler)
        : fail_logger_(fail_logger), type_(type), key_(key), handler_(handler)
      {
        key_->ref();
      }
      ~CallbackArg()
      {
        key_->unref();
      }

      RecordLogger* fail_logger_;
      TairRemoteSyncType type_;
      DataEntryWrapper* key_;
      ClusterHandler* handler_;
    } CallbackArg;

  public:
    explicit RemoteSyncManager(tair_manager* tair_manager);
    ~RemoteSyncManager();

    int init();
    int add_record(TairRemoteSyncType type, data_entry* key, data_entry* value);
    void run(tbsys::CThread*, void* arg);

    static void callback(int ret, void* arg);
    static int log_fail_record(RecordLogger* logger, TairRemoteSyncType type, FailRecord& record);

  private:
    int32_t remote_sync_thread_count();
    int32_t retry_remote_sync_thread_count();

    // check whether current node is the master one of key
    bool is_master_node(data_entry* key);
    // check whether this type record is valid
    bool is_valid_record(TairRemoteSyncType type);
    // check whether this type record need value to do remote synchronization
    bool is_remote_sync_need_value(TairRemoteSyncType type);

    int init_sync_conf();
    int init_sync_client();

    int do_remote_sync(int32_t index);
    int do_retry_remote_sync(int32_t index);

    // Process one record.
    // Returning TAIR_RETURN_SUCCESS means this record is out of remote synchronization
    // purpose(maybe synchronization succeed, or invalid record, or out-of-date
    // record of no synchronization need, etc.)
    int do_process_remote_sync_record(TairRemoteSyncType type, int32_t bucket_num,
                                      DataEntryWrapper* key_wrapper, data_entry* value,
                                      bool force_reget, bool retry, std::vector<FailRecord>& fail_records,
                                      std::string* cluster_info = NULL);
    int do_get_from_local_cluster(bool local_storage, int32_t bucket_num, data_entry* key, data_entry*& value);

  private:
    tair_manager* tair_manager_;

    // inited flag
    bool inited_;
    // whether retry when fail at first time
    bool retry_;

    // logger of records to be remote synchronized,
    // this logger is specific with storage engine.
    tair::common::RecordLogger* logger_;
    // logger of records to be retried after first synchronization,
    // use MemFileRecordLogger.
    tair::common::RecordLogger* retry_logger_;
    // logger of failed records after retring,
    // use FileRecordLogger.
    tair::common::RecordLogger* fail_logger_;

    // one local cluster handler
    ClusterHandler local_cluster_handler_;
    // multi remote cluster handler
    CLUSTER_HANDLER_MAP remote_cluster_handlers_;
  };

}

#endif
