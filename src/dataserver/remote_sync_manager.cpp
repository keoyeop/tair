/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/data_entry.hpp"
#include "common/record_logger.hpp"
#include "common/record_logger_util.hpp"

#include "tair_manager.hpp"
#include "remote_sync_manager.hpp"

namespace tair
{
  RemoteSyncManager::RemoteSyncManager(tair_manager* tair_manager)
    : tair_manager_(tair_manager), inited_(false), retry_(false), logger_(NULL), retry_logger_(NULL), fail_logger_(NULL)
  {
  }

  RemoteSyncManager::~RemoteSyncManager()
  {
    stop();
    wait();

    if (retry_logger_ != NULL)
    {
      delete retry_logger_;
    }
    if (fail_logger_ != NULL)
    {
      delete fail_logger_;
    }

    for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
         it != remote_cluster_handlers_.end(); ++it)
    {
      delete it->second;
    }
  }

  int RemoteSyncManager::init()
  {
    int ret = TAIR_RETURN_SUCCESS;
    std::string rsync_dir = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_RSYNC_DATA_DIR, "./data/rsync");
    std::string tmp_dir = rsync_dir;
    if (!tbsys::CFileUtil::mkdirs(const_cast<char*>(tmp_dir.c_str())))
    {
      log_error("make rsync data directory fail: %s", rsync_dir.c_str());
      ret = TAIR_RETURN_FAILED;      
    }
    else
    {
      if (retry_logger_ != NULL)
      {
        delete retry_logger_;
      }
      if (fail_logger_ != NULL)
      {
        delete fail_logger_;
      }

      logger_ = tair_manager_->get_storage_manager()->get_remote_sync_logger();
      retry_logger_ =
        new RingBufferRecordLogger((rsync_dir + "/retry_log").c_str(),
                                   TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,
                                                       TAIR_RSYNC_RETRY_LOG_MEM_SIZE, 100 << 20));
      fail_logger_ =
        new SequentialFileRecordLogger((rsync_dir + "/fail_log").c_str(),
                                       TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,
                                                           TAIR_RSYNC_FAIL_LOG_SIZE, 30 << 20), true/*rotate*/);

      retry_ = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_RSYNC_DO_RETRY, 0) > 0;

      if (NULL == logger_)
      {
        log_error("current storage engine has NO its own remote sync logger");
        ret = TAIR_RETURN_FAILED;
      }
      else if ((ret = logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init remote sync logger fail: %d", ret);
      }
      else if ((ret = retry_logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init retry logger fail: %d", ret);
      }
      else if ((ret = fail_logger_->init()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init fail logger fail: %d", ret);
      }
      else if ((ret = init_sync_conf()) != TAIR_RETURN_SUCCESS)
      {
        log_error("init remote sync config fail: %d", ret);
      }
      else
      {
        // every reader do each remote synchronization.
        setThreadCount(remote_sync_thread_count() + retry_remote_sync_thread_count());
        log_info("remote sync manger start. sync thread count: %d, retry sync thread count: %d, retry: %s",
                 remote_sync_thread_count(), retry_remote_sync_thread_count(), retry_ ? "true" : "false");
        start();
      }
    }
    return ret;
  }

  int RemoteSyncManager::add_record(TairRemoteSyncType type, data_entry* key, data_entry* value)
  {
    // now we only use one writer.
    return logger_->add_record(0, type, key, value);
  }

  void RemoteSyncManager::run(tbsys::CThread*, void* arg)
  {
    int32_t index = static_cast<int32_t>(reinterpret_cast<int64_t>(arg));

    if (index < remote_sync_thread_count())
    {
      log_warn("start do remote sync");
      do_remote_sync(index);
    }
    else if (index < remote_sync_thread_count() + retry_remote_sync_thread_count())
    {
      log_warn("start do retry remote sync");
      // index in retry_logger_
      do_retry_remote_sync(index - remote_sync_thread_count());
    }
    else
    {
      log_error("invalid thread index: %d", index);
    }

    log_warn("remote sync manager run over");
  }

  int RemoteSyncManager::do_remote_sync(int32_t index)
  {
    int ret = TAIR_RETURN_SUCCESS;
    int32_t type = TAIR_REMOTE_SYNC_TYPE_NONE;
    int32_t bucket_num = -1;
    data_entry* key = NULL;
    data_entry* value = NULL;
    bool force_reget = false;
    bool need_wait = false;
    std::vector<FailRecord> fail_records;

    while (!_stop)
    {
      if (need_wait)
      {
        sleep(1);
        need_wait = false;
      }

      // init client until success
      if ((ret = init_sync_client()) != TAIR_RETURN_SUCCESS)
      {
        log_warn("client init fail, ret: %d", ret);
        need_wait = true;
        continue;
      }

      // get one remote sync log record.
      ret = logger_->get_record(index, type, bucket_num, key, value, force_reget);

      // maybe storage not init yet, wait..
      if (TAIR_RETURN_SERVER_CAN_NOT_WORK == ret)
      {
        need_wait = true;
        continue;
      }

      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_error("get one record fail: %d", ret);
        continue;
      }

      // return success but key == NULL indicates no new record now..
      if (NULL == key)
      {
        log_debug("@@ no new record");
        need_wait = true;
        continue;
      }

      // has refed 1
      DataEntryWrapper* key_wrapper = new DataEntryWrapper(key);
      log_debug("@@ type: %d, %d, fg: %d, key: %s %d", type, bucket_num, force_reget, key->get_data() + 4, key->get_size());
      // process record to do remote sync
      ret = do_process_remote_sync_record(static_cast<TairRemoteSyncType>(type), bucket_num,
                                          key_wrapper, value, force_reget, retry_, fail_records);

      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_debug("remote sync fail: %d, will retry", ret);

        for (size_t i = 0; i < fail_records.size(); ++i)
        {
          if (retry_)
          {
            ret = log_fail_record(retry_logger_, static_cast<TairRemoteSyncType>(type), fail_records[i]);
          }

          // not retry or log retry logger fail
          if (!retry_ || ret != TAIR_RETURN_SUCCESS)
          {
            ret = log_fail_record(fail_logger_, static_cast<TairRemoteSyncType>(type), fail_records[i]);
            if (ret != TAIR_RETURN_SUCCESS)
            {
              log_error("add record to fail logger fail: %d", ret);
            }
          }
        }
      }

      // cleanup
      key_wrapper->unref();
      key = NULL;
      if (value != NULL)
      {
        delete value;
        value = NULL;
      }
      log_debug("do one sync suc.");
    }

    return ret;
  }

  int RemoteSyncManager::do_retry_remote_sync(int32_t index)
  {
    int ret = TAIR_RETURN_SUCCESS;
    int32_t type = TAIR_REMOTE_SYNC_TYPE_NONE;
    data_entry* logger_key = NULL;
    data_entry* key = NULL;
    data_entry* value = NULL;


    static int32_t retry_times = 1;

    bool force_reget = false;
    bool need_wait = false;
    int32_t bucket_num = -1;
    std::vector<FailRecord> fail_records;

    while (!_stop)
    {
      if (need_wait)
      {
        sleep(1);
        log_debug("@@ wait retry get");
      }

      // init client until success
      if ((ret = init_sync_client()) != TAIR_RETURN_SUCCESS)
      {
        log_warn("client inited fail, ret: %d", ret);
        need_wait = true;
        continue;
      }

      ret = retry_logger_->get_record(index, type, bucket_num, logger_key, value, force_reget);
      if (NULL == logger_key)
      {
        // return success means no more record
        if (TAIR_RETURN_SUCCESS == ret)
        {
          need_wait = true;
        }
        else
        {
          log_error("get one retry record fail: %d", ret);
        }
        continue;
      }

      FailRecord record(logger_key->get_data(), logger_key->get_size());
      key = record.key_;
      // we got real failed key
      delete logger_key;
      logger_key = NULL;

      log_debug("@@ type: %d, %d, fg: %d, key: %s %d", type, bucket_num, force_reget, key->get_data() + 4, key->get_size());
      // retry 'retry_times
      int32_t i = 0;
      DataEntryWrapper* key_wrapper = new DataEntryWrapper(key);
      do
      {
        // process record to do remote sync, force reget.
        ret = do_process_remote_sync_record(static_cast<TairRemoteSyncType>(type), bucket_num,
                                            key_wrapper, value, true/* force reget */, false/* not retry */,
                                            fail_records, &record.cluster_info_);
      } while (!_stop && ret != TAIR_RETURN_SUCCESS && ++i < retry_times);

      // still fail after retry, log failed records.
      if (ret != TAIR_RETURN_SUCCESS)
      {
        for (size_t i = 0; i < fail_records.size(); ++i)
        {
          ret = log_fail_record(fail_logger_, static_cast<TairRemoteSyncType>(type), fail_records[i]);
          // oops, what can I do with this record ...
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("add record to fail logger fail: %d", ret);
          }
        }
      }

      // cleanup
      key_wrapper->unref();
      key = NULL;
      if (value != NULL)
      {
        delete value;
        value = NULL;
      }
      log_debug("do one retry suc.");
      need_wait = false;
    }

    return ret;
  }

#ifdef DO_REMOTE_SYNC_OP
#undef DO_REMOTE_SYNC_OP
#endif
// brown sugar...
// how come you look so dummy...
#define DO_REMOTE_SYNC_OP(type, op_func)                                \
  do {                                                                  \
    fail_records.clear();                                               \
    RecordLogger* callback_logger = retry ? retry_logger_ : fail_logger_; \
    if (cluster_info != NULL)                                           \
    {                                                                   \
      CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.find(*cluster_info); \
      if (it != remote_cluster_handlers_.end())                         \
      {                                                                 \
        ClusterHandler* handler = it->second;                           \
        RemoteSyncManager::CallbackArg* arg =                           \
          new RemoteSyncManager::CallbackArg(callback_logger, type, key_wrapper, handler); \
        ret = handler->client()->op_func;                               \
        if (ret != TAIR_RETURN_SUCCESS)                                 \
        {                                                               \
          delete arg;                                                   \
          fail_records.push_back(FailRecord(key, handler->info(), ret)); \
        }                                                               \
      }                                                                 \
      else                                                              \
      {                                                                 \
        log_error("invalid cluster info");                              \
      }                                                                 \
    }                                                                   \
    else                                                                \
    {                                                                   \
      for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin(); \
           it != remote_cluster_handlers_.end(); ++it)                  \
      {                                                                 \
        ClusterHandler* handler = it->second;                            \
        RemoteSyncManager::CallbackArg* arg =                           \
          new RemoteSyncManager::CallbackArg(callback_logger, type, key_wrapper, handler); \
        ret = handler->client()->op_func;                               \
        if (ret != TAIR_RETURN_SUCCESS)                                 \
        {                                                               \
          delete arg;                                                   \
          fail_records.push_back(FailRecord(key, handler->info(), ret)); \
        }                                                               \
      }                                                                 \
    }                                                                   \
    ret = fail_records.empty() ? TAIR_RETURN_SUCCESS : TAIR_RETURN_FAILED; \
  } while (0)

  int RemoteSyncManager::do_process_remote_sync_record(TairRemoteSyncType type, int32_t bucket_num,
                                                       DataEntryWrapper* key_wrapper, data_entry* value,
                                                       bool force_reget, bool retry, std::vector<FailRecord>& fail_records,
                                                       std::string* cluster_info)
  {
    int ret = TAIR_RETURN_SUCCESS;
    bool is_master = false, need_reget = false;
    data_entry *key = key_wrapper->entry();

    if (!is_valid_record(type))
    {
      log_warn("invalid record type: %d, ignore", type);
      // treat as success
    }
    else
    {
      // force reget or this record need synchronize value but value is null
      if (force_reget || (NULL == value && is_remote_sync_need_value(type)))
      {
        log_debug("@@ fr 1 %d %x %d", force_reget, value, is_remote_sync_need_value(type));
        need_reget = true;
        // need is_master to determines whether record can get from local_storage
        is_master = tair_manager_->is_master_node(bucket_num, *key);
      }
      else if (!(is_master = tair_manager_->is_master_node(bucket_num, *key)))
      {
        // current node is not master one of this key
        log_debug("@@ fr 2");
        need_reget = true;
      }

      // re-get current data status in local cluster
      if (need_reget)
      {
        log_debug("@@ get local %d", is_master);
        ret = do_get_from_local_cluster(is_master, bucket_num, key, value);
      }

      if (TAIR_RETURN_SUCCESS == ret || TAIR_RETURN_DATA_NOT_EXIST == ret)
      {
        // rsync server flag
        key->server_flag = TAIR_SERVERFLAG_RSYNC;

        log_debug("@@ value %s %d", value != NULL ? value->get_data() : "n", value != NULL ? value->get_size(): 0);
        // do remote synchronization based on type
        switch (type)
        {
        case TAIR_REMOTE_SYNC_TYPE_DELETE:
        {
          // only sync delete type record when data does NOT exist in local cluster.
          if (!need_reget || TAIR_RETURN_DATA_NOT_EXIST == ret)
          {
            DO_REMOTE_SYNC_OP(TAIR_REMOTE_SYNC_TYPE_DELETE, remove(key->area, *key, &RemoteSyncManager::callback, arg));
          }
          else
          {
            log_debug("sync DELETE but data exist in local cluster");
            ret = TAIR_RETURN_SUCCESS;
          }
          break;
        }
        case TAIR_REMOTE_SYNC_TYPE_PUT:
        {
          // only sync put type record when data really exists in local cluster
          if (TAIR_RETURN_SUCCESS == ret)
          {
            DO_REMOTE_SYNC_OP(TAIR_REMOTE_SYNC_TYPE_PUT, put(key->area, *key, *value, 0, 0, false,
                                                             &RemoteSyncManager::callback, arg));
          }
          else
          {
            log_debug("sync PUT but data not exist in local cluster");
            ret = TAIR_RETURN_SUCCESS;
          }
          break;
        }
        default:
        {
          // after is_valid_record(), no invalid record type, ignore here.
          log_debug("ignore record type: %d", type);
          ret = TAIR_RETURN_SUCCESS;
        }
        }
      }
    }

    return ret;
  }

  int RemoteSyncManager::do_get_from_local_cluster(bool local_storage, int32_t bucket_num,
                                                   data_entry* key, data_entry*& value)
  {
    int ret = TAIR_RETURN_SUCCESS;
    if (local_storage)
    {
      value = new data_entry();
      // get from local server storage directly
      ret = tair_manager_->get_storage_manager()->get(bucket_num, *key, *value);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        delete value;
        value = NULL;
      }
    }
    else
    {
      // key has merged area
      ret = local_cluster_handler_.client()->get(key->area, *key, value);
    }
    return ret;
  }

  int32_t RemoteSyncManager::remote_sync_thread_count()
  {
    // one thread service one remote sync log reader
    return logger_ != NULL ? logger_->get_reader_count() : 0;
  }

  int32_t RemoteSyncManager::retry_remote_sync_thread_count()
  {
    return (retry_ && retry_logger_ != NULL) ? retry_logger_->get_reader_count() : 0;
  }

  bool RemoteSyncManager::is_valid_record(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_NONE || type < TAIR_REMOTE_SYNC_TYPE_MAX);
  }

  bool RemoteSyncManager::is_remote_sync_need_value(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_WITH_VALUE_START);
  }

 // much resemble json format.
 // one local cluster config and one or multi remote cluster config.
 // {local:[master_cs_addr,slave_cs_addr,group_name,timeout_ms],remote:[...],remote:[...]}
  int RemoteSyncManager::init_sync_conf()
  {
    static const char* LOCAL_CLUSTER_CONF_KEY = "local";
    static const char* REMOTE_CLUSTER_CONF_KEY = "remote";

    int ret = TAIR_RETURN_SUCCESS;
    std::string conf_str = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_REMOTE_SYNC_CONF, "");
    std::vector<std::string> clusters;
    tair::common::string_util::split_str(conf_str.c_str(), "]}", clusters);

    if (clusters.size() < 2)
    {
      ret = TAIR_RETURN_FAILED;
      log_error("invalid clusters count in conf");
    }
    else
    {
      for (size_t i = 0; i < clusters.size(); ++i)
      {
        ret = TAIR_RETURN_FAILED;
        std::vector<std::string> one_conf;
        tair::common::string_util::split_str(clusters[i].c_str(), "{[", one_conf);

        if (one_conf.size() != 2)
        {
          log_error("invalid one cluster key/value conf format: %s", clusters[i].c_str());
          break;
        }

        std::string& conf_key = one_conf[0];
        std::string& conf_value = one_conf[1];
        std::vector<std::string> client_conf;
        tair::common::string_util::split_str(conf_key.c_str(), ",: ", client_conf);
        if (client_conf.size() != 1)
        {
          log_error("invalid cluster key: %s", conf_key.c_str());
          break;
        }
        conf_key = client_conf[0];
        client_conf.clear();
        tair::common::string_util::split_str(conf_value.c_str(), ", ", client_conf);

        if (conf_key == LOCAL_CLUSTER_CONF_KEY)
        {
          if (local_cluster_handler_.conf_inited())
          {
            log_error("multi local cluster config.");
            break;
          }
          if (local_cluster_handler_.init_conf(client_conf) != TAIR_RETURN_SUCCESS)
          {
            log_error("invalid local cluster conf value: %s", conf_value.c_str());
            break;
          }
        }
        else if (conf_key == REMOTE_CLUSTER_CONF_KEY)
        {
          ClusterHandler* handler = new ClusterHandler();
          if (handler->init_conf(client_conf) != TAIR_RETURN_SUCCESS)
          {
            log_error("invalid remote cluster conf value: %s", conf_value.c_str());
            delete handler;
            break;
          }
          else
          {
            remote_cluster_handlers_[handler->info()] = handler;
          }
        }
        else
        {
          log_error("invalid cluster conf type: %s", conf_key.c_str());
          break;
        }
        ret = TAIR_RETURN_SUCCESS;
      }
    }

    if (ret != TAIR_RETURN_SUCCESS)
    {
      log_error("invalid remote sync conf: %s", conf_str.c_str());
    }
    else if (!local_cluster_handler_.conf_inited())
    {
      log_error("local cluster not inited");
      ret = TAIR_RETURN_FAILED;
    }
    else if (remote_cluster_handlers_.empty())
    {
      log_error("remote cluster not config");
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      std::string debug_str = std::string(LOCAL_CLUSTER_CONF_KEY);
      debug_str.append(":");
      debug_str.append(local_cluster_handler_.debug_string());
      for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
           it != remote_cluster_handlers_.end(); ++it)
      {
        debug_str.append(REMOTE_CLUSTER_CONF_KEY);
        debug_str.append(":");
        debug_str.append(it->second->debug_string());
      }
      log_warn("init remote sync conf: %s", debug_str.c_str());
    }

    return ret;
  }

  // init_sync_conf() should return success
  int RemoteSyncManager::init_sync_client()
  {
    int ret = TAIR_RETURN_SUCCESS;

    if (!inited_)
    {
      if ((ret = local_cluster_handler_.start()) != TAIR_RETURN_SUCCESS)
      {
        log_error("start local cluster fail: %d, %s", ret, local_cluster_handler_.debug_string().c_str());
      }
      else
      {
        for (CLUSTER_HANDLER_MAP::iterator it = remote_cluster_handlers_.begin();
             it != remote_cluster_handlers_.end(); ++it)
        {
          if ((ret = it->second->start()) != TAIR_RETURN_SUCCESS)
          {
            log_error("start remote cluster fail: %d, %s", ret, it->second->debug_string().c_str());
            break;
          }
        }

        if (ret == TAIR_RETURN_SUCCESS)
        {
          log_warn("start all cluster client.");
          inited_ = true;
        }
      }
    }

    return ret;
  }

  int RemoteSyncManager::log_fail_record(RecordLogger* logger, TairRemoteSyncType type, FailRecord& record)
  {
    data_entry entry(record.encode(), record.encode_size(), false);
    // only one writer here
    return logger->add_record(0, type, &entry, NULL);
  }

  void RemoteSyncManager::callback(int ret, void* arg)
  {
    RemoteSyncManager::CallbackArg* callback_arg = (RemoteSyncManager::CallbackArg*)arg;

    bool failed = false;
    switch (callback_arg->type_)
    {
    case TAIR_REMOTE_SYNC_TYPE_PUT:
      failed = (ret != TAIR_RETURN_SUCCESS);
      break;
    case TAIR_REMOTE_SYNC_TYPE_DELETE:
      // not exist or expired also OK
      failed = (ret != TAIR_RETURN_SUCCESS && ret != TAIR_RETURN_DATA_NOT_EXIST && ret != TAIR_RETURN_DATA_EXPIRED);
      break;
    default:
      log_error("unknown callback type: %d, ret: %d", callback_arg->type_, ret);
      break;
    }

    if (failed)
    {
      FailRecord record(callback_arg->key_->entry(), callback_arg->handler_->info(), ret);
      ret = RemoteSyncManager::log_fail_record(callback_arg->fail_logger_, callback_arg->type_, record);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        // this is all I can do...
        log_error("log fail record fail: %d", ret);
      }
    }

    log_debug("@@ cb %d", ret);
    delete callback_arg;
  }

}
