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
#include "client/tair_client_api_impl.hpp"

#include "tair_manager.hpp"
#include "remote_sync_manager.hpp"

namespace tair
{
  RemoteSyncManager::RemoteSyncManager(tair_manager* tair_manager)
    : tair_manager_(tair_manager)
  {
    logger_ = tair_manager_->get_storage_manager()->get_remote_sync_logger();
    retry_logger_ = new tair::common::MemFileRecordLogger();
    fail_logger_ = new tair::common::FileRecordLogger();
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
  }

  int RemoteSyncManager::init()
  {
    int ret = TAIR_RETURN_SUCCESS;
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
    else if ((ret = init_sync_client()) != TAIR_RETURN_SUCCESS)
    {
      log_error("init sync client fail, ret: %d", ret);
    }
    else
    {
      // every reader do each remote synchronization.
      setThreadCount(remote_sync_thread_count() + retry_remote_sync_thread_count());
      log_info("remote sync manger start. sync thread count: %d, retry sync thread count: %d",
               remote_sync_thread_count(), retry_remote_sync_thread_count());
      start();
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
    data_entry* key = NULL;
    data_entry* value = NULL;

    while (!_stop)
    {
      // get one remote sync log record.
      ret = logger_->get_record(index, type, key, value);

      if (ret != TAIR_RETURN_SUCCESS || NULL == key)
      {
        log_error("get one record fail: %d", ret);
        continue;
      }

      // process record to do remote sync
      ret = do_process_remote_sync_record(static_cast<TairRemoteSyncType>(type), key, value);

      if (ret != TAIR_RETURN_SUCCESS)
      {
        log_debug("remote sync fail: %d, will retry", ret);
        // only use one retry log writer, only log key
        ret = retry_logger_->add_record(0, type, key, NULL);

        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("add to retry logger fail: %d, will log fail", ret);
          // only use one fail log writer
          ret = fail_logger_->add_record(0, type, key, NULL);
          // oops, what can I do with this record ...
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("add record to fail logger fail: %d", ret);
          }
        }
      }
      // TODO: .. cleanup
      log_debug("do one sync suc.");
    }

    return ret;
  }

  int RemoteSyncManager::do_retry_remote_sync(int32_t index)
  {
    int ret = TAIR_RETURN_SUCCESS;
    int32_t type = TAIR_REMOTE_SYNC_TYPE_NONE;
    data_entry* key = NULL;
    data_entry* value = NULL;

    static int32_t retry_times = 3;

    while (!_stop)
    {
      ret = retry_logger_->get_record(index, type, key, value);
      if (ret != TAIR_RETURN_SUCCESS || NULL == key)
      {
        log_error("get one retry record fail: %d", ret);
        continue;
      }

      // retry 'retry_times
      int32_t i = 0;
      do
      {
        // process record to do remote sync, force reget.
        ret = do_process_remote_sync_record(static_cast<TairRemoteSyncType>(type), key, value, true);
      } while (!_stop && ret != TAIR_RETURN_SUCCESS && ++i < retry_times);

      // still fail after retry, log failed records.
      if (ret != TAIR_RETURN_SUCCESS)
      {
        // only use one fail log writer
        ret = fail_logger_->add_record(0, type, key, NULL);
        // oops, what can I do with this record ...
        if (ret != TAIR_RETURN_SUCCESS)
        {
          log_error("add record to fail logger fail: %d", ret);
        }
      }
      // TODO: .. cleanup
      log_debug("do one retry suc.");
    }

    return ret;
  }

  int RemoteSyncManager::do_process_remote_sync_record(TairRemoteSyncType type, data_entry* key, data_entry* value,
                                                       bool force_reget)
  {
    int ret = TAIR_RETURN_SUCCESS;
    bool is_master = true, need_reget = true;

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
        need_reget = true;
        // need is_master to determines whether record can get from local_storage
        is_master = is_master_node(key);
      }
      else if (!(is_master = is_master_node(key))) // current node is not master one of this key
      {
        need_reget = true;
      }

      // re-get current data status in local cluster
      if (need_reget)
      {
        ret = do_get_from_local_cluster(is_master, key, value);
      }

      if (TAIR_RETURN_SUCCESS == ret || TAIR_RETURN_DATA_NOT_EXIST == ret)
      {
        // do remote synchronization based on type
        switch (type)
        {
        case TAIR_REMOTE_SYNC_TYPE_DELETE:
        {
          // only sync delete type record when data does NOT exist in local cluster.
          if (!need_reget || TAIR_RETURN_DATA_NOT_EXIST == ret)
          {
            ret = do_remote_sync_delete(key);
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
            ret = do_remote_sync_put(key, value);
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

  int RemoteSyncManager::do_get_from_local_cluster(bool local_storage, data_entry* key, data_entry*& value)
  {
    int ret = TAIR_RETURN_SUCCESS;
    if (local_storage)
    {
      value = new data_entry();
      // get from local server storage directly
      ret = tair_manager_->get_storage_manager()->get(tair_manager_->get_bucket_number(*key), *key, *value);
      if (ret != TAIR_RETURN_SUCCESS)
      {
        delete value;
        value = NULL;
      }
    }
    else
    {
      // key has merged area
      ret = local_client_->get(key->area, *key, value);
    }
    return ret;
  }

  int RemoteSyncManager::do_remote_sync_delete(data_entry* key)
  {
    int ret = TAIR_RETURN_SUCCESS;
    // aync
    return ret;
  }

  int RemoteSyncManager::do_remote_sync_put(data_entry* key, data_entry* value)
  {
    int ret = TAIR_RETURN_SUCCESS;
    // aync
    return ret;
  }

  int32_t RemoteSyncManager::remote_sync_thread_count()
  {
    // one thread service one remote sync log reader
    return logger_ != NULL ? logger_->get_reader_count() : 0;
  }

  int32_t RemoteSyncManager::retry_remote_sync_thread_count()
  {
    return retry_logger_ != NULL ? retry_logger_->get_reader_count() : 0;
  }

  bool RemoteSyncManager::is_master_node(data_entry* key)
  {
    // check..
    return true;
  }

  bool RemoteSyncManager::is_valid_record(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_NONE || type < TAIR_REMOTE_SYNC_TYPE_MAX);
  }

  bool RemoteSyncManager::is_remote_sync_need_value(TairRemoteSyncType type)
  {
    return (type > TAIR_REMOTE_SYNC_TYPE_WITH_VALUE_START);
  }

  int RemoteSyncManager::init_sync_client()
  {
    int ret = TAIR_RETURN_SUCCESS;

    std::string remote_sync_conf = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_REMOTE_SYNC_CONF, "");
    if (remote_sync_conf.empty())
    {
      log_error("no remote sycn config %s found", TAIR_REMOTE_SYNC_CONF);
      ret = TAIR_RETURN_FAILED;
    }
    else
    {
      log_info("remote sync conf: %s", remote_sync_conf.c_str());
      // config like this:

    }

    return ret;
  }

}
