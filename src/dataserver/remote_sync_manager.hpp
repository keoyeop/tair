/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * RemoteSyncManager:
 *   Manage synchronization between local and remote cluster.
 *   Data synchronization is based on commit log interface, so different
 *   user should implement its own commit logger writer/reader
 *   Now, logger can have only one writer and several readers.
 *
 * Version: $Id: remote_sync_manager.hpp 28 2012-07-30 10:18:09Z nayan@taobao.com $
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H
#define TAIR_DATASERVER_REMOTE_SYNC_MANAGER_H

#include <tbsys.h>

class tair::common::RecordLogger;

namespace tair
{
  class tair_client_impl;
  class tair_manager;

  class RemoteSyncManager : public tbsys::CDefaultRunnable
  {
  public:
    explicit RemoteSyncManager(tair_manager* tair_manager);
    ~RemoteSyncManager();

    int init();
    int add_record(TairRemoteSyncType type, data_entry* key, data_entry* value);
    void run(tbsys::CThread*, void* arg);

  private:
    int32_t remote_sync_thread_count();
    int32_t retry_remote_sync_thread_count();

    // check whether current node is the master one of key
    bool is_master_node(data_entry* key);
    // check whether this type record is valid
    bool is_valid_record(TairRemoteSyncType type);
    // check whether this type record need value to do remote synchronization
    bool is_remote_sync_need_value(TairRemoteSyncType type);

    int init_sync_client();

    int do_remote_sync(int32_t index);
    int do_retry_remote_sync(int32_t index);

    // Process one record.
    // Returning TAIR_RETURN_SUCCESS means this record is out of remote synchronization
    // purpose(maybe synchronization succeed, or invalid record, or out-of-date
    // record of no synchronization need, etc.)
    int do_process_remote_sync_record(TairRemoteSyncType type, data_entry* key, data_entry* value, bool force_reget = false);
    int do_get_from_local_cluster(bool local_storage, data_entry* key, data_entry*& value);
    int do_remote_sync_delete(data_entry* key);
    int do_remote_sync_put(data_entry* key, data_entry* value);
    // TODO: other ..

  private:
    tair_manager* tair_manager_;

    // logger of records to be remote synchronized,
    // this logger is specific with storage engine.
    tair::common::RecordLogger* logger_;
    // logger of records to be retried after first synchronization,
    // use MemFileRecordLogger.
    tair::common::RecordLogger* retry_logger_;
    // logger of failed records after retring,
    // use FileRecordLogger.
    tair::common::RecordLogger* fail_logger_;

    tair_client_impl* local_client_;
    tair_client_impl* remote_client_;
  };

}

#endif
