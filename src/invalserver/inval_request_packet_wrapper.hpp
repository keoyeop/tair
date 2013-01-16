/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * defination of the status of request, and parameter of the tair_client API's callback funcation.
 *
 * Version: $Id: inval_request_packet_wrapper.hpp 1173 2012-09-27 08:41:45Z fengmao $
 *
 * Authors:
 *   fengmao <fengmao.pj@taobao.com>
 *
 */
#ifndef TAIR_request_inval_PacketWrapper_H
#define TAIR_request_inval_PacketWrapper_H
#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "inval_group.hpp"
#include "inval_processor.hpp"
namespace tair {
  //the class request_hide_by_proxy, request_hides_by_proxy,
  //request_prefix_invalids are the subclass of request_invalid.
  typedef request_invalid request_inval_packet;

  //the transition diagram of request's status
  //(create wrapper, PREPARE_COMMIT) ==> (send request to ds, COMMITED_SUCCESS)
  //                                 ==> (send request to ds, COMMITED_FAILED) ==> (push retry_thread's queue, RETRY_COMMIT) ==>
  //(finally failed, CACHED_IN_STORAGE).
  //definition of the status of request
  enum
  {
    //init value of the request's status, that indicated the request was not processed by any group.
    PREPARE_COMMIT = 0,
    //request was processed success.
    COMMITTED_SUCCESS = 1,
    //request was processed failed
    COMMITTED_FAILED = 2,
    //after failed, retry to process the request
    RETRY_COMMIT = 3,
    //finally failed, cache the request to the memory, and write the request to the disk,
    //while the count of cached packet will be greater than some amount.
    CACHED_IN_STORAGE = 4
  };

  //reference counter, delay count and request status
  struct SharedInfo
  {
    //when the value of `reference_count was equ. 0, the return packet whould be send to client, except retry request.
    atomic_t reference_count;

    //the state of the request.
    atomic_t request_status;

    //record the retry times.
    atomic_t retry_times;

    //request packet.
    request_inval_packet *packet;

    SharedInfo(int init_reference_count, request_inval_packet *packet)
    {
      atomic_set(&reference_count, init_reference_count);
      atomic_set(&retry_times, 0);
      atomic_set(&request_status, PREPARE_COMMIT);
      this->packet = packet;
    }

    ~SharedInfo()
    {
      //packet, which was cached in the `request_storage, should not be released.
      if (atomic_read(&request_status) != CACHED_IN_STORAGE && packet != NULL)
      {
        delete packet;
        packet = NULL;
      }
    }

    inline void set_request_reference_count(int ref_count)
    {
      atomic_set(&reference_count, ref_count);
    }

    inline int dec_and_return_reference_count(int c)
    {
      return atomic_sub_return(c, &reference_count);
    }

    inline void set_request_status(int status)
    {
      atomic_set(&request_status, status);
    }

    inline int get_request_status()
    {
      return atomic_read(&request_status);
    }

    inline int get_retry_times()
    {
      return atomic_read(&retry_times);
    }

    inline void set_retry_times(int this_retry_times)
    {
      atomic_set(&retry_times, this_retry_times);
    }

    inline void inc_retry_times()
    {
      atomic_inc(&retry_times);
    }
  };

  class PacketWrapper {
  public:
    PacketWrapper(TairGroup *group, SharedInfo *shared)
    {
      this->group = group;
      this->shared = shared;
      is_need_return_packet = true;
    }

    virtual ~PacketWrapper()
    {
      if (dec_and_return_reference_count(1) == 0)
      {
        //finish the request, first of all, send return packet to client if necessary,
        //second, retry the request if failed.
        REQUEST_PROCESSOR.end_request(this);
        //release the shared.
        delete shared;
      }
    }

    inline int get_request_reference_count()
    {
      return atomic_read(&(shared->reference_count));
    }

    inline void set_request_reference_count(int this_reference_count)
    {
      atomic_set(&(shared->reference_count), this_reference_count);
    }

    inline int dec_and_return_reference_count(int c)
    {
      return shared->dec_and_return_reference_count(c);
    }

    inline void inc_request_reference_count()
    {
      atomic_inc(&(shared->reference_count));
    }

    inline void dec_request_reference_count()
    {
      atomic_dec(&(shared->reference_count));
    }

    inline void set_request_status(int rs)
    {
      atomic_set(&(shared->request_status), rs);
    }

    inline int get_request_status()
    {
      return atomic_read(&(shared->request_status));
    }

    inline int get_needed_return_packet()
    {
      return is_need_return_packet;
    }

    inline void set_needed_return_packet(bool is_need)
    {
      is_need_return_packet = is_need;
    }

    inline TairGroup *get_group()
    {
      return group;
    }

    inline tair_client_impl *get_tair_client()
    {
      return group != NULL ? group->get_tair_client() : NULL;
    }

    inline request_inval_packet *get_packet()
    {
      return shared->packet;
    }

    inline SharedInfo *get_shared_info()
    {
      return shared;
    }
  protected:
    bool is_need_return_packet;

    SharedInfo *shared;

    TairGroup *group;
  };

  //with single key
  class SingleWrapper : public PacketWrapper
  {
  public:
    SingleWrapper(TairGroup *group, SharedInfo *shared, data_entry *key)
      : PacketWrapper(group, shared)
    {
      this->key = key;
    }

    ~SingleWrapper()
    {
    }

    inline data_entry* get_key()
    {
      return key;
    }
  protected:
    data_entry *key;
  };

  //with multi-keys
  class MultiWrapper : public PacketWrapper
  {
  public:
    MultiWrapper(TairGroup *group, SharedInfo *shared, tair_dataentry_set *keys)
      : PacketWrapper(group, shared)
    {
      this->keys = keys;
    }

    ~MultiWrapper()
    {
      if (keys != NULL)
      {
        delete keys;
        keys = NULL;
      }
    }

    inline tair_dataentry_set *get_keys()
    {
      return keys;
    }
  protected:
    tair_dataentry_set *keys;
  };
}
#endif
