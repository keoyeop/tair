/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet code and base packet are defined here
 *
 * Version: $Id: inval_request_packet_wrapper.hpp 1173 2012-09-27 08:41:45Z fengmao $
 *
 * Authors:
 *   fengmao <fengmao.pj@taobao.com>
 *
 */
#ifndef TAIR_request_inval_packet_wrapper_H
#define TAIR_request_inval_packet_wrapper_H
#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"
namespace tair {
  //the class request_hide_by_proxy, request_hides_by_proxy,
  //request_prefix_invalids are the subclass of request_invalid.
  typedef request_invalid request_inval_packet;
  class InvalRetryThread;
  class inval_request_storage;
  class request_inval_packet_wrapper
  {
  public:
    request_inval_packet_wrapper(request_inval_packet *packet, InvalRetryThread *retry_thread,
        inval_request_storage *request_storage)
    {
      this->packet = packet;
      this->retry = retry_thread;
      this->request_storage = request_storage;
      retry_times = 0;
      set_reference_count(0);
      reset_failed_flag();
    }
    ~request_inval_packet_wrapper()
    {
      //do not free the packet, which is deleted by request_processor.
    }

    //FAILED: failed to send request packet to DS.
    //FAILED_RETRY_QUEUE: the request packet, from the client, is in the retry_threads's queue.
    //FAILED_STORAGE_QUEUE: the request packet, from the client, is in the request_storage's queue.
    //SUCCESS: all operation needed were successfully executed.
    enum {FAILED = 0, FAILED_RETRY_QUEUE = 1, FAILED_STORAGE_QUEUE = 2, SUCCESS = 3};

    inline int get_reference_count()
    {
      return atomic_read(&reference_count);
    }

    inline void set_reference_count(int this_reference_count)
    {
      atomic_set(&reference_count, this_reference_count);
    }

    inline void dec_reference_count()
    {
      atomic_dec(&reference_count);
    }

    inline int dec_and_return_reference_count(int c)
    {
      return atomic_sub_return(c, &reference_count);
    }

    inline void set_failed_flag(int flag)
    {
      atomic_set(&is_failed, flag);
    }

    inline int get_failed_flag()
    {
      return atomic_read(&is_failed);
    }

    inline void reset_failed_flag()
    {
      atomic_set(&is_failed, SUCCESS);
    }

    inline uint16_t get_retry_times()
    {
      return retry_times;
    }

    inline void inc_retry_times()
    {
      retry_times ++;
    }


    inline void release_packet()
    {
      if (packet != NULL) {
        delete packet;
        packet = NULL;
      }
    }

    //if /remove/hide/removes/hides operation was failed, the request packet
    //should be pushed into queue of `retry_thread.
    InvalRetryThread *retry;

    //if /remove/hide/removes/hides operation was failed finally, the request packet
    //should be pushed into the queue of `request_storage.
    inval_request_storage *request_storage;

    request_inval_packet* packet;
  protected:
    atomic_t reference_count;
    atomic_t is_failed;
    uint16_t retry_times;
  };

  class request_inval_packet_ex_wrapper : public request_inval_packet_wrapper
  {
  public:
    request_inval_packet_ex_wrapper(request_inval_packet *packet, InvalRetryThread *retry_thread,
        inval_request_storage *request_storage) :request_inval_packet_wrapper(packet, retry_thread, request_storage),
    failed_key_set(NULL)
    {
    }

    ~request_inval_packet_ex_wrapper()
    {
      if (failed_key_set != NULL) {
        delete failed_key_set;
        failed_key_set = NULL;
      }
    }
  public:
    tair_dataentry_set *failed_key_set;
    data_entry pkey;
  };

}

#endif
