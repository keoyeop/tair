/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: inval_processor.hpp.hpp 1173 2012-09-27 08:41:45Z fengmao.pj@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef INVAL_PROCESSOR_HPP
#define INVAL_PROCESSOR_HPP
#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include <map>
#include <utility>
#include "inval_request_packet_wrapper.hpp"

namespace tair {
  class request_inval_packet_wrapper;
  class RequestProcessor {
  public:
    RequestProcessor(InvalLoader *invalid_loader);

    //callback function
    static void client_callback_func(int rcode, void *args);
    //callbcak function for operation with multi-keys
    static void client_callback_func(int rcode, const key_code_map_t *key_code_map, void *args);
  public:
    //invalid
    inline int process(request_invalid *req, request_inval_packet_wrapper *wrapper)
    {
      log_debug("invalid server process, invalid, request packet's pcode: %d", req->getPCode());
      return do_process(req, &tair_client_impl::remove, wrapper);
    }

    //hide
    inline int process(request_hide_by_proxy *req, request_inval_packet_wrapper *wrapper)
    {
      log_debug("invalid server process, hide, request packet's pcode: %d", req->getPCode());
      return do_process(req, &tair_client_impl::hide, wrapper);
    }

    //prefix_invalid
    inline int process(request_prefix_invalids *req, request_inval_packet_ex_wrapper *wrapper)
    {
      log_debug("invalid server process, prefix_invalid, request packet's pcode: %d", req->getPCode());
      return do_process(req, &tair_client_impl::removes, wrapper);
    }

    //prefix_hide
    inline int process(request_prefix_hides_by_proxy *req, request_inval_packet_ex_wrapper *wrapper)
    {
      log_debug("invalid server process, prefix_hide, request packet's pcode: %d", req->getPCode());
      return do_process(req, &tair_client_impl::hides, wrapper);
    }


  protected:
    //if operation(such as hide, remove, hides and removes) failed, the request
    //packet should be pushed into some queue.
    static void do_rescue_failure(request_inval_packet_wrapper* wrapper);

    //finish the work
    static void do_process_ultimate(request_inval_packet_wrapper *wrapper, const int ret);
    //process failed keys
    static void do_process_failed_keys(request_inval_packet_wrapper *wrapper, const tair_dataentry_set &keys);

    //defination of function type.
    typedef int (tair_client_impl::*PROCESS_RH_FUNC_T) (int area, const data_entry &key, TAIRCALLBACKFUNC pfunc, void *parg);
    //process invalid/hide operation
    int do_process(request_invalid *req, PROCESS_RH_FUNC_T pproc, request_inval_packet_wrapper *wrapper);

    //defination of function type.
    typedef int (tair_client_impl::*PROCESS_RHS_FUNC_T) (int area, const tair_dataentry_set &key_set,
        key_code_map_t *key_code_map,
        TAIRCALLBACKFUNC_EX pfunc, void *parg);
    //process prefix_invalid/prefix_hide operation
    int do_process(request_invalid *req, PROCESS_RHS_FUNC_T pproc, request_inval_packet_ex_wrapper *wrapper);

  private:
    //obtain the instances of client according to the group name.
    InvalLoader *invalid_loader;

    static tair_packet_factory packet_factory;

    //mapping packet's pcode to the operation's name, such as INVALID, PREFIX_INVALID, HIDE, PREFIX_HIDE.
    static std::map<int, int> pcode_opname_map;
  };
}
#endif
