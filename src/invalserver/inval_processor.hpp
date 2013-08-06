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
namespace tair {
#define REQUEST_PROCESSOR tair::RequestProcessor::request_processor_instance
  class InvalRetryThread;
  class InvalRequestStorage;
  class SharedInfo;
  class PacketWrapper;
  class SingleWrapper;
  class MultiWrapper;
  class InvalHeartbeatThread;
  class RequestProcessor {
  public:
    RequestProcessor();

    void set_thread_parameter(InvalRetryThread *retry_thread,
        InvalRequestStorage *request_storage,
        InvalHeartbeatThread *heartbeat_thread);

    void set_dumpkey_switch(bool on_or_off);
  public:
    static RequestProcessor request_processor_instance;
  public:
    //process the request
    void process(PacketWrapper *wrapper);

    //call back
    void process_callback(int rcode, PacketWrapper *wrapper);

    //finish the request
    void end_request(PacketWrapper *wrapper);

    //process the failed request
    void process_failed_request(PacketWrapper *wrapper);

    //send return packet to client
    void send_return_packet(PacketWrapper *wrapper, const int ret, const char *msg);

    //callback function
    void static client_callback_with_single_key(int rcode, void *args);
    void static client_callback_with_multi_keys(int rcode, const key_code_map_t *key_code_map, void *args);
  protected:

    //defination of function type.
    typedef int (tair_client_impl::*PROCESS_RH_FUNC_T) (int area, const data_entry &key, TAIRCALLBACKFUNC pfunc, void *parg);

    //process invalid/hide operation
    void do_process(PROCESS_RH_FUNC_T pproc, SingleWrapper *wrapper);

    //defination of function type.
    typedef int (tair_client_impl::*PROCESS_RHS_FUNC_T) (int area, const tair_dataentry_set &key_set,
        key_code_map_t *key_code_map,
        TAIRCALLBACKFUNC_EX pfunc, void *parg);

    //process prefix_invalids/prefix_hides operation
    void do_process(PROCESS_RHS_FUNC_T pproc, MultiWrapper *wrapper);
    void do_process_request(PROCESS_RH_FUNC_T pproc, PacketWrapper *wrapper);
    void do_process_request(PROCESS_RHS_FUNC_T pproc, PacketWrapper *wrapper);
    //obtain ds address with a key
    std::string obtain_ds_addr(PacketWrapper *wrapper);
    //dump key
    void dump_key(const std::string &tag, SharedInfo *shared);
    void do_dump_key(data_entry *key, const char *msg);
  private:
    static tair_packet_factory packet_factory;

    //mapping packet's pcode to the operation's name, such as INVALID, PREFIX_INVALID, HIDE, PREFIX_HIDE.
    static std::map<int, int> pcode_opname_map;

    InvalRetryThread *retry_thread;
    InvalRequestStorage *request_storage;
    InvalHeartbeatThread *heartbeat_thread;

    //just used as the parameter, not to insert any data.
    key_code_map_t failed_key_code_map;

    static const int DATA_EXPIRED = -3988;
    //dump key, when `dump_key_switch's value equal to true, otherwise do not dump it.
    bool dump_key_switch;
  };
}
#endif
