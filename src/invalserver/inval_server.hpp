#ifndef SERVER_PAIR_ID_H
#define SERVER_PAIR_ID_H

#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <string>
#include <ext/hash_map>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "base_packet.hpp"
#include "packet_factory.hpp"
#include "packet_streamer.hpp"
#include "invalid_packet.hpp"
#include "ping_packet.hpp"
#include "util.hpp"
#include "inval_loader.hpp"
#include "inval_retry_thread.hpp"
#include "inval_processor.hpp"
#include "retry_all_packet.hpp"
#include "inval_stat_packet.hpp"
#include "inval_request_storage.hpp"
#include "op_cmd_packet.hpp"
#include "inval_request_packet_wrapper.hpp"
namespace tair {
  class InvalServer: public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler {
  public:
    InvalServer();
    ~InvalServer();

    void start();
    void stop();

    tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
    bool handlePacketQueue(tbnet::Packet *packet, void *args);
    bool push_task(tbnet::Packet *packet);
    int task_queue_size();
  private:
    void do_request(request_inval_packet *req, int factor, int request_type, bool merged);
    inline void do_invalid(request_invalid *req)
    {
      //the default value of `request_reference_count is the product of the group's count and `key_count,
      //while the operation is any of {invalid, hide}. in this case, the factor's value is equal to `key_count.
      do_request(req, /*factor = */ req->key_count, InvalStatHelper::INVALID, false);
    }

    inline void do_hide(request_hide_by_proxy *req)
    {
      do_request(req, req->key_count, InvalStatHelper::HIDE, false);
    }

    inline void do_prefix_hides(request_prefix_hides_by_proxy *req)
    {
      //the default value of `request_reference_count is the count of groups,
      //while the operation is any of prefix_{invalid, hide}. in this case, the factor's value is aways equal to 1.
      do_request(req, /*factor = */ 1, InvalStatHelper::PREFIX_HIDE, true);
    }

    inline void do_prefix_invalids(request_prefix_invalids *req)
    {
      do_request(req, 1, InvalStatHelper::PREFIX_INVALID, true);
    }

    int do_request_stat(request_inval_stat *req, response_inval_stat *resp);
    int do_retry_all(request_retry_all* req);
    bool init();
    bool destroy();

    //debug support
    int do_debug_support(request_op_cmd *rq);
    int parse_params(const std::vector<std::string>& params,
        std::string& group_name, int32_t& area, int32_t& add_request_storage);
    void construct_debug_infos(std::vector<std::string>& infos);
  private:
    void process_unknown_groupname_request(tbnet::Packet *packet);
    bool _stop;
    bool ignore_zero_area;

    tair_packet_factory packet_factory;
    tair_packet_streamer streamer;
    tbnet::Transport transport;

    tbnet::PacketQueueThread task_queue_thread;
    InvalLoader invalid_loader;
    RequestProcessor processor;
    InvalRetryThread retry_thread;

    //local storage for request packet.
    InvalRequestStorage request_storage;
    //for debug info
    int sync_task_thread_count;
  };

  class RetryWorkThread : public tbsys::CDefaultRunnable {
  public:
    RetryWorkThread(InvalRequestStorage *request_storage, InvalServer *inval_server);
    void run(tbsys::CThread * thread, void *arg);
  private:
    //killed
    ~RetryWorkThread();
    InvalRequestStorage *request_storage;
    InvalServer* inval_server;
    static const int MAX_EXECUTED_COUNT = 100;
    static const int MAX_TASK_QUEUE_SIZE = 10000;
  };
}
#endif
