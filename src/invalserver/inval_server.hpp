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
    inline void stop_retry_work()
    {
      atomic_set(&retry_work_status, RETRY_STOP);
    }
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

    void do_request_stat(request_inval_stat *req);
    void do_retry_all(request_retry_all* req);
    void do_inval_server_cmd(request_op_cmd *rq);
    std::string get_info();
    void process_unknown_groupname_request(tbnet::Packet *packet);
    bool init();
    bool destroy();
  private:
    bool _stop;
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
    enum
    {
      RETRY_STOP = 0,
      RETRY_START = 1
    };
    atomic_t retry_work_status;

    //do not cache sufficient request packets in memory. the `lower_limit_ratio and `upper_limit_ratio control
    //the count of request packets cached in memory. in other words, read packets from disk to memroy, while
    //the count of request packets is less than the value of `lower_limit_ratio * MAX_CACHED_PACKET_COUNT, and
    //write the packets in memory to disk, while the count of request packets is more than the value of
    //'upper_limit_ratio * MAX_CACHED_PACKET_COUNT;
    static const float upper_limit_ratio = 0.8;
    static const float lower_limit_ratio = 0.2;
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
    //to avoid to process the rety-working without end, it's necessary to limit the exected times of retry-working.
    static const int MAX_EXECUTED_COUNT = 100;
    //the `retry_worker_thread will push the cached packet into the queue of task thread pool. the value of
    //`MAX_TASK_QUEUE_SIZE is the upper bound of queue's size while `retry_worker_thread is running.
    static const int MAX_TASK_QUEUE_SIZE = 10000;
  };
}
#endif
