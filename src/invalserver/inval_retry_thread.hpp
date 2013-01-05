#ifndef INVAL_RETRY_THREAD_HPP
#define INVAL_RETRY_THREAD_HPP

#include <tbsys.h>
#include <tbnet.h>

#include "log.hpp"
#include "base_packet.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "inval_request_storage.hpp"
#include "inval_stat_helper.hpp"
#include <queue>

#ifndef PACKET_GROUP_NAME
#define PACKET_GROUP_NAME(ipacket, hpacket) \
  ((ipacket) ? (ipacket)->group_name : (hpacket)->group_name)
#endif

namespace tair {
  class PacketWrapper;
  class InvalLoader;
  class InvalRetryThread: public tbsys::CDefaultRunnable {
  public:
    InvalRetryThread();
    ~InvalRetryThread();

    void setThreadParameter(InvalLoader *invalid_loader, InvalRequestStorage * request_storage);

    void stop();
    void run(tbsys::CThread *thread, void *arg);
    void add_packet(PacketWrapper *wrapper, int index);
    int retry_queue_size(int index);

    static const int RETRY_COUNT = 3;
  private:
    void do_retry_commit_request(PacketWrapper *wrapper, int operation_type, bool merged);
  private:
    static const int MAX_QUEUE_SIZE = 10000;
    tbsys::CThreadCond queue_cond[RETRY_COUNT];
    std::queue<PacketWrapper*> retry_queue[RETRY_COUNT];

    InvalLoader *invalid_loader;
    InvalRequestStorage *request_storage; //from inval server
  };
}
#endif
