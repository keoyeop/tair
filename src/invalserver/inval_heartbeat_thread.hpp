#ifndef INVAL_HEARTBEAT_THREAD_H
#define INVAL_HEARTBEAT_THREAD_H
#include <unistd.h>
#include <vector>
#include <tbsys.h>
#include <tbnet.h>
#include "define.hpp"
#include "log.hpp"
#include "inval_heartbeat_packet.hpp"
#include "packet_streamer.hpp"
namespace tair {
  class InvalHeartbeatThread: public tbsys::CDefaultRunnable, public tbnet::IPacketHandler
  {
  public:
    InvalHeartbeatThread();
    virtual ~InvalHeartbeatThread();

    void run(tbsys::CThread *thread, void *arg);

    void set_thread_parameters(tair_packet_streamer *streamer, tbnet::Transport* transport);

    tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet *packet, void *args);

    int get_config_server_version();
  protected:
    //send heartbeat request to host configserver
    void do_request_heartbeat();

    //receve heartbeat response from host configserver
    void do_response_heartbeat(response_inval_heartbeat* resp);

    //load config
    bool load_host_configserver();
  protected:
    request_inval_heartbeat heartbeat_packet;
    tbnet::ConnectionManager* conn_manager;
    vector<uint64_t> host_config_server_list;
    atomic_t config_server_version;
    static const int HEARTBEAT_SLEEP_TIME = 1;
  };
}
#endif
