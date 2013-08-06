#include "inval_heartbeat_thread.hpp"
  namespace tair {
    InvalHeartbeatThread::InvalHeartbeatThread()
    {
      conn_manager = NULL;
    }

    InvalHeartbeatThread::~InvalHeartbeatThread()
    {
      //the transport stoped by `inval_server
      if (conn_manager)
      {
        delete conn_manager;
      }
    }
    int InvalHeartbeatThread::get_config_server_version()
    {
      return atomic_read(&config_server_version);
    }
    void InvalHeartbeatThread::set_thread_parameters(tair_packet_streamer *streamer,
        tbnet::Transport *transport)
    {
      heartbeat_packet.server_id = local_server_ip::ip;
      conn_manager = new tbnet::ConnectionManager(transport, streamer, this);
    }

    tbnet::IPacketHandler::HPRetCode InvalHeartbeatThread::handlePacket(tbnet::Packet *packet, void *args)
    {
      if (!packet->isRegularPacket()) {
        tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
        log_error("ControlPacket, cmd:%d", cp->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }
      int pcode = packet->getPCode();
      if (pcode == TAIR_RESP_INVAL_HEARTBEAT_PACKET)
      {
        response_inval_heartbeat *resp = (response_inval_heartbeat*) packet;
        do_response_heartbeat(resp);
      }
      else
      {
        log_error("Unknown packet: %d was received by Inval Server", pcode);
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }

    bool InvalHeartbeatThread::load_host_configserver()
    {
      bool ret = true;
      vector<const char*> str_list = TBSYS_CONFIG.getStringList(TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
      if (str_list.size() == 0U)
      {
        log_error("miss config item %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
        ret = false;
      }
      if (ret)
      {
        int port = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
        uint64_t id = 0;
        for (uint32_t i=0; i<str_list.size(); i++)
        {
          id = tbsys::CNetUtil::strToAddr(str_list[i], port);
          if (id == 0) continue;
          log_info("got host configserver: %s", tbsys::CNetUtil::addrToString(id).c_str());
          host_config_server_list.push_back(id);
          if (host_config_server_list.size() == 2U)
          {
            break;
          }
        }
      }

      if (ret && host_config_server_list.size() == 0U)
      {
        log_error("config item is not avaliable %s:%s", TAIRPUBLIC_SECTION, TAIR_CONFIG_SERVER);
        ret = false;
      }
      return ret;
    }

    void InvalHeartbeatThread::do_request_heartbeat()
    {
      if (host_config_server_list.size() != 0)
      {
        for (size_t i = 0; i < host_config_server_list.size(); ++i)
        {
          request_inval_heartbeat *req = new request_inval_heartbeat(heartbeat_packet);
          if (conn_manager->sendPacket(host_config_server_list[i], req, NULL, (void*)((long)i)) == false)
          {
            log_error("failed to send heartbeat packet to host configserver: %s",
                tbsys::CNetUtil::addrToString(host_config_server_list[i]).c_str());
            delete req;
          }
        }
      }
      else
      {
        log_error("lost host config server");
      }
    }

    void InvalHeartbeatThread::do_response_heartbeat(response_inval_heartbeat* resp)
    {
      //the `config_server_version returned by slave configserver was equal to zero.
      if (resp != NULL && resp->get_config_server_version() > 0)
      {
        atomic_set(&config_server_version, resp->get_config_server_version());
      }
    }


    void InvalHeartbeatThread::run(tbsys::CThread *thread, void *arg)
    {
      //load the host config server
      if (!load_host_configserver())
      {
        log_error("failed to load host configserver, inval heartbeat thread will be shutdown.");
        return;
      }
      while(!_stop)
      {
        do_request_heartbeat();
        TAIR_SLEEP(_stop, HEARTBEAT_SLEEP_TIME);
      }
    }
  }
