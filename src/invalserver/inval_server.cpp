#include "inval_server.hpp"
#include "inval_request_packet_wrapper.hpp"
#include "inval_group.hpp"
  namespace tair {
    InvalServer::InvalServer()
    {
      _stop = false;
      sync_task_thread_count = 0;
      stop_retry_work();
    }

    InvalServer::~InvalServer()
    {
    }

    void InvalServer::start()
    {
      //~ initialization
      if (!init())
      {
        return ;
      }

      //~ start the thread to save the request packets.
      request_storage.start();
      //~ start thread that retrieves the group infos.
      invalid_loader.start();
      //~ start the retry threads.
      retry_thread.start();
      //~ start the threads handling the packets received from clients.
      task_queue_thread.start();
      //~ start the stat thread
      TAIR_INVAL_STAT.start();

      char spec[32];
      bool ret = true;
      //~ establish the server socket.
      if (ret)
      {
        int port = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PORT, TAIR_INVAL_SERVER_DEFAULT_PORT);
        snprintf(spec, sizeof(spec), "tcp::%d", port);
        if (transport.listen(spec, &streamer, this) == NULL)
        {
          log_error("listen on port %d failure.", port);
          ret = false;
        }
        else
        {
          log_info("listen on port %d.", port);
        }
      }
      //~ start the network components.
      if (ret)
      {
        log_info("invalid server start running, pid: %d", getpid());
        transport.start();
      }
      else
      {
        stop();
      }

      //~ wait threads to complete.
      invalid_loader.wait();
      task_queue_thread.wait();
      retry_thread.wait();
      TAIR_INVAL_STAT.wait();
      request_storage.wait();
      transport.wait();

      destroy();
    }

    void InvalServer::stop()
    {
      //~ stop threads.
      if (!_stop)
      {
        _stop = true;
        transport.stop();
        log_warn("stopping transport");
        task_queue_thread.stop();
        log_warn("stopping task_queue_thread");
        retry_thread.stop();
        log_warn("stopping retry_thread");
        invalid_loader.stop();
        log_warn("stopping invalid_loader");
        TAIR_INVAL_STAT.stop();
        log_warn("stopping stat_helper");
        request_storage.stop();
        log_warn("stopping request_storage");
      }
    }

    //~ the callback interface of IServerAdapter
    tbnet::IPacketHandler::HPRetCode InvalServer::handlePacket(tbnet::Connection *connection,
        tbnet::Packet *packet)
    {
      if (!packet->isRegularPacket())
      {
        log_error("ControlPacket, cmd: %d", ((tbnet::ControlPacket*)packet)->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }
      base_packet *bp = (base_packet*)packet;
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);
      //~ push regular packet to task queue
      if (!task_queue_thread.push(bp))
      {
        log_error("push packet to 'task_queue_thread' failed.");
      }

      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    //~ the callback interface of IPacketQueueHandler
    bool InvalServer::handlePacketQueue(tbnet::Packet *packet, void *arg)
    {
      base_packet *bp = (base_packet*)packet;
      int pcode = bp->getPCode();

      bool send_ret = true;
      int ret = TAIR_RETURN_SUCCESS;
      const char *msg = "";

      switch (pcode)
      {
        case TAIR_REQ_INVAL_PACKET:
          {
            request_invalid *req = dynamic_cast<request_invalid*>(bp);
            if (req != NULL)
            {
              do_invalid(req);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to request_invalid packet.");
            }
            break;
          }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          {
            request_hide_by_proxy *req = dynamic_cast<request_hide_by_proxy*>(bp);
            if (req != NULL)
            {
              do_hide(req);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to request_hide_by_proxy packet.");
            }
            break;
          }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          {
            request_prefix_hides_by_proxy *req = dynamic_cast<request_prefix_hides_by_proxy*>(bp);
            if (req != NULL)
            {
              do_prefix_hides(req);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to request_prefix_hides_by_proxy packet.");
            }
            break;
          }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          {
            request_prefix_invalids *req = dynamic_cast<request_prefix_invalids*>(bp);
            if (req != NULL)
            {
              do_prefix_invalids(req);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to request_prefix_invalids packet.");
            }
            break;
          }
        case TAIR_REQ_PING_PACKET:
          {
            if (invalid_loader.is_loading())
            {
              log_warn("ping packet received, but clients are still not ready");
              ret = TAIR_RETURN_FAILED;
              msg = "iv not ready";
              break;
            }
            request_ping *req = dynamic_cast<request_ping*>(bp);
            if (req != NULL)
            {
              log_warn("ping packet received, config_version: %u, value: %d", req->config_version, req->value);
              ret = TAIR_RETURN_SUCCESS;
            }
            else
            {
              ret = TAIR_RETURN_FAILED;
            }
            break;
          }
        case TAIR_REQ_INVAL_STAT_PACKET:
          {
            request_inval_stat *req = dynamic_cast<request_inval_stat*>(bp);
            if (req != NULL)
            {
              do_request_stat(req);
              send_ret = false;
            }
            else
            {
              log_error("the request should be request_stat.");
              ret = TAIR_RETURN_FAILED;
            }
            break;
          }
        case TAIR_REQ_RETRY_ALL_PACKET:
          {
            request_retry_all *req = dynamic_cast<request_retry_all*>(bp);
            if (req != NULL)
            {
              do_retry_all(req);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to request_retry_all packet.");
              ret = TAIR_RETURN_FAILED;
            }
            break;
          }
        case TAIR_REQ_OP_CMD_PACKET:
          {
            request_op_cmd *op_cmd = dynamic_cast<request_op_cmd*>(bp);
            if (op_cmd != NULL)
            {
              do_inval_server_cmd(op_cmd);
              send_ret = false;
            }
            else
            {
              log_error("packet could not be casted to requet_op_cmd packet.");
              ret = TAIR_RETURN_FAILED;
            }
            break;
          }
        default:
          {
            log_error("packet not recognized, pcode: %d.", pcode);
            ret = TAIR_RETURN_FAILED;
          }
      }
      //~ set and send the general return_packet.
      if (send_ret && bp->get_direction() == DIRECTION_RECEIVE)
      {
        tair_packet_factory::set_return_packet(bp, ret, msg, 0);
      }
      //~ do not let 'tbnet' delete this 'packet'
      return false;
    }

    void InvalServer::send_return_packet(request_inval_packet* req, int code, const char* msg)
    {
      if (req != NULL && req->get_direction() == DIRECTION_RECEIVE)
      {
        tair_packet_factory::set_return_packet(req, code, msg, 0);
      }
    }

    void InvalServer::process_unknown_groupname_request(request_inval_packet* req)
    {
      //can't find the instance of TairGroup, according to the `req->group_name.
      //just send the return packet to client
      if (req != NULL && req->request_time > 0)
      {
        if (invalid_loader.is_loading())
        {
          send_return_packet(req, TAIR_RETURN_FAILED, "inval server is not ready.");
          log_warn("inval server is still loading group name, request packet will be cached. pcode: %d, group name: %s",
              req->getPCode(), req->group_name);
          //write the request to `request_storage
          request_storage.write_request(req);
        }
        else
        {
          log_warn("unknown request packet, pcode: %d, group name: %s", req->getPCode(), req->group_name);
          send_return_packet(req, TAIR_RETURN_FAILED, "unknown group name.");
          //packet should be released.
          delete req;
        }
      }
    }

    bool InvalServer::push_task(tbnet::Packet *packet)
    {
      return task_queue_thread.push(packet);
    }

    int InvalServer::task_queue_size()
    {
      return task_queue_thread.size();
    }

    //return packet sent by `request_processor.
    void InvalServer::do_request(request_inval_packet *req, int factor, int request_type, bool merged)
    {
      if (req->key_count <= 0)
      {
        delete req;
        log_error("the key count's value is not illegal, key count: %d", req->key_count);
      }
      else
      {
        vector<TairGroup*>* tair_groups = NULL;
        int local_cluster_count = 0;
        bool got = invalid_loader.find_groups(req->group_name, tair_groups, &local_cluster_count);
        if (!got || tair_groups == NULL || tair_groups->empty())
        {
          process_unknown_groupname_request(req);
        }
        else
        {
          bool need_return_packet = true;
          size_t global_reference_count = tair_groups->size() * factor;
          int local_reference_count = local_cluster_count * factor;

          //request packet was cached in the momery, when the request failedkk.
          //return packet should be send to client before packet cached, and set the `request_time to be -1.
          //when re-processing the request packet cached, do not to send return packet anymore.
          if (req->request_time < 0)
          {
            req->request_time = 0;
            need_return_packet = false;
          }
          if (local_reference_count == 0)
          {
            //send return packet to the client;
            send_return_packet(req, TAIR_RETURN_SUCCESS, "success");
            need_return_packet = false;
          }

          //add up the operation
          TAIR_INVAL_STAT.statistcs(request_type,
              std::string(req->group_name), req->area, inval_area_stat::FIRST_EXEC);

          //send request
          SharedInfo *shared = new SharedInfo(global_reference_count, local_reference_count, req);
          for (size_t i = 0; i < tair_groups->size(); ++i)
          {
            (*tair_groups)[i]->commit_request(shared, merged, need_return_packet);
          }
        }
      }
    }

    void InvalServer::do_retry_all(request_retry_all* req)
    {
      int ret = TAIR_RETURN_SUCCESS;
      if (atomic_add_return(1, &retry_work_status) == RETRY_START)
      {
        RetryWorkThread *worker = new RetryWorkThread(&request_storage, this);
        //the worker release by the the function RetryWorkThreadrun
        //never block this thread.
        if (worker == NULL)
        {
          log_error("failed to create retry worker thread.");
          ret = TAIR_RETURN_FAILED;
        }
      }
      else
      {
        //here, the value of retry_work_status is more then 1.
        atomic_dec(&retry_work_status);
        log_warn("retry thread is already working ....");
      }

      //return response
      if (req->get_direction() == DIRECTION_RECEIVE)
      {
        tair_packet_factory::set_return_packet(req, ret, "", 0);
      }
      delete req;
    }

    void InvalServer::do_request_stat(request_inval_stat *req)
    {
      int ret = TAIR_RETURN_SUCCESS;
      response_inval_stat *resp = new response_inval_stat();
      unsigned long buffer_size = 0;
      unsigned long uncompressed_data_size = 0;
      int group_count = 0;
      char *buffer = 0;
      TAIR_INVAL_STAT.get_stat_buffer(buffer, buffer_size, uncompressed_data_size, group_count);
      if (buffer_size == 0 || buffer == NULL)
      {
        log_error("NONE stat info at all");
        ret = TAIR_RETURN_FAILED;
      }
      else
      {
        std::string key("inval_stats");
        resp->key = new data_entry(key.c_str());
        resp->stat_value = new data_entry(buffer, buffer_size, true); //alloc the new memory
        resp->uncompressed_data_size = uncompressed_data_size;
        resp->group_count = group_count;
      }

      resp->set_code(ret);
      resp->setChannelId(req->getChannelId());
      if (req->get_connection()->postPacket(resp) == false)
      {
        log_error("fail to send stat info to client.");
        delete resp;
        ret = TAIR_RETURN_FAILED;
      }
      delete [] buffer; // alloced by `inval_stat_helper
      buffer = NULL;
      delete req;
    }

    void InvalServer::do_inval_server_cmd(request_op_cmd *req)
    {
      int ret = TAIR_RETURN_SUCCESS;
      response_op_cmd *resp = new response_op_cmd ();
      std::vector<std::string> &cmd_set = req->params;
      if (cmd_set.size() < 1)
      {
        log_error("none cmd at all");
        resp->infos.push_back("none cmd");
        ret = TAIR_RETURN_FAILED;
      }
      else
      {
        std::string cmd = cmd_set[0];
        if (strncmp(cmd.c_str(), "info", 4) == 0)
        {
          stringstream buffer;
          buffer << endl << " INVAL SERVER" << endl;
          buffer << get_info() << endl;

          buffer << endl << " INVAL LOADER" << endl;
          buffer << invalid_loader.get_info() << endl;

          buffer << endl << " REQUEST STORAGE" << endl;
          buffer << request_storage.get_info() << endl;

          buffer << endl << " RETRY THREADS" << endl;
          buffer << retry_thread.get_info() << endl;

          resp->infos.push_back(buffer.str());
        }
        else
        {
          log_error("unknown cmd: %s", cmd.c_str());
          resp->infos.push_back("unknown cmd");
          ret = TAIR_RETURN_FAILED;
        }
      }
      resp->set_code(ret);
      resp->setChannelId(req->getChannelId());
      if (req->get_connection()->postPacket(resp) == false)
      {
        log_error("fail to send stat info to client.");
        delete resp;
      }
      delete req;
    }

    bool InvalServer::init()
    {
      //~ get local address
      const char *dev_name = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_DEV_NAME, "eth0");
      uint32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      int port = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PORT, TAIR_INVAL_SERVER_DEFAULT_PORT);
      util::local_server_ip::ip = tbsys::CNetUtil::ipToAddr(ip, port);
      log_info("address: %s", tbsys::CNetUtil::addrToString(util::local_server_ip::ip).c_str());

      const char *data_dir = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_INVAL_DATA_DIR,
          TAIR_INVAL_DEFAULT_DATA_DIR);
      log_info("invalid server data path: %s", data_dir);
      const char *queue_name = "disk_queue";
      //the packet's count default value is 10,000.
      const int cached_packet_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_INVAL_CACHED_PACKET_COUNT,
          TAIR_INVAL_DEFAULT_CACHED_PACKET_COUNT);
      request_storage.setThreadParameter(data_dir, queue_name, lower_limit_ratio, upper_limit_ratio,
          cached_packet_count, &packet_factory);
      //~ set packet factory for packet streamer.
      streamer.setPacketFactory(&packet_factory);
      int thread_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, 4);
      //~ set the number of threads to handle the requests.
      task_queue_thread.setThreadParameter(thread_count, this, NULL);
      retry_thread.setThreadParameter(&invalid_loader, &request_storage);
      sync_task_thread_count = thread_count;
      REQUEST_PROCESSOR.setThreadParameter(&retry_thread, &request_storage);
      int max_failed_count = TBSYS_CONFIG.getInt(INVALSERVER_SECTION,
          TAIR_INVAL_MAX_FAILED_COUNT, TAIR_INVAL_DEFAULT_MAX_FAILED_COUNT);
      if (max_failed_count <=0)
        max_failed_count = TAIR_INVAL_DEFAULT_MAX_FAILED_COUNT;
      invalid_loader.setThreadParameter(max_failed_count);
      return true;
    }

    bool InvalServer::destroy()
    {
      return true;
    }

    std::string InvalServer::get_info()
    {
      stringstream buffer;
      buffer << " task thread count: " << sync_task_thread_count << endl;
      buffer << " retry thread is working: " << (atomic_read(&retry_work_status) == RETRY_START ? "YES" : "NO") << endl;
      return buffer.str();
    }

    //begin retry_worker_thread
    RetryWorkThread::RetryWorkThread(InvalRequestStorage *request_storage, InvalServer *inval_server)
    {
      this->request_storage = request_storage;
      this->inval_server = inval_server;
      start();
      log_warn("retry_worker start.");
    }

    RetryWorkThread::~RetryWorkThread()
    {
      wait();
      log_warn("retry_worker stop.");
    }

    void RetryWorkThread::run(tbsys::CThread *thread, void *arg)
    {
      log_warn("retry thread start");
      int executed_count = 0;
      if (request_storage != NULL && inval_server != NULL)
      {
        float safety_ratio = 0.75;
        while (!_stop && executed_count < MAX_EXECUTED_COUNT)
        {
          if(request_storage->get_packet_count() == 0)
          {
            log_debug("the request_storage is empty, retry worker stop.");
            break;
          }
          int limit_size = (int) (MAX_TASK_QUEUE_SIZE * safety_ratio);
          int task_queue_size = inval_server->task_queue_size();
          int retry_packet_count = limit_size - task_queue_size;
          if (retry_packet_count <= 0)
          {
            log_warn("task queue size: %d", task_queue_size);
          }
          else
          {
            std::vector<base_packet*> packet_vector;
            request_storage->read_request(packet_vector, retry_packet_count);
            //push packet to the task_queue
            for (size_t i = 0; i < packet_vector.size(); ++i)
            {
              //typedef request_invalid request_inval_packet.
              //request_invalid, request_prefix_invalids, request_hide_by_proxy, and
              //request_prefix_hides_by_proxy are the subclass of request_invalid.
              request_inval_packet *req = (request_inval_packet*)packet_vector[i];
              if (req != NULL)
              {
                //should not send return packet to client.
                req->request_time = -1;
                if (!inval_server->push_task(req))
                {
                  log_warn("failed to push retry request packet to task queue, task queue size: %d",
                      inval_server->task_queue_size());
                  //write to the storage.
                  request_storage->write_request(req);
                }
              }
            }
          }
          executed_count++;
          sleep(10);
        }
      }

      stop();
      log_warn("stop retry worker, cached packet count: %d, disk_queue is empty: %s",
          request_storage->get_packet_count(), request_storage->get_packet_count() == 0 ? "yes" : "no");

      //never use it at all.
      inval_server->stop_retry_work();
      delete this;
    }

  } //~ end of namespace tair

tair::InvalServer * invalid_server = NULL;
uint64_t tair::util::local_server_ip::ip = 0;

//~ signal handler
void sig_handler(int sig)
{
  switch (sig)
  {
    case SIGTERM:
    case SIGINT:
      if (invalid_server != NULL)
      {
        invalid_server->stop();
      }
      break;
    case 40:
      TBSYS_LOGGER.checkFile();
      break;
    case 41:
    case 42:
      if (sig == 41)
      {
        TBSYS_LOGGER._level++;
      }
      else
      {
        TBSYS_LOGGER._level--;
      }
      log_error("log level changed to %d.", TBSYS_LOGGER._level);
      break;
  }
}
//~ print the help information
void print_usage(char *prog_name)
{
  fprintf(stderr, "%s -f config_file\n"
      "    -f, --config_file  config file\n"
      "    -h, --help         show this info\n"
      "    -V, --version      show build time\n\n",
      prog_name);
}
//~ parse the command line
char *parse_cmd_line(int argc, char *const argv[])
{
  int opt;
  const char *optstring = "hVf:";
  struct option longopts[] = {
    {"config_file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {"version", 0, NULL, 'V'},
    {0, 0, 0, 0}
  };

  char *configFile = NULL;
  while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'f':
        configFile = optarg;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\nSVN: %s\n", __DATE__, __TIME__, TAIR_SVN_INFO);
        exit (1);
      case 'h':
        print_usage(argv[0]);
        exit (1);
    }
  }
  return configFile;
}

int main(int argc, char **argv)
{
  const char *configfile = parse_cmd_line(argc, argv);
  if (configfile == NULL)
  {
    print_usage(argv[0]);
    return 1;
  }
  if (TBSYS_CONFIG.load(configfile))
  {
    fprintf(stderr, "load ConfigFile %s failed.\n", configfile);
    return 1;
  }

  const char *pidFile = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_PID_FILE, "inval.pid");
  const char *logFile = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_FILE, "inval.log");

  if (1)
  {
    char *p, dirpath[256];
    snprintf(dirpath, sizeof(dirpath), "%s", pidFile);
    p = strrchr(dirpath, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath))
    {
      fprintf(stderr, "mkdirs %s failed.\n", dirpath);
      return 1;
    }
    snprintf(dirpath, sizeof(dirpath), "%s", logFile);
    p = strrchr(dirpath, '/');
    if (p != NULL) *p = '\0';
    if (p != NULL && !tbsys::CFileUtil::mkdirs(dirpath))
    {
      fprintf(stderr, "mkdirs %s failed.\n", dirpath);
      return 1;
    }
  }
  //~ check if one process is already running.
  int pid;
  if ((pid = tbsys::CProcess::existPid(pidFile)))
  {
    fprintf(stderr, "process already running, pid:%d\n", pid);
    return 1;
  }

  const char *logLevel = TBSYS_CONFIG.getString(INVALSERVER_SECTION, TAIR_LOG_LEVEL, "info");
  TBSYS_LOGGER.setLogLevel(logLevel);
  TBSYS_LOGGER.setMaxFileSize(1<<30);

  //~ run as daemon process
  if (tbsys::CProcess::startDaemon(pidFile, logFile) == 0)
  {
    //~ register signal handlers.
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(40, sig_handler);
    signal(41, sig_handler);
    signal(42, sig_handler);

    //~ function starts.
    invalid_server = new tair::InvalServer();
    log_info("starting invalid server.");
    invalid_server->start();
    delete invalid_server;
    log_info("process exit.");
  }
  return 0;
}
