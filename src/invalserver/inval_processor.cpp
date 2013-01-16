#include "inval_processor.hpp"
#include "inval_retry_thread.hpp"
#include "inval_stat.hpp"
#include "inval_request_storage.hpp"
#include "base_packet.hpp"
#include "inval_group.hpp"
#include "inval_request_packet_wrapper.hpp"
namespace tair {
  //map pcode to operation_name
  std::map<int, int> RequestProcessor::pcode_opname_map;
  tair_packet_factory RequestProcessor::packet_factory;
  RequestProcessor RequestProcessor::request_processor_instance;

  //constructor
  RequestProcessor::RequestProcessor()
  {
    pcode_opname_map[TAIR_REQ_INVAL_PACKET] = InvalStatHelper::INVALID;
    pcode_opname_map[TAIR_REQ_PREFIX_INVALIDS_PACKET] = InvalStatHelper::PREFIX_INVALID;
    pcode_opname_map[TAIR_REQ_HIDE_BY_PROXY_PACKET] = InvalStatHelper::HIDE;
    pcode_opname_map[TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET] = InvalStatHelper::PREFIX_HIDE;
  }

  void RequestProcessor::setThreadParameter(InvalRetryThread *retry_thread, InvalRequestStorage *request_storage)
  {
    this->retry_thread = retry_thread;
    this->request_storage = request_storage;
  }

  void RequestProcessor::do_process_request(PROCESS_RH_FUNC_T pproc, PacketWrapper *wrapper)
  {
    SingleWrapper *single_wrapper = dynamic_cast<SingleWrapper*>(wrapper);
    if (single_wrapper != NULL)
    {
      do_process(pproc, single_wrapper);
    }
    else
    {
      log_error("FATAL ERROR, the wrapper should be SingleWrapper, pcode: %d", wrapper->get_packet()->getPCode());
    }
  }

  void RequestProcessor::do_process_request(PROCESS_RHS_FUNC_T pproc, PacketWrapper *wrapper)
  {
    MultiWrapper *multi_wrapper = dynamic_cast<MultiWrapper*>(wrapper);
    if (multi_wrapper != NULL)
    {
      do_process(pproc, multi_wrapper);
    }
    else
    {
      log_error("FATAL ERROR, the wrapper should be MultiWrapper, pcode: %d", wrapper->get_packet()->getPCode());
    }
  }

  void RequestProcessor::process(PacketWrapper *wrapper)
  {
    if (wrapper == NULL)
    {
      log_error("FATAL ERROR, wrapper is null.");
    }
    else
    {
      int pcode = wrapper->get_packet()->getPCode();
      switch (pcode)
      {
        case TAIR_REQ_INVAL_PACKET:
          {
            do_process_request(&tair_client_impl::remove, wrapper);
            break;
          }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          {
            do_process_request(&tair_client_impl::hide, wrapper);
            break;
          }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          {
            do_process_request(&tair_client_impl::hides, wrapper);
            break;
          }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          {
            do_process_request(&tair_client_impl::removes, wrapper);
            break;
          }
        default:
          log_error("FATAL ERORR, unknown pakcet, pcode: %d", pcode);
      }
    }
  }

  void RequestProcessor::process_callback(int rcode, PacketWrapper *wrapper)
  {
    log_debug("callback invoked from cluster %s, reference count: %d",
        wrapper->get_group()->get_cluster_name().c_str(),
        wrapper->get_request_reference_count());
    TairGroup *group = wrapper->get_group();
    group->dec_uninvoked_callback_count();


    //change request's status, if dataserver returns the failed `rcode.
    //should change the request status.
    if (rcode == TAIR_RETURN_SUCCESS || rcode == TAIR_RETURN_DATA_NOT_EXIST)
    {
      log_debug("request committed to cluster %s success, rcode; %d",
          wrapper->get_group()->get_cluster_name().c_str(), rcode);
      if (wrapper->get_request_status() != COMMITTED_FAILED)
      {
        wrapper->set_request_status(COMMITTED_SUCCESS);
      }
    }
    else
    {
      log_debug("request committed to cluster %s failed, rcode; %d",
          wrapper->get_group()->get_cluster_name().c_str(), rcode);
      wrapper->set_request_status(COMMITTED_FAILED);
      if (rcode == TAIR_RETURN_TIMEOUT)
      {
        group->inc_request_timeout_count();
      }
    }
    //release wrapper
    log_debug("release wrapper by cluster %s", wrapper->get_group()->get_cluster_name().c_str());
    delete wrapper;
  }

  void RequestProcessor::end_request(PacketWrapper *wrapper)
  {
    if (wrapper->get_needed_return_packet())
    {
      log_debug("request finished");
      send_return_packet(wrapper, TAIR_RETURN_SUCCESS);
    }

    if (wrapper->get_request_status() == COMMITTED_FAILED)
    {
      //retry request.
      SharedInfo *old_shared = wrapper->get_shared_info();
      int retry_times = old_shared->get_retry_times();
      if (retry_times < InvalRetryThread::RETRY_COUNT)
      {
        log_error("REQUEST FAILED, request will be retried to process, cluster name: %s, group name: %s, queue: %d, pcode: %d",
            wrapper->get_group()->get_cluster_name().c_str(), wrapper->get_group()->get_group_name().c_str(),
            retry_times, old_shared->packet->getPCode());
        SharedInfo *new_shared = new SharedInfo(0, old_shared->packet);
        new_shared->set_retry_times(retry_times);
        retry_thread->add_packet(new_shared, retry_times);
      }
      else
      {
        old_shared->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request(old_shared->packet);
      }
      //request packet was released by `shared, while the request's status is equ. to COMMITTED_SUCCESS.
      old_shared->packet = NULL;
    }
  }

  void RequestProcessor::send_return_packet(PacketWrapper *wrapper, const int ret)
  {
    if (wrapper->get_packet()->get_direction() == DIRECTION_RECEIVE)
    {
      tair_packet_factory::set_return_packet(wrapper->get_packet(), ret, "success.", 0);
    }
  }

  void RequestProcessor::do_process(PROCESS_RH_FUNC_T pproc, SingleWrapper *wrapper)
  {
    tair_client_impl *tair_client = wrapper->get_tair_client();
    if (tair_client != NULL)
    {
      TairGroup *group = wrapper->get_group();
      if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_key()),
            client_callback_with_single_key, (void*)wrapper) != TAIR_RETURN_SUCCESS)
      {
        log_error("send request to cluster %s failed.",
            wrapper->get_group()->get_cluster_name().c_str());
        vector<std::string> servers;
        tair_client->get_server_with_key(*(wrapper->get_key()), servers);
        if (!servers.empty())
        {
          log_warn("failed to send request to data server: %s, group name: %s",
              servers[0].c_str(), wrapper->get_packet()->get_group_name());
        }
        wrapper->set_request_status(COMMITTED_FAILED);
        delete wrapper;
      }
      else
      {
        if (group != NULL)
        {
          log_debug("send request to cluster %s success.",
              group->get_cluster_name().c_str());
        }
      }
    }
    else
    {
      //bug: should not be here.
      log_error("FATAL ERROR, wrapper without tair_client, cluster name: %s, group name: %s",
          wrapper->get_group()->get_cluster_name().c_str(),
          wrapper->get_group()->get_group_name().c_str());
      delete wrapper;
    }
  }

  void RequestProcessor::process_failed_request(PacketWrapper *wrapper)
  {
    if (wrapper != NULL)
    {
      wrapper->set_request_status(COMMITTED_FAILED);
      //just release the wrapper
      delete wrapper;
    }
    else
    {
      log_error("FATAL ERROR, wrapper is null.");
    }
  }

  void RequestProcessor::do_process(PROCESS_RHS_FUNC_T pproc, MultiWrapper *wrapper)
  {
    tair_client_impl *tair_client = wrapper->get_tair_client();
    if (tair_client != NULL)
    {
      TairGroup *group = wrapper->get_group();
      if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_keys()),&failed_key_code_map,
            client_callback_with_multi_keys, (void*)wrapper) != TAIR_RETURN_SUCCESS)
      {
        log_debug("send request to cluster %s failed.",
            wrapper->get_group()->get_cluster_name().c_str());
        vector<std::string> servers;
        tair_client_impl *client = wrapper->get_tair_client();
        tair_dataentry_set *keys = wrapper->get_keys();
        if (keys == NULL || keys->empty())
        {
          //bug: request without any key.
          log_error("FATAL ERROR, request without any key, cluster name: %s, group name: %s, pcode: %d",
              wrapper->get_group()->get_cluster_name().c_str(),
              wrapper->get_group()->get_group_name().c_str(), wrapper->get_packet()->getPCode());
          return;
        }
        client->get_server_with_key(**(keys->begin()), servers);
        wrapper->set_request_status(COMMITTED_FAILED);
        //just release the wrapper
        delete wrapper;
      }
      else
      {
        if (group != NULL)
        {
          log_debug("send request to cluster %s success.",
              group->get_cluster_name().c_str());
        }
      }
    }
    else
    {
      //bug: should not be here.
      log_error("FATAL ERROR, wrapper without tair_client, cluster name: %s, group name: %s",
          wrapper->get_group()->get_cluster_name().c_str(),
          wrapper->get_group()->get_group_name().c_str());
      delete wrapper;
    }
  }
}

