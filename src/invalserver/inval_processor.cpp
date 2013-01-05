#include "inval_processor.hpp"
#include "inval_retry_thread.hpp"
#include "inval_stat.hpp"
#include "inval_request_storage.hpp"
#include "base_packet.hpp"
#include "inval_group.hpp"
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
    log_debug("call back invoked");

    TairGroup *group = wrapper->get_group();
    group->dec_uninvoked_callback_count();

    if (wrapper->dec_and_return_reference_count(1) == 0)
    {
      //send packet to client
      if (wrapper->get_needed_return_packet())
      {
        REQUEST_PROCESSOR.send_return_packet(wrapper, TAIR_RETURN_SUCCESS);
      }

      //change request's status, if dataserver returns the failed `rcode.
      if (!(rcode == TAIR_RETURN_SUCCESS || rcode == TAIR_RETURN_DATA_NOT_EXIST))
      {
        wrapper->set_request_status(COMMITTED_FAILED);
        if (rcode == TAIR_RETURN_TIMEOUT)
        {
          group->inc_request_timeout_count();
        }
      }

      if (wrapper->get_request_status() == COMMITTED_SUCCESS)
      {
        //release the wrapper
        log_debug("release the wrapper, finished the request.");
        delete wrapper;
      }
      else
      {
        log_warn("request failed, retry the request, retry times: %d", wrapper->get_retry_times());
        //change the request's status from COMMITTED_FAILED to RETRY_COMMIT,
        //and push the wrapper into retry_thread's queue.
        wrapper->set_needed_return_packet(false);
        wrapper->set_request_status(RETRY_COMMIT);
        retry_thread->add_packet(wrapper, wrapper->get_retry_times());
        wrapper->inc_retry_times();
        //wrapper will be released by `retry_thread.
      }
    }
    else
    {
      //`request_reference_count is not equal to 0.
      //check the request status necessarily.
      log_debug("the reques reference count is more than 0, just release the wrapper.");
      if (!(rcode == TAIR_RETURN_SUCCESS || rcode == TAIR_RETURN_DATA_NOT_EXIST))
      {
        wrapper->set_request_status(COMMITTED_FAILED);
        if (rcode == TAIR_RETURN_TIMEOUT)
        {
          group->inc_request_timeout_count();
        }
      }
      delete wrapper;
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
    if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_key()),
          client_callback_with_single_key, (void*)wrapper) != TAIR_RETURN_SUCCESS)
    {
      vector<std::string> servers;
      tair_client_impl *client = wrapper->get_tair_client();
      client->get_server_with_key(*(wrapper->get_key()), servers);
      log_warn("failed to send request to data server: %s, group name: %s",
          servers[0].c_str(), wrapper->get_packet()->group_name);
      process_failed_request(wrapper);
    }
    else
    {
      log_debug("send request to the data server successfully.");
    }
  }

  void RequestProcessor::do_process(PROCESS_RHS_FUNC_T pproc, MultiWrapper *wrapper)
  {
    tair_client_impl *tair_client = wrapper->get_tair_client();
    if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_keys()),&failed_key_code_map,
          client_callback_with_multi_keys, (void*)wrapper) != TAIR_RETURN_SUCCESS)
    {
      vector<std::string> servers;
      tair_client_impl *client = wrapper->get_tair_client();
      tair_dataentry_set *keys = wrapper->get_keys();
      if (keys == NULL || keys->empty())
      {
        //bug: request without any key.
        log_error("FATAL ERROR, request without any key");
        return;
      }
      client->get_server_with_key(**(keys->begin()), servers);
      log_warn("failed to send request to data server: %s, group name: %s",
          servers[0].c_str(), wrapper->get_packet()->group_name);
      process_failed_request(wrapper);
    }
    else
    {
      log_debug("send request to the data server successfully.");
    }
  }

  void RequestProcessor::end_request(PacketWrapper *wrapper)
  {
    //send return packet if needed.
    if (wrapper->get_needed_return_packet() == true)
    {
      send_return_packet(wrapper, TAIR_RETURN_SUCCESS);
    }

    //push the request to retry_thread's queue if request failed.
    if (wrapper->get_request_status() == COMMITTED_FAILED)
    {
      wrapper->set_needed_return_packet(false);
      wrapper->set_request_status(RETRY_COMMIT);
      //push the wrapper into the retry_thread's queue
      retry_thread->add_packet(wrapper, wrapper->get_retry_times());
      wrapper->inc_retry_times();
    }
    else
    {
      log_error("FATAL ERORR, request state: %d", wrapper->get_request_status());
    }
  }
  void RequestProcessor::process_failed_request(PacketWrapper *wrapper)
  {
    wrapper->set_request_status(COMMITTED_FAILED);
    //decrease the reference count
    if (wrapper->dec_and_return_reference_count(1) == 0)
    {
      end_request(wrapper);
    }
    else
    {
      delete wrapper;
    }
  }
}

