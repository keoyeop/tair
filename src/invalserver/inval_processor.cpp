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

  void RequestProcessor::process(PacketWrapper *wrapper)
  {
    int pcode = wrapper->get_packet()->getPCode();
    switch (pcode)
    {
      case TAIR_REQ_INVAL_PACKET:
        {
        SingleWrapper * single_wrapper = dynamic_cast<SingleWrapper*>(wrapper);
        do_process(&tair_client_impl::remove, single_wrapper);
        break;
        }
      case TAIR_REQ_HIDE_BY_PROXY_PACKET:
        break;
      case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
        break;
      case TAIR_REQ_PREFIX_INVALIDS_PACKET:
        break;
      default:
        log_error("[BUG] ");
    }
  }

  void RequestProcessor::process_callback(int rcode, PacketWrapper *wrapper)
  {

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
        delete wrapper;
      }
      else
      {
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
      process_failed_request(wrapper);
    }
  }

  void RequestProcessor::do_process(PROCESS_RHS_FUNC_T pproc, MultiWrapper *wrapper)
  {
    tair_client_impl *tair_client = wrapper->get_tair_client();
    if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_keys()),&failed_key_code_map,
            client_callback_with_multi_keys, (void*)wrapper) != TAIR_RETURN_SUCCESS)
    {
      process_failed_request(wrapper);
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
  }
}

