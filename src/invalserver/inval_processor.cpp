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
      if (wrapper->get_needed_return_packet() == true)
      {
        REQUEST_PROCESSOR.send_return_packet(wrapper, TAIR_RETURN_SUCCESS);
      }

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
        delete wrapper;
      }
      else
      {
        wrapper->set_needed_return_packet(false);
        wrapper->set_request_status(RETRY_COMMIT);
        do_rescue_failure(wrapper);
      }
    }
    else
    {
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

  void RequestProcessor::do_rescue_failure(PacketWrapper *wrapper)
  {
    //retry operation
    if (wrapper->get_retry_times() < InvalRetryThread::RETRY_COUNT)
    {
      wrapper->set_request_status(RETRY_COMMIT);
      retry_thread->add_packet(wrapper, wrapper->get_retry_times());
      wrapper->inc_retry_times();
    }
    //write the request packet to disk
    else
    {
      if (wrapper->get_request_reference_count() == 0 && wrapper->get_request_delay_count() == 0)
      {
        wrapper->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request(wrapper->get_packet());
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
      retry_thread->add_packet(wrapper, wrapper->get_retry_times());
    }
  }

  void RequestProcessor::do_process(PROCESS_RHS_FUNC_T pproc, MultiWrapper *wrapper)
  {
    tair_client_impl *tair_client = wrapper->get_tair_client();
    if ((tair_client->*pproc)(wrapper->get_packet()->area, *(wrapper->get_keys()),&failed_key_code_map,
            client_callback_with_multi_keys, (void*)wrapper) != TAIR_RETURN_SUCCESS)
    {
      retry_thread->add_packet(wrapper, wrapper->get_retry_times());
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
    }
  }
}

