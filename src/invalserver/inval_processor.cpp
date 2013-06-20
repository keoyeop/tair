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
    dump_key_switch = false;
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
      log_error("the wrapper should be SingleWrapper, pcode: %d", wrapper->get_packet()->getPCode());
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
      log_error("the wrapper should be MultiWrapper, pcode: %d", wrapper->get_packet()->getPCode());
    }
  }

  void RequestProcessor::process(PacketWrapper *wrapper)
  {
    if (wrapper == NULL)
    {
      log_error("wrapper is null.");
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
          log_error("unknown pakcet, pcode: %d", pcode);
      }
    }
  }

  void RequestProcessor::client_callback_with_single_key(int rcode, void* args)
  {
    if (args == NULL)
    {
      log_error("the args is null, rcode: %d", rcode);
    }
    else
    {
      PacketWrapper *wrapper = (PacketWrapper*)args;
      if (wrapper == NULL)
      {
        log_error("the callback's parameter is not instance of PacketWrapper, rcode: %d", rcode);
      }
      else
      {
        REQUEST_PROCESSOR.process_callback(rcode, wrapper);
      }
    }
  }

  void RequestProcessor::client_callback_with_multi_keys(int rcode, const key_code_map_t *key_code_map, void *args)
  {
    if (args == NULL)
    {
      log_error("the args is null, rcode: %d", rcode);
    }
    else
    {
      PacketWrapper *wrapper = (PacketWrapper*)args;
      if (wrapper == NULL)
      {
        log_error("the callback's parameter is not instance of PacketWrapper, rcode: %d", rcode);
      }
      else
      {
        if (rcode == TAIR_RETURN_PARTIAL_SUCCESS && key_code_map != NULL)
        {
          SharedInfo *shared = wrapper->get_shared_info();
          request_inval_packet *req = NULL;
          if (shared != NULL)
          {
            req = shared->packet;
          }
          if (req != NULL)
          {
            bool all_success = true;
            for (key_code_map_t::const_iterator it = key_code_map->begin(); it != key_code_map->end(); ++it)
            {
              if (!(it->second == TAIR_RETURN_DATA_NOT_EXIST || it->second == DATA_EXPIRED))
              {
                all_success = false;
                log_debug("multi-keys callback, rcode; %d, pcode: %d, sub rcode: %d", rcode, req->getPCode(), it->second);
                break;
              }
            }
            if (all_success)
            {
              rcode = TAIR_RETURN_SUCCESS;
            }
          }
        }
        REQUEST_PROCESSOR.process_callback(rcode, wrapper);
      }
    }
  }
  void RequestProcessor::process_callback(int rcode, PacketWrapper *wrapper)
  {
    log_debug("callback invoked from cluster %s, reference count: %d",
        wrapper->get_group()->get_cluster_name().c_str(),
        wrapper->get_request_reference_count());
    TairGroup *group = wrapper->get_group();


    //change request's status, if dataserver returns the failed `rcode.
    //should change the request status.
    if (rcode == TAIR_RETURN_SUCCESS || rcode == TAIR_RETURN_DATA_NOT_EXIST || rcode == DATA_EXPIRED)
    {
      log_debug("request committed to cluster %s success, rcode; %d",
          group->get_cluster_name().c_str(), rcode);
      if (wrapper->get_request_status() != COMMITTED_FAILED)
      {
        wrapper->set_request_status(COMMITTED_SUCCESS);
      }
      group->successed();
    }
    else
    {
      log_debug("request committed to cluster %s failed, rcode; %d",
          group->get_cluster_name().c_str(), rcode);
      wrapper->set_request_status(COMMITTED_FAILED);
      if (rcode == TAIR_RETURN_TIMEOUT)
      {
        group->failed();
      }
      dump_key("callback_failed", wrapper->get_shared_info());
    }
    //release wrapper
    log_debug("release wrapper by cluster %s", group->get_cluster_name().c_str());
    delete wrapper;
  }

  std::string RequestProcessor::obtain_ds_addr(PacketWrapper *wrapper)
  {
    std::string ds = "none.";
    tair_client_impl *client = wrapper->get_group()->get_tair_client();
    request_inval_packet *req = wrapper->get_shared_info()->packet;
    if (client != NULL && req != NULL)
    {
      data_entry *key = NULL;
      if (req->key != NULL)
      {
        key = req->key;
      }
      else if (req->key_list != NULL)
      {
        tair_dataentry_set::iterator it = req->key_list->begin();
        if (it != req->key_list->end())
        {
          key = *(it);
        }
      }

      if (key != NULL)
      {
        std::vector<std::string> servers;
        client->get_server_with_key(*key, servers);
        if (servers.size() > 0)
        {
          ds = servers[0];
        }
        else
        {
          ds = "none, servers is empty.";
        }
      }
      else
      {
        ds = "none, key is null.";
      }
    }
    return ds;
  }
  void RequestProcessor::end_request(PacketWrapper *wrapper)
  {
    if (dump_key_switch)
    {
      dump_key("end_request", wrapper->get_shared_info());
    }
    //if the request' status is COMMITTED_SUCCESS, to do nothing here.
    if (wrapper->get_request_status() == COMMITTED_FAILED)
    {
      //retry request.
      SharedInfo *old_shared = wrapper->get_shared_info();
      request_inval_packet *req = old_shared->packet;
      std::string ds = obtain_ds_addr(wrapper);
      old_shared->packet = NULL;
      int retry_times = old_shared->get_retry_times();
      if (retry_times < InvalRetryThread::RETRY_COUNT)
      {
        log_error("REQUEST FAILED, RETRY, retry_times: %d, cluster name: %s, group name: %s, pcode: %d, ds: %s",
            retry_times, wrapper->get_group()->get_cluster_name().c_str(),
            wrapper->get_group()->get_group_name().c_str(), req->getPCode(), ds.c_str());
        SharedInfo *new_shared = new SharedInfo(0, 0, req);
        new_shared->set_retry_times(retry_times);
        //request packet was released by `shared, while the request's status is equ. to COMMITTED_SUCCESS.
        retry_thread->add_packet(new_shared, retry_times);
        //`old_shared should not be released here, and it will be released by is wrapper.
      }
      //cache the request packet.
      else
      {
        log_error("REQUEST FAILED, HOLD, cluster name: %s, group name: %s, pcode: %d, ds: %s",
            wrapper->get_group()->get_cluster_name().c_str(), wrapper->get_group()->get_group_name().c_str(),
            req->getPCode(), ds.c_str());
        //change the `shared status, the request packet hold by `shared will not be released by disconstructor of `shared.
        old_shared->set_request_status(CACHED_IN_STORAGE);
        //statistic
        TAIR_INVAL_STAT.statistcs(pcode_opname_map[req->getPCode()], std::string(req->group_name),
            req->area, inval_area_stat::FINALLY_EXEC);
        //cache request packet
        request_storage->write_request(req);
      }
    }
  }

  void RequestProcessor::send_return_packet(PacketWrapper *wrapper, const int ret, const char *msg)
  {
    if (wrapper->get_packet()->get_direction() == DIRECTION_RECEIVE)
    {
      tair_packet_factory::set_return_packet(wrapper->get_packet(), ret, msg, 0);
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
            group->get_cluster_name().c_str());
        vector<std::string> servers;
        tair_client->get_server_with_key(*(wrapper->get_key()), servers);
        if (!servers.empty())
        {
          log_error("failed to send request to data server: %s, group name: %s",
              servers[0].c_str(), group->get_group_name().c_str());
        }
        wrapper->set_request_status(COMMITTED_FAILED);
        dump_key("failed_send_request", wrapper->get_shared_info());
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
      log_error("wrapper without tair_client, cluster name: %s, group name: %s",
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
      log_error("wrapper is null.");
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
        dump_key("failed_send_request", wrapper->get_shared_info());
        log_debug("send request to cluster %s failed.",
            wrapper->get_group()->get_cluster_name().c_str());
        vector<std::string> servers;
        tair_client_impl *client = wrapper->get_tair_client();
        tair_dataentry_set *keys = wrapper->get_keys();
        if (keys == NULL || keys->empty())
        {
          //bug: request without any key.
          log_error("request without any key, cluster name: %s, group name: %s, pcode: %d",
              wrapper->get_group()->get_cluster_name().c_str(),
              wrapper->get_group()->get_group_name().c_str(), wrapper->get_packet()->getPCode());
        }
        else
        {
          client->get_server_with_key(**(keys->begin()), servers);
          if (!servers.empty())
          {
            log_error("failed to send request to data server: %s, group name: %s",
                servers[0].c_str(), wrapper->get_packet()->get_group_name());
          }
          wrapper->set_request_status(COMMITTED_FAILED);
        }
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
      log_error("wrapper without tair_client, cluster name: %s, group name: %s",
          wrapper->get_group()->get_cluster_name().c_str(),
          wrapper->get_group()->get_group_name().c_str());
      delete wrapper;
    }
  }

  void RequestProcessor::do_dump_key(data_entry *key, const char *msg)
  {
    char* data = util::string_util::bin2ascii(key->get_data(), key->get_size(), NULL, 0);
    if (data != NULL)
    {
      log_warn("DUMPMSG: %s, KEY: %s", msg, data);
      free(data);
    }
  }

  void RequestProcessor::dump_key(const std::string &tag, SharedInfo *shared)
  {
    request_inval_packet *req = shared->packet;
    if (req != NULL)
    {
      int pcode = req->getPCode();
      if (req->key_count == 1)
      {
       //single key
        char msg[256];
        snprintf(msg, sizeof(msg), "tag: %s, request type: %d, group: %s, area: %d, stauts: %d, retry times: %d", 
            tag.c_str(), pcode, req->group_name, req->area, shared->get_request_status(), shared->get_retry_times());
        do_dump_key(req->key, msg);
      }
      else
      {
        //multi keys
        tair_dataentry_set* keys = req->key_list;
        char msg[256];
        int idx = 0;
        for (tair_dataentry_set::const_iterator it = keys->begin(); it != keys->end(); it++)
        {
          data_entry *key = *it;
          if (key != NULL)
          {
            snprintf(msg, sizeof(msg), "tag: %s, request type: %d, group: %s, area: %d, status: %d, retry times: %d, key[%d]",
                tag.c_str(), req->getPCode(), req->group_name, req->area, shared->get_request_status(), shared->get_retry_times(), idx++);
            do_dump_key(key, msg);
          }
        }
      }
    }
    else
    {
      log_error("request without packet!");
    }
  }
  void RequestProcessor::set_dumpkey_switch(bool on_or_off)
  {
    dump_key_switch = on_or_off;
  }
}
