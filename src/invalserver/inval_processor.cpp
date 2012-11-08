#include "inval_processor.hpp"
#include "inval_retry_thread.hpp"
#include "inval_stat.hpp"
#include "inval_request_storage.hpp"
#include "base_packet.hpp"
namespace tair {
  //map pcode to operation_name
  std::map<int, int> RequestProcessor::pcode_opname_map;
  tair_packet_factory RequestProcessor::packet_factory;

  //constructor
  RequestProcessor::RequestProcessor(InvalLoader *invalid_loader)
  {
    this->invalid_loader = invalid_loader;
    pcode_opname_map[TAIR_REQ_INVAL_PACKET] = inval_stat_helper::INVALID;
    pcode_opname_map[TAIR_REQ_PREFIX_INVALIDS_PACKET] = inval_stat_helper::PREFIX_INVALID;
    pcode_opname_map[TAIR_REQ_HIDE_BY_PROXY_PACKET] = inval_stat_helper::HIDE;
    pcode_opname_map[TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET] = inval_stat_helper::PREFIX_HIDE;
  }

  void RequestProcessor::do_rescue_failure(request_inval_packet_wrapper *wrapper)
  {
    //set failed flag
    wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED_RETRY_QUEUE);
    //add the retry_thread
    request_inval_packet *packet = wrapper->packet;
    int pcode = packet->getPCode();
    //retry the operation RETRY_COUNT times.
    if (wrapper->get_retry_times() < InvalRetryThread::RETRY_COUNT) {
      log_debug("Failed to execute the operation: %d, op_type: %d, retry_times: %d",
          pcode_opname_map[pcode], inval_area_stat::RETRY_EXEC,
          wrapper->get_retry_times());
      TAIR_INVAL_STAT.statistcs(pcode_opname_map[pcode], std::string(packet->group_name),
          packet->area, inval_area_stat::RETRY_EXEC);
      //if before packet pushed into the retry's queue, send return packet to client.
      if (wrapper->get_retry_times() == 0
          && wrapper->packet->is_sync == SYNC_INVALID
          && wrapper->packet->get_direction() == DIRECTION_RECEIVE) {
          tair_packet_factory::set_return_packet(wrapper->packet, TAIR_RETURN_SUCCESS,
              "inval server will retry your request", 0);
      }
      wrapper->retry->add_packet(wrapper, wrapper->get_retry_times());
      wrapper->inc_retry_times();
    }
    else {
      log_error("Operation: %d, RetryFailedFinally, add the packet to the request_storage.", pcode_opname_map[pcode]);
      //simply write the request packet to request_storage,
      //and this operation is always successful.
      wrapper->request_storage->write_request((base_packet*)packet);
      //the wrapper will be free.
      wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED_STORAGE_QUEUE);
      TAIR_INVAL_STAT.statistcs(pcode_opname_map[pcode], std::string(packet->group_name),
          packet->area, inval_area_stat::FINALLY_EXEC);
    }
  }

  void RequestProcessor::client_callback_func(int rcode, void *args)
  {
    if (args == NULL) {
      log_error("[FATAL ERROR] callback func's args should not be null.");
      return;
    }

    request_inval_packet_wrapper *wrapper = (request_inval_packet_wrapper*)args;
    if (wrapper == NULL) {
      log_error("[FATAL ERROR] args is demo.");
      return;
    }

    if (wrapper->get_failed_flag() == request_inval_packet_wrapper::SUCCESS) {
      request_inval_packet *packet = (request_inval_packet*) wrapper->packet;
      if (rcode != TAIR_RETURN_SUCCESS && rcode != TAIR_RETURN_DATA_NOT_EXIST) {
        log_error("[FATAL ERROR] The operation has failed, rcode: %d, operation: %d, pcode: %d",
            rcode, pcode_opname_map[packet->getPCode()],packet->getPCode());
        wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED);
      }
      else {
        log_debug("[OK] Success.  reference: %d, pcode: %d, rcode: %d",
            wrapper->get_reference_count(), packet->getPCode(), rcode);
      }
    }

    //decrease the reference count.
    //while reference_cout is equ. to zero, all the callback functions were invoked.
    if (wrapper->dec_and_return_reference_count(1) == 0) {
      if (wrapper->get_failed_flag() != request_inval_packet_wrapper::SUCCESS) {
        //push the request packet into retry_threads's queue , or
        //write the packet to the request_storage.
        log_debug("do_rescure_failure");
        do_rescue_failure(wrapper);
      }
      //send return packet if the `is_sync is set the value SYNC_INVALID.
      //release the packet and its wrapper, while all operations were seccussfully executed.
      log_debug("do_process_ultimate");
      do_process_ultimate(wrapper, rcode);
    }
  }

  void RequestProcessor::client_callback_func(int rcode, const key_code_map_t* key_code_map, void *args)
  {
    if (args == NULL) {
      log_error("[FATAL ERROR] callback func's args should not be null.");
      return;
    }

    request_inval_packet_ex_wrapper *wrapper = (request_inval_packet_ex_wrapper*)args;
    if (wrapper == NULL) {
      log_error("[FATAL ERROR] args is demo.");
      return;
    }

    if (wrapper->get_failed_flag() == request_inval_packet_wrapper::SUCCESS) {
      request_inval_packet *packet = (request_inval_packet*) wrapper->packet;
      if (rcode != TAIR_RETURN_SUCCESS && rcode != TAIR_RETURN_DATA_NOT_EXIST) {
        if (rcode == TAIR_RETURN_PARTIAL_SUCCESS) {
          //here, the failed key(s) be collected
          if (key_code_map == NULL) {
            log_error("[FATAL ERROR], key_code_map is null, but got the PARTIAL_SUCCESS.");
          }
          else {
            log_debug("exist keys failed, pcode: %d", wrapper->packet->getPCode());
            key_code_map_t::const_iterator itr = key_code_map->begin();
            while (itr != key_code_map->end()) {
              if (itr->second != TAIR_RETURN_DATA_NOT_EXIST && itr->second != TAIR_RETURN_DATA_EXPIRED) {
                data_entry *mkey = new data_entry();
                merge_key(wrapper->pkey, *itr->first, *mkey);
                if (wrapper->failed_key_set == NULL) {
                  wrapper->failed_key_set = new tair_dataentry_set();
                }
                if (wrapper->failed_key_set->insert(mkey).second == false) {
                  delete mkey;
                }
              }
              ++itr;
            }
          }
        }
        else {
          log_error("[FATAL ERROR] The operation has failed, rcode: %d, operation: %d, pcode: %d",
              rcode, pcode_opname_map[packet->getPCode()],packet->getPCode());
          wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED);
        }
      }
      else {
        log_debug("[OK] reference: %d, pcode: %d, rcode: %d",
            wrapper->get_reference_count(), packet->getPCode(), rcode);
      }
    }
    //decrease the reference count.
    //while reference_cout is equ. to zero, all the callback functions were invoked.
    if (wrapper->dec_and_return_reference_count(1) == 0) {
      if (wrapper->get_failed_flag() != request_inval_packet_wrapper::SUCCESS) {
        //push the request packet into retry_threads's queue , or
        //write the packet to the request_storage.
        log_debug("do_rescure_failure, pcode: %d", wrapper->packet->getPCode());
        do_rescue_failure(wrapper);
      }
      else {
        //key(s) failed
        if (wrapper->failed_key_set != NULL && wrapper->failed_key_set->size() != 0) {
          log_debug("do_process_failed_keys, pcode: %d", wrapper->packet->getPCode());
          do_process_failed_keys(wrapper, *(wrapper->failed_key_set));
          //not release the key.
          wrapper->failed_key_set->clear();
        }
      }
      //send return packet if the `is_sync is set the value SYNC_INVALID.
      //release the packet and its wrapper, while all operations were seccussfully executed.
      log_debug("do_process_ultimate");
      do_process_ultimate(wrapper, rcode);
    }
  }

  void RequestProcessor::do_process_ultimate(request_inval_packet_wrapper *wrapper, const int ret)
  {
    //release the packet, only if all operations were executed successfully,
    //otherwise, the request packet was added to retry_thread's queue, and
    //do not release it.
    if (wrapper->get_failed_flag() == request_inval_packet_wrapper::SUCCESS) {
      log_debug("release request packet wrapper, pcode: %d, reference_count: %d",
          wrapper->packet->getPCode(), wrapper->get_reference_count());
      if (wrapper->packet->is_sync == SYNC_INVALID  && wrapper->packet->get_direction() == DIRECTION_RECEIVE) {
        tair_packet_factory::set_return_packet(wrapper->packet, ret, "success.", 0);
      }
      //release the packet, and wrapper;
      wrapper->release_packet();
      delete wrapper;
    }
    else if (wrapper->get_failed_flag() == request_inval_packet_wrapper::FAILED_STORAGE_QUEUE) {
      log_debug("only release the wrapper, packet is in the request_stroage_queue.");
      delete wrapper;
    }
  }

  void RequestProcessor::do_process_failed_keys(request_inval_packet_wrapper* wrapper,
      const tair_dataentry_set &failed_key_set)
  {
    log_debug("some keys failed, request packet's pcode: %d", wrapper->packet->getPCode());
    request_inval_packet *req = wrapper->packet;
    request_inval_packet *post_req = (request_inval_packet*)packet_factory.createPacket(req->getPCode());
    post_req->set_group_name(req->group_name);
    post_req->area = req->area;
    tair_dataentry_set::const_iterator itr = failed_key_set.begin();
    while (itr != failed_key_set.end()) {
      //do not release the key in the distructor of class request_inval_packet_wrapper.
      post_req->add_key(*itr++, false);
    }
    //async.
    request_inval_packet_wrapper *new_wrapper = NULL;
    if (post_req->getPCode() == TAIR_REQ_PREFIX_INVALIDS_PACKET
        || post_req->getPCode() == TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET) {
      new_wrapper = new request_inval_packet_ex_wrapper(post_req, wrapper->retry, wrapper->request_storage);
    }
    else {
      new_wrapper = new request_inval_packet_wrapper(post_req, wrapper->retry, wrapper->request_storage);
    }
    wrapper->retry->add_packet(new_wrapper, 0);
    new_wrapper->inc_retry_times();
    //push the post_req to retry_thread's queue.
    TAIR_INVAL_STAT.statistcs(inval_stat_helper::INVALID, std::string(post_req->group_name),
        post_req->area, inval_area_stat::RETRY_EXEC);
  }

  int RequestProcessor::do_process(request_inval_packet *req, PROCESS_RH_FUNC_T pproc, request_inval_packet_wrapper *wrapper)
  {
    int ret = TAIR_RETURN_SUCCESS;
    int failed_counter = 0;
    std::vector<tair_client_impl*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      wrapper->set_failed_flag(request_inval_packet_wrapper::SUCCESS);
      wrapper->set_reference_count(clients->size() * req->key_count);
      log_debug("operation: %d, group name: %s, client's count: %d, key's count: %d",
          pcode_opname_map[req->getPCode()], req->group_name, clients->size(), req->key_count);
      //~ 'set' to collect keys that failed
      tair_dataentry_set failed_key_set;
      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_impl *client = (*clients)[i];
        //~ single key
        if (req->key_count == 1) {
          if ((ret = (client->*pproc)(req->area, *(req->key),
                  client_callback_func, (void*)wrapper)) != TAIR_RETURN_SUCCESS) {
            //if synchronous remove/hide operation failed (the return value is not equ. to TAIR_RETURN_SUCCESS),
            //the callback function will not be invoked.
            std::vector<std::string> servers;
            if (servers.size() > 0) {
              log_error("[FATAL ERROR] RemoveFailure: Group: %s, DataServer: %s",
                  req->group_name, servers[0].c_str());
            }
            data_entry *key = new data_entry(*req->key);
            if (failed_key_set.insert(key).second == false) {
              delete key;
            }
            //if tair_client_impl::remove/hide function returns TAIR_RETURN_INVALID_ARGUMENT/ITMESIZE_ERROR/
            //NONE_DATASERVER, the callback function was not invoked by client,
            //and the reference count of callback_param should be decrease.
            if (ret == TAIR_RETURN_INVALID_ARGUMENT
                || ret == TAIR_RETURN_ITEMSIZE_ERROR
                || ret == TAIR_RETURN_NONE_DATASERVER) {
              log_error("[FATAL ERROR] the callback will not be invoked.");
              //here, we can not decrease the reference count of the wrapper.
              failed_counter++;
              wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED);
            }
          }
          else{
            log_debug("client[%d], ok, operation was executed.", i);
          }
        }
        else if (req->key_count > 1) {
          //~ multiple keys
          for (tair_dataentry_set::iterator it = req->key_list->begin();
              it != req->key_list->end(); ++it) {
            if ((ret = (client->*pproc)(req->area, **it,
                    client_callback_func, (void*)wrapper)) != TAIR_RETURN_SUCCESS) {
              std::vector<std::string> servers;
              client->get_server_with_key(**it, servers);
              log_error("[FATAL ERROR] RemoveFailure: Group: %s, DataServer: %s.",
                  req->group_name, servers[0].c_str());
              data_entry *key = new data_entry(**it);
              if (failed_key_set.insert(key).second == false) {
                delete key;
              }
              //if tair_client_impl::remove/hide function returns TAIR_RETURN_INVALID_ARGUMENT/ITMESIZE_ERROR/
              //NONE_DATASERVER, the callback function was not invoked by client,
              //and the reference count of callback_param should be decrease.
              if (ret == TAIR_RETURN_INVALID_ARGUMENT
                  || ret == TAIR_RETURN_ITEMSIZE_ERROR
                  || ret == TAIR_RETURN_NONE_DATASERVER) {
                failed_counter++;
                wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED);
              }
            }
          }
        }
      }
      if (!failed_key_set.empty()) {
        log_debug("do_process_failed_keys");
        do_process_failed_keys(wrapper, failed_key_set);
        ret = TAIR_RETURN_SUCCESS;
      }
      if (failed_counter > 0) {
        //we will be decrease the reference count.
        //here, wrapper is not free, because the reference_count > 0.
        if (wrapper->dec_and_return_reference_count(failed_counter) == 0) {
          do_rescue_failure(wrapper);
          do_process_ultimate(wrapper, TAIR_RETURN_FAILED);
        }
      }
    }
    else {
      log_error("[FATAL ERROR] cannot find clients, group name: %s, packet's pcode: %d",
          req->group_name, req->getPCode());
      if (wrapper->packet->is_sync == SYNC_INVALID && wrapper->packet->get_direction() == DIRECTION_RECEIVE) {
        char *msg = "cann't find clients.";
        tair_packet_factory::set_return_packet(wrapper->packet, TAIR_RETURN_FAILED, msg, 0);
      }
      wrapper->release_packet();
      delete wrapper;
      ret = TAIR_RETURN_FAILED;
    }
    return ret;
  }

  int RequestProcessor::do_process(request_inval_packet *req, PROCESS_RHS_FUNC_T pproc,
      request_inval_packet_ex_wrapper *wrapper)
  {
    int ret = TAIR_RETURN_SUCCESS;
    int failed_counter = 0;
    vector<tair_client_impl*> *clients = invalid_loader->get_client_list(req->group_name);
    if (clients != NULL) {
      wrapper->set_failed_flag(request_inval_packet_wrapper::SUCCESS);
      wrapper->set_reference_count(clients->size());
      log_debug("operation: %d, group name: %s, client's count: %d, key's count: %d, reference_count: %d",
          pcode_opname_map[req->getPCode()], req->group_name, clients->size(),
          req->key_count, wrapper->get_reference_count());
      //~ 'set' to collect keys that failed
      tair_dataentry_set mkey_set;
      tair_dataentry_set failed_key_set;
      if (req->key != NULL) {
        mkey_set.insert(req->key);
        split_key(req->key, &wrapper->pkey, NULL);
      } else if (req->key_list != NULL) {
        tair_dataentry_set::iterator itr = req->key_list->begin();
        split_key(*itr, &wrapper->pkey, NULL);
        while (itr != req->key_list->end()) {
          mkey_set.insert(*itr);
          ++itr;
        }
      }

      for (size_t i = 0; i < clients->size(); ++i) {
        tair_client_impl *client = clients->at(i);
        if ((ret = (client->*pproc)(req->area, mkey_set, NULL,
                client_callback_func, (void*)wrapper)) != TAIR_RETURN_SUCCESS) {
          //if tair_client_impl::hides function return TAIR_RETURN_INVALID_ARGUMENT/NONE_DATASERVER,
          //the callback function was not invoked by client, and the reference count of `callback_param
          //should be decrease.
          if (ret == TAIR_RETURN_INVALID_ARGUMENT || ret == TAIR_RETURN_NONE_DATASERVER) {
            log_error("[FATAL ERROR] the callback will not be invoked.");
            failed_counter++;
            wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED);
          }
        }
      }

      if (failed_counter > 0) {
        //we will be decrease the reference count.
        //here, wrapper is not free, because the reference_count > 0.
        if (wrapper->dec_and_return_reference_count(failed_counter) == 0) {
          do_rescue_failure(wrapper);
          do_process_ultimate(wrapper, TAIR_RETURN_FAILED);
        }
      }
    }
    else {
      log_error("[FATAL ERROR] cannot find clients, group name: %s, packet's pcode: %d",
          req->group_name, req->getPCode());
      if (wrapper->packet->is_sync == SYNC_INVALID && wrapper->packet->get_direction() == DIRECTION_RECEIVE) {
        char *msg = "cann't find clients.";
        tair_packet_factory::set_return_packet(wrapper->packet, TAIR_RETURN_FAILED, msg, 0);
      }
      wrapper->release_packet();
      delete wrapper;
      ret = TAIR_RETURN_FAILED;
    }
    return ret;
  }
}

