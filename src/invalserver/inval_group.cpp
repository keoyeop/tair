#include "inval_group.hpp"
#include "inval_processor.hpp"
#include "tair_client_api_impl.hpp"
#include "inval_request_packet_wrapper.hpp"
namespace tair
{
  TairGroup::TairGroup(const std::string &cluster_name,
      uint64_t master,
      uint64_t slave,
      const std::string &this_group_name)
    : group_name(this_group_name)
  {
    this->master = master;
    this->slave = slave;
    this->cluster_name = cluster_name;
    //default values.
    atomic_set(&failed_count, 0);
    atomic_set(&max_failed_count, DEFAULT_MAX_FAILED_COUNT);
    connected = false;
  }

  TairGroup::~TairGroup()
  {
    if(tair_client != NULL)
    {
      tair_client->close();
      delete tair_client;
    }
  }

  void TairGroup::do_process_merged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet)
  {
    request_inval_packet *req = shared->packet;
    tair_dataentry_set *keys = new tair_dataentry_set();
    if (req->key != NULL)
    {
      keys->insert(req->key);
    }
    else if(req->key_list != NULL)
    {
      tair_dataentry_set &key_list = *(req->key_list);
      for (tair_dataentry_set::iterator it = key_list.begin(); it != key_list.end(); ++it)
      {
        keys->insert(*it);
      }
    }
    else
    {
      //bug: request packet without any key, should never be here.
      log_error("FATAL ERORR, cluster name: %s, group name: %s, request packet without any key, key's count: %d",
          get_cluster_name().c_str(), get_group_name().c_str(), req->key_count);
    }
    MultiWrapper *wrapper = new MultiWrapper(this, shared, keys);
    wrapper->set_needed_return_packet(need_return_packet);
    (REQUEST_PROCESSOR.*func)(wrapper);
  }

  void TairGroup::do_process_unmerged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet)
  {
    request_inval_packet *req = shared->packet;
    if (req->key_count == 1)
    {
      SingleWrapper *wrapper = new SingleWrapper(this, shared, req->key);
      wrapper->set_needed_return_packet(need_return_packet);
      (REQUEST_PROCESSOR.*func)(wrapper);
    }
    else if (req->key_count > 1)
    {
      for (tair_dataentry_set::iterator it = req->key_list->begin(); it != req->key_list->end(); ++it)
      {
        SingleWrapper *wrapper = new SingleWrapper(this, shared, *it);
        wrapper->set_needed_return_packet(need_return_packet);
        (REQUEST_PROCESSOR.*func)(wrapper);
      }
    }
    else
    {
      //bug: request packet without any key, should never be here.
      log_error("FATAL ERORR, cluster name: %s, group name: %s, request packet without any key, key's count: %d",
          get_cluster_name().c_str(), get_group_name().c_str(), req->key_count);
    }
  }

  void TairGroup::commit_request(SharedInfo *shared, bool merged, bool need_return_packet)
  {
    if (is_healthy())
    {
      //send request to data server by `process
      log_debug("cluster %s is healthy", cluster_name.c_str());
      process_commit_request(&RequestProcessor::process, shared, merged, need_return_packet);
    }
    else
    {
      //change the reuqest's state, and add request to the `retry_thread's queue
      log_warn("cluster %s is sick, group name: %s.", cluster_name.c_str(), group_name.c_str());
      process_commit_request(&RequestProcessor::process_failed_request, shared, merged, need_return_packet);
    }
  }

  void TairGroup::process_commit_request(PROC_FUNC_T func, SharedInfo *shared, bool merged, bool need_return_packet)
  {
    if (merged)
    {
      do_process_merged_keys(func, shared, need_return_packet);
    }
    else
    {
      do_process_unmerged_keys(func, shared, need_return_packet);
    }
  }

  //sample data, that indicates the state of group.
  //invoked by `inval_loader at regular intervals.
  void TairGroup::sampling(bool connected)
  {
    if (!is_healthy())
    {
      if (connected)
        atomic_set(&failed_count, 0);
    }
  }
}
