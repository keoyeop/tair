#include "inval_group.hpp"
#include "inval_processor.hpp"
#include "tair_client_api_impl.hpp"
namespace tair
{
  const size_t TairGroup::DATA_ITEM_SIZE = 10;
  TairGroup::TairGroup(uint64_t master,
      uint64_t slave,
      const std::string &this_group_name,
      tair_client_impl *tair_client)
    : group_name(this_group_name)
  {
    this->master = master;
    this->slave = slave;
    this->tair_client = tair_client;
    tail = -1;
    set_request_timeout_count_limit(0);
    set_uninvoked_callback_count_limit(0);
  }

  TairGroup::~TairGroup()
  {
    if(tair_client != NULL)
    {
      tair_client->close();
      delete tair_client;
    }
  }

  void TairGroup::do_commit_request(PacketWrapper *wrapper)
  {
    if (is_healthy())
    {
      //send the request to dataserver.
      REQUEST_PROCESSOR.process(wrapper);
    }
    else
    {
      //push request packet to retry_thread's queue, and not to return packet to client,
      //because the return packet had sent to client before the request packet pushed into retry_thread's queue.
      wrapper->set_needed_return_packet(false);
      REQUEST_PROCESSOR.process_failed_request(wrapper);
    }
  }

  void TairGroup::do_process_merged_keys(PROC_FUNC_T func, SharedInfo *shared, bool need_return_packet)
  {
    request_inval_packet *req = shared->packet;
    tair_dataentry_set *keys = req->key_list;
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
      //bug: request packet without any key.
    }
  }

  void TairGroup::commit_request(SharedInfo *shared, bool merged, bool need_return_packet)
  {
    if (is_healthy())
    {
      //send request to data server by `process
      process_commit_request(&RequestProcessor::process, shared, merged, need_return_packet);
    }
    else
    {
      //change the reuqest's state, and add request to the `retry_thread's queue
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
  void TairGroup::sampling()
  {
    size_t idx = ++tail;
    if (idx >= DATA_ITEM_SIZE)
    {
      idx = 0;
      tail = 0;
    }
    //sampling data
    atomic_set(&uninvoked_callback_sampling_data[idx], atomic_read(&uninvoked_callback_count));
    atomic_set(&request_timeout_sampling_data[idx], atomic_read(&request_timeout_count));
    reset_request_timeout_count();

    //caculate the average value
    int uninvoked_callback_count_temp = 0;
    int timeout_count_temp = 0;
    for (size_t i = 0; i < DATA_ITEM_SIZE; i++)
    {
      uninvoked_callback_count_temp += atomic_read(&uninvoked_callback_sampling_data[i]);
      timeout_count_temp += atomic_read(&request_timeout_sampling_data[i]);
    }
    uninvoked_callback_count_temp /= DATA_ITEM_SIZE;
    timeout_count_temp /= DATA_ITEM_SIZE;

    if (uninvoked_callback_count_temp > atomic_read(&uninvoked_callback_count_limit)
        || timeout_count_temp > atomic_read(&request_timeout_count_limit))
    {
      atomic_set(&healthy, SICK);
    }
    else
    {
      atomic_set(&healthy, HEALTHY);
    }
  }
}
