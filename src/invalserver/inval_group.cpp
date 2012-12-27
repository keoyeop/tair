#include "inval_group.hpp"
#include "inval_processor.hpp"
#include "inval_retry_thread.hpp"
#include "tair_client_api_impl.hpp"
namespace tair
{
  const int TairGroup::DATA_ITEM_SIZE = 10;
  TairGroup::TairGroup(uint64_t master, uint64_t slave, const std::string &this_group_name, tair_client_impl *tair_client)
    : group_name(this_group_name)
  {
    this->master = master;
    this->slave = slave;
    this->tair_client = tair_client;
    retry_thread = NULL;
  }

  TairGroup::~TairGroup()
  {
    if(tair_client != NULL)
    {
      tair_client->close();
      delete tair_client;
    }
  }

  bool TairGroup::is_healthy()
  {
    return atomic_read(&healthy) == HEALTHY;
  }

  void TairGroup::do_commit_request(PacketWrapper *wrapper)
  {
    if (is_healthy() == true)
    {
      //send the request to dataserver.
      REQUEST_PROCESSOR.process(wrapper);
    }
    else
    {
      //push wrapper to retry_thread's queue.
      retry_thread->add_packet(wrapper, wrapper->get_retry_times());
    }
  }

  void TairGroup::commit_request(SharedInfo *shared, bool merged, bool need_return_packet)
  {
    request_inval_packet *req = shared->packet;
    if (is_healthy())
    {
        tair_dataentry_set *keys = req->key_list;
        MultiWrapper *wrapper = new MultiWrapper(this, shared, keys);
        //send request to dataserver.
        REQUEST_PROCESSOR.process(wrapper);
    }
    else
    {
        tair_dataentry_set *keys = req->key_list;
        MultiWrapper *wrapper = new MultiWrapper(this, shared, keys);
        //push the request to retry_thread's queue.
        cache_request(wrapper, need_return_packet);
    }
  }

  void TairGroup::sampling()
  {
  }
}
