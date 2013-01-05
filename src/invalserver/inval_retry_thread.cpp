#include "inval_retry_thread.hpp"
#include "inval_request_packet_wrapper.hpp"
#include "inval_group.hpp"
namespace tair {
  InvalRetryThread::InvalRetryThread()
  {
    invalid_loader = NULL;
    setThreadCount(RETRY_COUNT);
    request_storage = NULL;
  }

  InvalRetryThread::~InvalRetryThread()
  {
  }

  void InvalRetryThread::setThreadParameter(InvalLoader* invalid_loader, InvalRequestStorage * request_storage)
  {
    this->invalid_loader = invalid_loader;
    this->request_storage = request_storage;
  }

  void InvalRetryThread::stop()
  {
    CDefaultRunnable::stop();
    for (int i = 0; i < RETRY_COUNT; ++i)
    {
      queue_cond[i].broadcast();
    }
  }

  void InvalRetryThread::do_retry_commit_request(PacketWrapper *wrapper, int operation_type, bool merged)
  {
    int ret = TAIR_RETURN_SUCCESS;
    request_inval_packet *packet = NULL;
    if ((packet = wrapper->get_packet()) == NULL)
    {
      log_error("FATAL ERROR, packet is null.");
      ret = TAIR_RETURN_FAILED;
    }

    vector<TairGroup*>* groups = NULL;
    if (ret == TAIR_RETURN_SUCCESS && (groups = invalid_loader->find_groups(packet->group_name)) == NULL)
    {
      log_error("FATAL ERROR, can't find the group according the group name: %s", packet->group_name);
      ret = TAIR_RETURN_FAILED;
    }

    SharedInfo *shared = NULL;
    if (ret == TAIR_RETURN_SUCCESS && (shared = wrapper->get_shared_info()) == NULL)
    {
      log_error("FATAL ERROR, sharedinfo is null in the wrapper.");
      ret = TAIR_RETURN_FAILED;
    }

    if (ret == TAIR_RETURN_SUCCESS)
    {
      log_debug("retry the request, group name: %s, current retry times: %d", packet->group_name,
          wrapper->get_retry_times());
      shared->set_request_reference_count(groups->size());
      for (size_t i = 0; i < groups->size(); ++i)
      {
        (*groups)[i]->retry_commit_request(wrapper, merged);
      }

      TAIR_INVAL_STAT.statistcs(operation_type, std::string(packet->group_name),
          packet->area, inval_area_stat::RETRY_EXEC);
    }
  }

  void InvalRetryThread::run(tbsys::CThread *thread, void *arg)
  {
    int index = (int)((long)arg);
    if (index < 0 || index >= RETRY_COUNT || invalid_loader == NULL)
    {
      return ;
    }
    log_warn("RetryThread %d starts.", index);

    tbsys::CThreadCond *cur_cond = &(queue_cond[index]);
    std::queue<PacketWrapper*> *cur_queue = &(retry_queue[index]);

    int delay_time = index * 3 + 1;

    PacketWrapper *wrapper = NULL;
    while (!_stop)
    {
      cur_cond->lock();
      //~ wait until request is available, or stopped.
      while (!_stop && cur_queue->size() == 0)
      {
        cur_cond->wait();
      }
      if (_stop)
      {
        cur_cond->unlock();
        break;
      }

      wrapper = cur_queue->front();
      cur_queue->pop();
      cur_cond->unlock();
      int pc = wrapper->get_packet()->getPCode();

      int towait = wrapper->get_packet()->request_time + delay_time - time(NULL);
      if (towait > 0)
      {
        TAIR_SLEEP(_stop, towait);
        if (_stop)
          break;
      }

      switch (pc)
      {
        case TAIR_REQ_INVAL_PACKET:
          {
            do_retry_commit_request(wrapper, InvalStatHelper::INVALID, /*merged =*/ false);
            break;
          }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          {
            do_retry_commit_request(wrapper, InvalStatHelper::HIDE, /*merged =*/ false);
            break;
          }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          {
            do_retry_commit_request(wrapper, InvalStatHelper::PREFIX_HIDE, /*merged =*/ true);
            break;
          }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          {
            do_retry_commit_request(wrapper, InvalStatHelper::PREFIX_HIDE, /*merged =*/ true);
            break;
          }
        default:
          {
            log_error("unknown packet with code %d", pc);
            break;
          }
      }
      //release the wrapper
      delete wrapper;
    }
    //~ clear the queue when stopped.
    cur_cond->lock();
    while (cur_queue->size() > 0)
    {
      //write to request_storage
      PacketWrapper *wrapper = cur_queue->front();
      cur_queue->pop();
      if (wrapper != NULL && wrapper->get_packet() != NULL)
      {
        wrapper->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request((base_packet*)wrapper->get_packet());
        delete wrapper;
      }
    }
    cur_cond->unlock();
    log_warn("RetryThread %d is stopped", index);
  }

  void InvalRetryThread::add_packet(PacketWrapper *wrapper, int index)
  {
    if (index < 0 || index > RETRY_COUNT)
    {
      log_error("FATAL ERROR, index: %d, mast be in the range of [0, %d]", index, RETRY_COUNT);
      return;
    }
    queue_cond[index].lock();
    //the request will be write to `request_storage.
    //1)  the retry's queue is overflowed;
    //2)  the retry times were enough.
    //3)  the retry thread were stop;
    if ((int)retry_queue[index].size() >= MAX_QUEUE_SIZE
        || index >= RETRY_COUNT || _stop)
    {
      queue_cond[index].unlock();
      std::string res;
      if (_stop)
      {
        res = "retry threads were stop";
      }
      else if (index >= RETRY_COUNT)
      {
        res = "retry times was sufficient";
      }
      else
      {
        res = "the retry thread's queue has overflowed";
      }
        log_error("ERROR, request failed, retry_times: %d, reason: %s, and write the request packet to `request_storage",
            index, res.c_str());
      if (wrapper != NULL && wrapper->get_packet() != NULL)
      {
        log_debug("write request to the `request_storage, retry_times: %d", index);
        wrapper->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request((base_packet*)wrapper->get_packet());
        //just release the wrapper, the request packet will be released by `request_storage.
        delete wrapper;
      }
      return ;
    }
    log_warn("add packet to RetryThread %d, group name: %s", index, wrapper->get_packet()->group_name);
    wrapper->get_packet()->request_time = time(NULL);
    retry_queue[index].push(wrapper);
    queue_cond[index].unlock();
    queue_cond[index].signal();
  }

  int InvalRetryThread::retry_queue_size(const int index)
  {
    int size = 0;
    if (index < 0 || index >= RETRY_COUNT || _stop == true)
    {
      log_error("failed to retrieve retry_queue_size");
      return 0;
    }
    queue_cond[index].lock();
    size = retry_queue[index].size();
    queue_cond[index].unlock();

    return size;
  }
}
