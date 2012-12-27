#include "inval_retry_thread.hpp"
#include "inval_request_packet_wrapper.hpp"
#include "inval_group.hpp"
namespace tair {
  InvalRetryThread::InvalRetryThread()
  {
    invalid_loader = NULL;
    processor = NULL;
    setThreadCount(RETRY_COUNT);
    request_storage = NULL;
  }

  InvalRetryThread::~InvalRetryThread()
  {
  }

  void InvalRetryThread::setThreadParameter(InvalLoader *loader, RequestProcessor *processor,
      InvalRequestStorage * requeststorage)
  {
    this->invalid_loader = loader;
    this->processor = processor;
    this->request_storage = requeststorage;
  }

  void InvalRetryThread::stop()
  {
    CDefaultRunnable::stop();
    for (int i = 0; i < RETRY_COUNT; ++i)
    {
      queue_cond[i].broadcast();
    }
  }

  void InvalRetryThread::run(tbsys::CThread *thread, void *arg)
  {
    int index = (int)((long)arg);
    if (index < 0 || index >= RETRY_COUNT || invalid_loader == NULL )
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
            SingleWrapper *swrapper = (SingleWrapper*) wrapper;
            request_invalid* packet = (request_invalid*)swrapper->get_packet();
            vector<TairGroup*>* groups = invalid_loader->find_groups(packet->group_name);
            SharedInfo *shared = swrapper->get_shared_info();

            shared->set_request_reference_count(groups->size());
            shared->set_request_delay_count(0);

            for (size_t i = 0; i < groups->size(); ++i)
            {
              (*groups)[i]->commit_request(swrapper->get_key(), shared);
            }

            TAIR_INVAL_STAT.statistcs(InvalStatHelper::INVALID, std::string(packet->group_name),
                packet->area, inval_area_stat::RETRY_EXEC);

            delete swrapper;
            break;
          }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          {
            //TAIR_INVAL_STAT.statistcs(InvalStatHelper::HIDE, std::string(wrapper->packet->group_name),
            //    wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          {
            //TAIR_INVAL_STAT.statistcs(InvalStatHelper::PREFIX_HIDE, std::string(wrapper->packet->group_name),
            //    wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          {
           // TAIR_INVAL_STAT.statistcs(InvalStatHelper::PREFIX_INVALID, std::string(wrapper->packet->group_name),
           //     wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        default:
          {
            log_error("unknown packet with code %d", pc);
            break;
          }
      }
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

  void InvalRetryThread::add_packet(PacketWrapper *wrapper, int index) {
    if (index < 0 || index > RETRY_COUNT - 1 || _stop == true)
    {
    }
    queue_cond[index].lock();
    if ((int)retry_queue[index].size() >= MAX_QUEUE_SIZE)
    {
      queue_cond[index].unlock();
      log_error("[ERROR] Retry Queue %d has overflowed, packet is pushed into request_storage.", index);
      if (wrapper != NULL && wrapper->get_packet() != NULL)
      {
        wrapper->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request((base_packet*)wrapper->get_packet());
        delete wrapper;
      }
      return ;
    }
    log_debug("add packet to RetryThread %d", index);
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
