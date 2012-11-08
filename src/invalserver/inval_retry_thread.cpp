#include "inval_retry_thread.hpp"
#include "inval_request_packet_wrapper.hpp"
namespace tair {
  InvalRetryThread::InvalRetryThread() {
    invalid_loader = NULL;
    processor = NULL;
    setThreadCount(RETRY_COUNT);
    request_storage = NULL;
  }

  InvalRetryThread::~InvalRetryThread() {
  }

  void InvalRetryThread::setThreadParameter(InvalLoader *loader, RequestProcessor *processor,
      inval_request_storage * requeststorage) {
    this->invalid_loader = loader;
    this->processor = processor;
    this->request_storage = requeststorage;
  }

  void InvalRetryThread::stop() {
    CDefaultRunnable::stop();
    for (int i = 0; i < RETRY_COUNT; ++i) {
      queue_cond[i].broadcast();
    }
  }

  void InvalRetryThread::run(tbsys::CThread *thread, void *arg) {
    int index = (int)((long)arg);
    if (index < 0 || index >= RETRY_COUNT || invalid_loader == NULL ) {
      return ;
    }
    log_warn("RetryThread %d starts.", index);

    tbsys::CThreadCond *cur_cond = &(queue_cond[index]);
    std::queue<request_inval_packet_wrapper*> *cur_queue = &(retry_queue[index]);

    int delay_time = index * 3 + 1;

    while (!_stop) {
      request_inval_packet_wrapper *wrapper = NULL;
      cur_cond->lock();
      //~ wait until request is available, or stopped.
      while (!_stop && cur_queue->size() == 0) {
        cur_cond->wait();
      }
      if (_stop) {
        cur_cond->unlock();
        break;
      }

      wrapper = cur_queue->front();
      cur_queue->pop();
      cur_cond->unlock();
      int pc = wrapper->packet->getPCode();

      int towait = wrapper->packet->request_time + delay_time - time(NULL);
      if (towait > 0) {
        TAIR_SLEEP(_stop, towait);
        if (_stop)
          break;
      }

      switch (pc) {
        case TAIR_REQ_INVAL_PACKET:
          {
            processor->process((request_invalid*)wrapper->packet, (request_inval_packet_wrapper*)wrapper);
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::INVALID, std::string(wrapper->packet->group_name),
                wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        case TAIR_REQ_HIDE_BY_PROXY_PACKET:
          {
            processor->process((request_hide_by_proxy*)wrapper->packet, (request_inval_packet_wrapper*)wrapper);
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::HIDE, std::string(wrapper->packet->group_name),
                wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
          {
            processor->process((request_prefix_hides_by_proxy*)wrapper->packet, (request_inval_packet_ex_wrapper*)wrapper);
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_HIDE, std::string(wrapper->packet->group_name),
                wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        case TAIR_REQ_PREFIX_INVALIDS_PACKET:
          {
            processor->process((request_prefix_invalids*)wrapper->packet, (request_inval_packet_ex_wrapper*)wrapper);
            TAIR_INVAL_STAT.statistcs(inval_stat_helper::PREFIX_INVALID, std::string(wrapper->packet->group_name),
                wrapper->packet->area, inval_area_stat::RETRY_EXEC);
            break;
          }
        default:
          {
            log_error("unknown packet with code %d", pc);
            wrapper->release_packet();
            break;
          }
      }
    }
    //~ clear the queue when stopped.
    cur_cond->lock();
    while (cur_queue->size() > 0) {
      //write to request_storage
      request_inval_packet_wrapper *wrapper = cur_queue->front();
      cur_queue->pop();
      if (wrapper != NULL && wrapper->packet != NULL) {
        wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED_STORAGE_QUEUE);
        request_storage->write_request((base_packet*)wrapper->packet);
      }
    }
    cur_cond->unlock();
    log_warn("RetryThread %d is stopped", index);
  }

  void InvalRetryThread::add_packet(request_inval_packet_wrapper *wrapper, int index) {
    if (index < 0 || index > RETRY_COUNT - 1 || _stop == true) {
      log_error("add_packet failed: index: %d, _stop: %d", index, _stop);
      if (wrapper != NULL && wrapper->packet != NULL) {
        wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED_STORAGE_QUEUE);
        request_storage->write_request((base_packet*)wrapper->packet);
      }
    }
    queue_cond[index].lock();
    if (retry_queue[index].size() >= MAX_QUEUE_SIZE) {
      queue_cond[index].unlock();
      log_error("[ERROR] Retry Queue %d has overflowed, packet is pushed into request_storage.", index);
      if (wrapper != NULL && wrapper->packet != NULL) {
        wrapper->set_failed_flag(request_inval_packet_wrapper::FAILED_STORAGE_QUEUE);
        request_storage->write_request((base_packet*)wrapper->packet);
      }
      //here, will not release the wrapper.
      return ;
    }
    log_debug("add packet to RetryThread %d", index);
    wrapper->packet->request_time = time(NULL);
    retry_queue[index].push(wrapper);
    queue_cond[index].unlock();
    queue_cond[index].signal();
  }

  int InvalRetryThread::retry_queue_size(const int index) {
    int size = 0;
    if (index < 0 || index >= RETRY_COUNT || _stop == true) {
      log_error("failed to retrieve retry_queue_size");
      return 0;
    }
    queue_cond[index].lock();
    size = retry_queue[index].size();
    queue_cond[index].unlock();

    return size;
  }
}
