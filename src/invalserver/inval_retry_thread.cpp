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

    void InvalRetryThread::do_retry_commit_request(SharedInfo *shared, int factor, int operation_type, bool merged)
    {
      int ret = TAIR_RETURN_SUCCESS;
      request_inval_packet *packet = NULL;
      if ((packet = shared->packet) == NULL)
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

      if (ret == TAIR_RETURN_SUCCESS)
      {
        shared->inc_retry_times();
        shared->set_request_reference_count(groups->size() * factor);
        for (size_t i = 0; i < groups->size(); ++i)
        {
          (*groups)[i]->commit_request(shared, merged, false);
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
      std::queue<SharedInfo*> *cur_queue = &(retry_queue[index]);

      int delay_time = index * 3 + 1;

      SharedInfo *shared = NULL;
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

        shared = cur_queue->front();
        cur_queue->pop();
        cur_cond->unlock();
        int pc = shared->packet->getPCode();

        int towait = shared->packet->request_time + delay_time - time(NULL);
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
              do_retry_commit_request(shared, shared->packet->key_count, InvalStatHelper::INVALID, /*merged =*/ false);
              break;
            }
          case TAIR_REQ_HIDE_BY_PROXY_PACKET:
            {
              do_retry_commit_request(shared, shared->packet->key_count, InvalStatHelper::HIDE, /*merged =*/ false);
              break;
            }
          case TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET:
            {
              do_retry_commit_request(shared, 1, InvalStatHelper::PREFIX_HIDE, /*merged =*/ true);
              break;
            }
          case TAIR_REQ_PREFIX_INVALIDS_PACKET:
            {
              do_retry_commit_request(shared, 1, InvalStatHelper::PREFIX_HIDE, /*merged =*/ true);
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
        SharedInfo *shared = cur_queue->front();
        cur_queue->pop();
        if (shared!= NULL && shared->packet != NULL)
        {
          shared->set_request_status(CACHED_IN_STORAGE);
          request_storage->write_request((base_packet*)shared->packet);
          delete shared;
        }
      }
      cur_cond->unlock();
      log_warn("RetryThread %d is stopped", index);
    }
    void InvalRetryThread::cache_request_packet(SharedInfo *shared)
    {
      //the request will be write to `request_storage.
      //1)  the retry's queue is overflowed;
      //2)  the retry times were enough.
      //3)  the retry threads were stop;
      if (shared != NULL && shared->packet != NULL)
      {
        shared->set_request_status(CACHED_IN_STORAGE);
        request_storage->write_request((base_packet*)shared->packet);
        delete shared;
      }
    }

    void InvalRetryThread::add_packet(SharedInfo *shared, int index)
    {
      if (index < 0 || index >= RETRY_COUNT)
      {
        log_error("FATAL ERROR, should not be here, index: %d, mast be in the range of [0, %d]", index, RETRY_COUNT);
      }
      else
      {
        queue_cond[index].lock();
        if ((int)retry_queue[index].size() >= MAX_QUEUE_SIZE || _stop)
        {
          queue_cond[index].unlock();
          cache_request_packet(shared);
        }
        else
        {
          shared->packet->request_time = time(NULL);
          retry_queue[index].push(shared);
          queue_cond[index].unlock();
          queue_cond[index].signal();
        }
      }
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

    std::string InvalRetryThread::get_info()
    {
      return "none implementation";
    }
  }
