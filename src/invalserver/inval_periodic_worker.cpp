#include "inval_periodic_worker.hpp"
  namespace tair {
    PeriodicTask::PeriodicTask(std::string task_name, int periodic_time)
    {
      this->task_name = task_name;
      this->periodic_time = periodic_time;
    }

    PeriodicTask::~PeriodicTask()
    {
    }

    void PeriodicTask::start()
    {
    }

    void PeriodicTask::stop()
    {
    }

    void PeriodicTask::runTimerTask()
    {
    }

    PeriodicTaskWorker::PeriodicTaskWorker()
    {
      timer_ptr = new tbutil::Timer();
    }

    PeriodicTaskWorker::~PeriodicTaskWorker()
    {
      if (timer_ptr != 0)
      {
        stop();
      }
    }

    void PeriodicTaskWorker::regist_task(PeriodicTask *task)
    {
      if (task != NULL)
      {
        periodic_task_map.insert(periodic_task_map_t::value_type(task->get_task_name(), task));
      }
    }

    void PeriodicTaskWorker::stop()
    {
      //smart pointer
      if (timer_ptr != 0)
      {
        //NOTE: do not release the PeriodictTask instance by timer. those instances were maintainted by `inval_server
        timer_ptr = 0;
      }
    }
    void PeriodicTaskWorker::start()
    {
      for (periodic_task_map_t::iterator i = periodic_task_map.begin(); i != periodic_task_map.end(); ++i)
      {
        if (i->second != NULL)
        {
        int ret = timer_ptr->scheduleRepeated(i->second, tbutil::Time::seconds(i->second->get_periodic_time()));
        log_info("start periodic task: %s, periodic time: %d, result: %d",
            i->second->get_task_name().c_str(), i->second->get_periodic_time(), ret);
        }
      }
    }
  } // end tair namespace
