#ifndef INVAL_PERIODIC_WORKER_H
#define INVAL_PERIODIC_WORKER_H
#include <string>
#include <map>
#include <tbsys.h>
#include <tbnet.h>
#include "log.hpp"
#include "Timer.h"
namespace tair {
  class PeriodicTask : public tbutil::TimerTask
  {
  public:
    PeriodicTask(std::string task_name, int periodic_time);
    ~PeriodicTask();
    virtual void start();
    virtual void stop();
    virtual void runTimerTask();

    inline std::string &get_task_name()
    {
      return task_name;
    }

    inline int get_periodic_time()
    {
      return periodic_time;
    }
  protected:
    std::string task_name;
    int periodic_time;
  };

  typedef std::map<std::string, PeriodicTask*> periodic_task_map_t;
  class PeriodicTaskWorker
  {
  public:
    PeriodicTaskWorker();
    ~PeriodicTaskWorker();
    void regist_task(PeriodicTask* task);
    void start();
    void stop();
  protected:
    periodic_task_map_t periodic_task_map;
    tbutil::TimerPtr timer;
  };
}
#endif
