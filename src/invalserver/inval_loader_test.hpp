
#ifndef INVAL_LOADER_TEST_H
#define INVAL_LOADER_TEST_H

#include "inval_loader.hpp"

namespace tair {

  class inval_loader_test : public tair::InvalLoader {
  public:
    inval_loader_test(bool this_faild_abtain_group_names, bool this_faild_startup);
    ~inval_loader_test();
  private:
    void load_group_name();
    bool failed_obtain_group_names;
    bool failed_startup;
  };
}

#endif
