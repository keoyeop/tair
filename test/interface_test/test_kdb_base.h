#include <gtest/gtest.h>
#include <tbsys.h>
#include "data_entry.hpp"

using namespace tair::common; 

class TestKdbBase: public ::testing::Test
{
  public:
    TestKdbBase()
    {
      if(TBSYS_CONFIG.load("kdb_test.conf") == EXIT_FAILURE){
        TBSYS_LOG(ERROR, "read config file error: %s", "kdb_test.conf");
        exit(0);
      }
    }

    ~TestKdbBase()
    {
    }

    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }

    static bool compareDataValue(const data_entry& v1, const data_entry& v2)
    {
      if (v1.get_size() != v2.get_size()) return false;
      return memcmp(v1.get_data(), v2.get_data(), v1.get_size()) == 0;
    }

    static bool compareDataValueWithMeta(const data_entry& v1, const data_entry& v2)
    {
      if (compareDataValue(v1, v2) == false) return false;
      return memcmp(&v1.data_meta, &v2.data_meta, sizeof(v1.data_meta));
    }

  protected:
};

class TestKdbData
{
  public:
    void set_test_data()
    {
      
    }

    data_entry * get_test_key(int index)
    {
      return keys + index;
    }

    data_entry * get_test_value(int index)
    {
      return values + index;
    }
 
 private:
    data_entry keys[100];
    data_entry values[100];
};

