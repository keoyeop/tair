/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_api.cpp 1245 2013-06-13 11:07:41Z huan.wanghuan@alibaba-inc.com $
 *
 * Authors:
 *   yunhen <huan.wanghuan@alibaba-inc.com>
 *
 */
#include "test_api.hpp"
#include "tair_mc_client_api_test.hpp"

#include <gtest/gtest.h>
#include <DiamondClient.h>
#include <tair_mc_client_api.hpp>
#include <data_entry.hpp>
#include <tblog.h>

#include <string>
#include <cstdlib>
#include <time.h>

using namespace std;
using namespace tair;

DIAMOND::DiamondClient* diamond_client;
DIAMOND::DiamondConfig config;

bool set_diamond_config(const char* data_id, const char* group_id, const char* content)
{
   int i=0;
   while(i<10)
   {
      string tmp;
      diamond_client->getConfig(data_id, group_id, tmp);
         if (tmp == content)
            break;

      if (diamond_client->setConfig(data_id, group_id, content))
         sleep(2);
      i++;
   }
   if (i<10)
   {
      TBSYS_LOG(INFO, "set diamond server %s, %s success", data_id, group_id);
      return true;
   }
   TBSYS_LOG(INFO, "set diamond server %s, %s failed", data_id, group_id);
   return false;
}


class SetupDiamond: public testing::Environment
{
public:
    virtual void SetUp()
    {
       TBSYS_LOG(INFO, "seting environment");
   
       config["diamond.invoke.timeout"] = "2";    // timeout，单位秒
       diamond_client = new DIAMOND::DiamondClient(&config);
       
       TBSYS_LOG(INFO, "seting diaomnd server ok");
       return ;
    }
    virtual void TearDown()
    {
       if (diamond_client)
       {
          delete diamond_client;
          diamond_client = NULL;
       }
       TBSYS_LOG(INFO, "all test is over");
    }
};

class configuration_right: public testing::Test {
 protected:
  static void SetUpTestCase() {
     set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3);
       
     set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right);
       
     set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right);
  }
  static void TearDownTestCase() {
  }
  // Some expensive resource shared by all tests.
};
   
TEST_F(configuration_right, choose_CM3)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("ERROR");
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   tair_mc_client.close();
}

TEST_F(configuration_right, test_device_name)
{

   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_netdevice_test));
   sleep(5);
   
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   
   tair_mc_client.set_net_device_name("eth0");
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   tair_mc_client.close();

   tair_mc_client.set_net_device_name("eth0:0");
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM4", tair_mc_client.get_data_group_id().c_str());
   tair_mc_client.close();

   tair_mc_client.set_net_device_name("lo");
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   tair_mc_client.set_net_device_name("abcxx");
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
}

TEST_F(configuration_right, choose_CM4)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("ERROR");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM4));
   
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM4", tair_mc_client.get_data_group_id().c_str());
   tair_mc_client.close();
}

TEST_F(configuration_right, GROUP_change_real_group_not_change)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3_another));
   sleep(15);
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   tair_mc_client.close();
}

TEST_F(configuration_right, GROUP_change_real_group_change)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   
   // 改变 yunhen-test-GROUP的配置内容
   fprintf(stderr, "\n\nyunhen-test-GROUP changed, from CM3 to CM4\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM4));
   int start_time = time(NULL);
   while("yunhen-test-CM4" != tair_mc_client.get_data_group_id()){}
   int end_time = time(NULL);
   fprintf(stderr, "\n**********group change from CM3 to CM4, it take %ds**********\n", end_time - start_time);
   
   // 再次改变 yunhen-test-GROUP的配置内容
   fprintf(stderr, "\n\nyunhen-test-GROUP changed, from CM4 to CM3\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3_another));
   start_time = time(NULL);
   while("yunhen-test-CM3" != tair_mc_client.get_data_group_id()){}
   end_time = time(NULL);
   fprintf(stderr, "\n**********group change from CM4 to CM3, it take %ds**********\n", end_time - start_time);

   tair_mc_client.close();
}

TEST_F(configuration_right, data_group_change)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   
   // 改变 yunhen-test-CM3 和 yunhen-test-CM4的配置内容
   fprintf(stderr, "\n\n\nnow real_group_id is yunhen-test-CM3, CM3 and CM4 changed\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right_another));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right_another));
   sleep(5);

   // 改变 yunhen-test-GROUP的配置内容
   fprintf(stderr, "\n\n\nnow yunhen-test-GROUP changed, real_group_id change from CM3 to CM4\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM4));
   int start_time = time(NULL);
   while("yunhen-test-CM4" != tair_mc_client.get_data_group_id()){}
   int end_time = time(NULL);
   fprintf(stderr, "**********group change from CM3 to CM4, it take %ds**********\n", end_time - start_time);

   // 改变 yunhen-test-CM3 和 yunhen-test-CM4的配置内容
   fprintf(stderr, "\n\n\nnow real_group_id is yunhen-test-CM4, CM3 and CM4 changed\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right));
   sleep(5);
   
   tair_mc_client.close();
}

TEST_F(configuration_right, data_group_change_another)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   
   // 改变 yunhen-test-CM3 和 yunhen-test-CM4的配置内容
   fprintf(stderr, "\n\nnow real_group_id is yunhen-test-CM3, CM3 and CM4 changed\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM4_right_another));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right_another));
   sleep(5);

   tair_mc_client.close();
}

TEST(configuration_wrong, rule_group_wrong_nofatal)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_wrong_choose_CM3_nofatal));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
}

TEST(configuration_wrong, rule_group_wrong_fatal)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_wrong_choose_CM3_fatal));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));
}

TEST(configuration_wrong, data_group_wrong)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));

   fprintf(stderr, "\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", GROUP_right_choose_CM3));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   fprintf(stderr, "\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_wrong_json_format));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   fprintf(stderr, "\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_wrong_spell));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   fprintf(stderr, "\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_wrong_weight1));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));

   fprintf(stderr, "\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_wrong_weight2));
   ASSERT_FALSE(tair_mc_client.startup("yunhen-test"));
}

TEST(configuration_wrong, rule_group_change_wrong)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());

   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_wrong));
   sleep(5);
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
}

TEST(configuration_wrong, data_group_change_wrong)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());

   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_wrong_spell));
   sleep(5);
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
}

TEST(configuration_wrong, change_to_a_wrong_data_group)
{
   tair::tair_mc_client_api  tair_mc_client;
   tair_mc_client.set_log_level("INFO");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_wrong));
   ASSERT_TRUE(tair_mc_client.startup("yunhen-test"));

   fprintf(stderr, "\n\nstart to change from CM3 to CM4\n");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM4));
   sleep(5);
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
   
   fprintf(stderr, "\n\nCM3 and CM4 changed");
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right_another));
   ASSERT_TRUE(set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right));
   ASSERT_STREQ("yunhen-test-CM3", tair_mc_client.get_data_group_id().c_str());
}

class test_api : public testing::Test {
 protected:
  static void SetUpTestCase() {
     bool res1 = (set_diamond_config("yunhen-test", "yunhen-test-GROUP", GROUP_right_choose_CM3));
     bool res2 = (set_diamond_config("yunhen-test", "yunhen-test-CM3", CM3_right));
     bool res3 = (set_diamond_config("yunhen-test", "yunhen-test-CM4", CM4_right));
     if (!res1 || !res2 || !res3|| !client_test_api.startup("yunhen-test"))
     {
        TBSYS_LOG(ERROR, "startup client_test_api failed");
        exit(4);
     }
  }
  static void TearDownTestCase() {
     client_test_api.close();
  }
  // Some expensive resource shared by all tests.
   static tair_mc_client_test_api client_test_api;
};

tair_mc_client_test_api test_api::client_test_api;

TEST_F(test_api, test_put)
{
   ASSERT_TRUE(test_api::client_test_api.test_put());
}

TEST_F(test_api, test_all)
{
   const vector<func>& func_vec = client_test_api.get_all_func() ;
   for(vector<func>::const_iterator it = func_vec.begin(); it != func_vec.end(); ++it)
   {
      func_ptr_type tmp= (*it).func_ptr;
      EXPECT_TRUE((test_api::client_test_api.*tmp)()) << (*it).func_name;
   }
}

TEST_F(test_api, test_all_with_diamond_change)
{
   const vector<func>& func_vec = client_test_api.get_all_func() ;

   for (int i = 0; i < 100; ++i)
   {
      fprintf(stderr, "the %d time\n", i);
      for(vector<func>::const_iterator it = func_vec.begin(); it != func_vec.end(); ++it)
      {
         func_ptr_type tmp= (*it).func_ptr;
         EXPECT_TRUE((test_api::client_test_api.*tmp)()) << (*it).func_name;
      }
   }
}

int main(int argc, char* argv[]) {

   testing::AddGlobalTestEnvironment(new SetupDiamond);
   testing::InitGoogleTest(&argc, argv);  
  
   // Runs all tests using Google Test.  
   return RUN_ALL_TESTS();  
}  
