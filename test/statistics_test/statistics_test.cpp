#include "tair_client_api.hpp"
#include "inval_stat.hpp"
#include <string>
#include <map>
#include <iomanip>

using namespace std;
using namespace tair;
data_entry* generate_data_entry() {
  static char* cp ="qwertyuiop[]asddfghjkl;lzxcvbnm,./'][;.,lpokmijnuhbygvtfcrdxeszwaq";
  int size = strlen(cp);
  int start = 0;
  int end = 0;
  while(start >= end) {
    start = rand() % size;
    end = rand() % size;
  }
  char *p =cp+start;
  int length = end - start + 1;
  char *d = new char [length+1];
  strncpy(d,p,length);
  d[length] = '\0';

  return new data_entry(d, length, false);
}
//test

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("WARN");
  cout << "==============================TEST INVALID SERVER=========================" << endl;
  string group_name = "fengmao";
  string config1 = "10.232.35.40:50565";
  string config2 = "10.232.35.40:50656";

  tair_client_api* client1 = new tair_client_api();
  if (!client1->startup(config1.c_str(), NULL, group_name.c_str())) {
    cout<<"client1, start up failed"<<endl;
    exit(0);
  }
  client1->set_timeout(1000);
  tair_client_api* client2 = new tair_client_api();
  if (!client2->startup(config2.c_str(), NULL, group_name.c_str())) {
    cout<<"client2, start up failed"<<endl;
    exit(0);
  }
  client2->set_timeout(1000);
  tair_client_api* client = new tair_client_api();
  if (!client->startup(config1.c_str(), NULL, group_name.c_str())) {
    cout<<"invalid client,start up failed"<<endl;
    exit(0);
  }
  client->set_timeout(1000);
  //invalidate
  int ret = -1;
  while(true)
  {
    int in;
    cout << "input:(1- continue, 0- exit)";
    cin >> in;
    if (in == 0) break;
    data_entry *key = generate_data_entry();
    data_entry *value = generate_data_entry();
   ret = client1->put(1, *key, *value, 0, 0);
    if (ret == TAIR_RETURN_SUCCESS) {
      cout <<" client 1: put successfully."<<endl;
    }
    else {
      cout <<"client 1: failed put operation."<<endl;
    }
    ret = client2->put(1, *key, *value, 0, 0);
    if (ret == TAIR_RETURN_SUCCESS) {
      cout <<" client 2: put successfully."<<endl;
    }
    else {
      cout <<"client 2: failed put operation."<<endl;
    }
/*
    ret = client->invalidate(1, *key, group_name.c_str());
    if (ret == TAIR_RETURN_SUCCESS) cout<<"invalidate ,ok"<<endl;
    else cout<<"invalidate, failed"<<endl;
*/

    ret = client->prefix_invalidate(1, *key, group_name.c_str());
    if (ret == TAIR_RETURN_SUCCESS) cout<<"prefix_invalidate, ok"<<endl;
    else cout<<"prefix_invalidate, failed"<<endl;

 /*   ret = client->hide_by_proxy(1, *key, group_name.c_str());
    if (ret == TAIR_RETURN_SUCCESS) cout<<"ok"<<endl;
    else cout<<"failed"<<endl;
*/
/*
    ret = client->prefix_hide_by_proxy(1, *key, group_name.c_str());
    if (ret == TAIR_RETURN_SUCCESS) cout<<"ok"<<endl;
    else cout<<"failed"<<endl;
*/
    delete key;
    delete value;
  }
  delete client;
  delete client1;
  delete client2;
  // test_invalid_server_start_single();
  return 0;
}

