#include <iostream> 
#include <string> 
#include <cstdlib> 
#include <fstream> 
#include "data_entry.hpp"

#include "tair_client_api_impl.hpp"
using namespace std; 
using namespace tair; 


#ifndef __UNITTEST_TIME_CALC_ENABLE

#define __UNITTEST_TIME_CALC_ENABLE
#define __UNITTEST_TIME_CALC_BEGIN(_func)\
  struct timezone _tz##_func; \
struct timeval __tvStart##_func; \
gettimeofday(&__tvStart##_func,&_tz##_func);

#define __UNITTEST_TIME_CALC_END(_func)   \
      struct timeval __tvEnd##_func; \
gettimeofday(&__tvEnd##_func,&_tz##_func); \
float __func_dif_##_func; \
__func_dif_##_func= (__tvEnd##_func.tv_sec-__tvStart##_func.tv_sec)+(__tvEnd##_func.tv_usec-__tvStart##_func.tv_usec)/1000000.0; \

#endif

int main(int argc,char * argv[])
{
	int opt;
	const char *optstring = "c:g:Vh";
	struct option longopts[] = {
		{"configserver", 1, NULL, 'c'},
		{"groupname", 1, NULL, 'g'},
		{"verbose", 0, NULL, 'V'},
		{"help", 0, NULL, 'h'},
		{0, 0, 0, 0}
	};


	const char *server_addr ;
	const char *group_name;

	opterr = 0;
	while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) 
	{
	  switch (opt) {
		case 'c': 
		  server_addr = optarg;
		  break;
		case 'g':
		  group_name = strdup(optarg);
		  break;
		case 'V':
		  fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
		  return 0;
		case 'h':
		  fprintf(stderr, "%s -c configserver:port -g groupname\n\n",argv[0]);
		  return 0;
	  }

	}
	if (server_addr == NULL || group_name == NULL) 
	{
	  fprintf(stderr, "%s -c configserver:port -g groupname\n\n",argv[0]);
	  return -1;
	}

	TBSYS_LOGGER.setLogLevel("INFO");
	//TBSYS_LOGGER.setLogLevel("DEBUG");
	TBSYS_LOGGER.setFileName("./simple.log");


	tair::tair_client_impl client_helper;
	client_helper.set_timeout(5000);


	bool rv= client_helper.startup(server_addr , server_addr , group_name);
	if (rv== false) 
	{
		log_error("%s can not connect ",server_addr );
		return false;
	}
	//do it .

      int initValue = 100;
	  int count=1;

      int area = 0;
      char *pkey= "incr_test_1_init0";
      int pkeysize = strlen(pkey);

      data_entry key(pkey, pkeysize, false);
float elased_time = 0;
int success=0;
int failed=0;
int loop=100000;

loop=1;
		int retCount = 0;
	  for(int i=0; i<loop; i++)
	  {
		__UNITTEST_TIME_CALC_BEGIN(put)
		// put
		  int ret = client_helper.add_count(0, "dm1", -3, &retCount, 0);
		  //int ret = client_helper.add_count(area, key, count, &retCount, 0);

		if (ret != TAIR_RETURN_SUCCESS) {
		  log_info("add failed:%d.\n",(ret));
          exit(0);
failed++;
		} else {
		  //fprintf(stderr, "retCount: %d\n", retCount);
		log_info("retCount:%d",retCount);
		  success++;
		}
		__UNITTEST_TIME_CALC_END(put)
		  elased_time +=__func_dif_put;
	  }

		log_info("success:%d,failed:%d,c=%.3f,QPS=%.2f/s,la=%.7f,retCount=%d\n", success, failed,elased_time,(success+failed)/elased_time, elased_time/(success+failed),retCount);

	return 0;
}

