
#ifndef INVAL_LOADER_H
#define INVAL_LOADER_H

#include <unistd.h>
#include <vector>
#include <string>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "tair_client_api.hpp"
#include "tair_client_api_impl.hpp"
#include "data_entry.hpp"
#include "log.hpp"
#include "inval_stat_helper.hpp"
#include<string>
namespace tair {
  class TairGroup;
  typedef __gnu_cxx::hash_map<std::string, std::vector<TairGroup*>, tbsys::str_hash > CLIENT_HELPER_MAP;

  class InvalLoader: public tbsys::CDefaultRunnable {
  public:
    InvalLoader();
    virtual ~InvalLoader();

    bool find_groups(const char *groupname, std::vector<TairGroup*>* &groups, int* p_local_cluster_count);
    inline int get_client_count(const char *groupname)
    {
      vector<TairGroup*>* groups = NULL;
      bool got = find_groups(groupname, groups, NULL);
      return (got && groups != NULL) ? groups->size() : 0;
    }

    void run(tbsys::CThread *thread, void *arg);

    inline bool is_loading() const
    {
      return loading;
    }

    void stop();

    std::string get_info();

    void set_thread_parameter(int max_failed_count, std::string config_file_name);

    //the return value of the funciton `check_config
    enum
    {
      //ok, relax
      CONFIG_OK = 0,
      //config file was exist, but the item named `cluster_list was not contained.
      CONFIG_CLUSTER_NAME_LIST_NOT_EXIST = 1,
      //can not fetch the `cluster_name_list from the config
      CONFIG_CLUSTER_NAME_LIST_ILLEGAL = 2,
      //the config file which hold by the running inval_server was modified.
      //in this case, the inval_server should be restarted.
      CONFIG_CLUSTER_NAME_LIST_MODIFIED = 3,

      //config file was not exist
      CONFIG_FILE_NOT_EXIST = 4,

      //config cluster name error
      CONFIG_CLUSTER_NAME_ERROR = 5,
    };
    int check_config();
  protected:
    //map group name to `TairGroup
    typedef __gnu_cxx::hash_map<std::string, TairGroup*, tbsys::str_hash > group_info_map_t;
    struct ClusterInfo
    {
      uint64_t master;
      uint64_t slave;
      std::string cluster_name;
      std::vector<std::string> group_name_list;
      group_info_map_t groups;
      int8_t mode;
      bool all_connected;
      ClusterInfo();
      ~ClusterInfo();
    };
    typedef __gnu_cxx::hash_map<std::string, int, tbsys::str_hash > local_count_map_t;
    local_count_map_t local_count_map;

  protected:
    void load_group_name();

    //read cluster info from config file.
    void fetch_cluster_infos();

    //get the group name from the cluster.
    void fetch_group_names(ClusterInfo &ci);

    //create tair_client instance
    void connect_cluster(ClusterInfo &ci);

    //reload the group names.
    void do_reload_work();

    //read config info for the cluster named `cluster_name
    void fetch_info(const std::string &cluster_name);
    //parse the cluster's name list in the config file
    void parse_cluster_list(const char *p_cluster_list, std::vector<std::string> &cluster_name_list);

    //return true if the `section_name was registed in the `base_section_names, otherwise return false.
    bool is_base_section_name(const std::string &section_name);

    //return true if the two vectors contain the same elements.
    bool is_same_content(std::vector<std::string> &left, std::vector<std::string> &right);
  protected:
    bool loading;
    CLIENT_HELPER_MAP client_helper_map;
    std::vector<std::string> group_names;
    std::vector<TairGroup*> tair_groups;
    //collect the information of ervery cluster managed by invalid server.
    typedef __gnu_cxx::hash_map<std::string, ClusterInfo*, tbsys::str_hash > cluster_info_map_t;
    cluster_info_map_t clusters;
    int max_failed_count;
    static const int LOADER_SLEEP_TIME = 5;
    static const int MAX_ROTATE_TIME = (60 * 60 * 24) / LOADER_SLEEP_TIME;
    int config_file_version;
    std::string config_file_name;

    //base section name in the config file
    static std::vector<std::string> base_section_names;
  };
}
#endif
