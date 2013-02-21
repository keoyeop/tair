
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

    std::vector<TairGroup*>* find_groups(const char *groupname);
    inline int get_client_count(const char *groupname)
    {
      vector<TairGroup*>* groups = find_groups(groupname);
      return groups != NULL ? groups->size() : 0;
    }

    void run(tbsys::CThread *thread, void *arg);

    inline bool is_loading() const
    {
      return loading;
    }

    void stop();

    std::string get_info();

    void setThreadParameter(int max_failed_count);

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
      int mode;
      bool all_connected;
      ClusterInfo();
      ~ClusterInfo();
    };

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
  protected:
    bool loading;
    CLIENT_HELPER_MAP client_helper_map;
    std::vector<std::string> group_names;
    std::vector<TairGroup*> tair_groups;
    //collect the information of ervery cluster managed by invalid server.
    typedef __gnu_cxx::hash_map<std::string, ClusterInfo*, tbsys::str_hash > cluster_info_map_t;
    cluster_info_map_t clusters;
    int max_failed_count;
    enum
    {
      //local cluster
      LOCAL_MODE = 0,
      //remote cluster
      REMOTE_MODE = 1
    };

  };
}
#endif
