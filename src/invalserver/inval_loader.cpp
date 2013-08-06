#include "tair_client_api_impl.hpp"
#include "inval_loader.hpp"
#include <algorithm>
#include <functional>
#include <cstdlib>
#include "inval_group.hpp"
#include "util.hpp"
namespace tair {
  std::vector<std::string> InvalLoader::base_section_names;
  InvalLoader::ClusterInfo::ClusterInfo() : master(0), slave(0), all_connected(false)
  {
    mode = LOCAL_MODE;
  }

  InvalLoader::ClusterInfo::~ClusterInfo()
  {
    for (group_info_map_t::iterator it = groups.begin(); it != groups.end(); ++it)
    {
      if (it->second != NULL)
      {
        delete it->second;
      }
    }
    groups.clear();
  }

  InvalLoader::InvalLoader() : PeriodicTask("inval_loader", LOADER_SLEEP_TIME)
  {
    load_failure = true;
    max_failed_count = 0;
    config_file_version = 0;
    base_section_names.push_back("public");
    base_section_names.push_back("invalserver");
    config_file_status = CONFIG_OK;
    log_rotate_time = ((time(NULL) - timezone)) % 86400;
  }

  InvalLoader::~InvalLoader()
  {
    for (cluster_info_map_t::iterator it = clusters.begin(); it != clusters.end(); ++it)
    {
      if (it->second != NULL)
      {
        delete it->second;
      }
    }
    clusters.clear();
  }

  void InvalLoader::set_thread_parameter(int max_failed_count, std::string config_file_name)
  {
    this->max_failed_count = max_failed_count;
    this->config_file_name = config_file_name;
  }

  bool InvalLoader::find_groups(const char *groupname, std::vector<TairGroup*>* &groups, int* p_local_cluster_count)
  {
    groups = NULL;
    if (groupname == NULL)
    {
      log_error("groupname is NULL!");
      return false;
    }
    if (load_failure)
    {
      return false;
    }
    CLIENT_HELPER_MAP::iterator it = client_helper_map.find(groupname);
    local_count_map_t::iterator lit = local_count_map.find(groupname);
    if (it == client_helper_map.end() || lit == local_count_map.end())
    {
      return false;
    }
    groups = &(it->second);
    if (p_local_cluster_count != NULL)
    {
      *p_local_cluster_count = lit->second;
    }
    return true;
  }

  void InvalLoader::load_group_name()
  {
    //read the cluster infos from the configeration file.
    fetch_cluster_infos();
    do_reload_work();
  }

  void InvalLoader::do_reload_work()
  {
    int finished_count = 0;
    int cluster_count = 0;
    for (cluster_info_map_t::iterator it = clusters.begin(); it != clusters.end(); ++it)
    {
      if (it->second != NULL)
      {
        cluster_count++;
        ClusterInfo &ci = *(it->second);
        if (ci.group_name_list.empty())
        {
          //load group name
          fetch_group_names(ci);
          //create `tair_client instance for every group name.
          if (!ci.group_name_list.empty() && ci.all_connected == false)
          {
            connect_cluster(ci);
          }
        }
        if (!ci.group_name_list.empty()  && ci.all_connected)
        {
          finished_count++;
        }
      }
    }

    if (finished_count == cluster_count)
    {
      //build the `local_count_map
      for (CLIENT_HELPER_MAP::iterator it = client_helper_map.begin(); it != client_helper_map.end(); ++it)
      {
        std::vector<TairGroup*> &groups = it->second;
        int local_cluster_count = 0;
        for (size_t i = 0; i < groups.size(); ++i)
        {
          if (groups[i] != NULL && groups[i]->get_mode() == LOCAL_MODE)
          {
            local_cluster_count++;
          }
        }

        local_count_map_t::iterator lit = local_count_map.find(it->first);
        if (lit == local_count_map.end())
        {
          local_count_map.insert(local_count_map_t::value_type(it->first, local_cluster_count));
        }
        else
        {
          lit->second = local_cluster_count;
        }
      }
      //finish the loading group name task
      load_failure = false;
      //set the group name to `stat_helper.
      if (!group_names.empty())
      {
        TAIR_INVAL_STAT.setThreadParameter(group_names);
      }
      else
      {
        log_error("FATAL ERROR, group_names is empty.");
      }
    }
  }

  void InvalLoader::start()
  {
    load_group_name();
  }

  void InvalLoader::runTimerTask()
  {
    if (load_failure)
    {
      //loading
      do_reload_work();
    }
    else
    {

      const char *pkey = "invalid_server_counter";
      data_entry key(pkey, strlen(pkey), false);
      int value = 0;
      int ret = TAIR_RETURN_SUCCESS;
      int alive_count = 0;

      //sampling and keepalive.
      alive_count = 0;
      for (size_t i = 0; i < tair_groups.size(); i++)
      {
        if(tair_groups[i] != NULL)
        {
          tair_client_impl *tair_client = tair_groups[i]->get_tair_client();
          if (tair_client != NULL)
          {
            ret = tair_client->add_count(0, key, 1, &value);
            tair_groups[i]->sampling(ret != TAIR_RETURN_TIMEOUT && ret != TAIR_RETURN_SEND_FAILED);
            if (tair_groups[i]->is_healthy())
            {
              log_debug("cluster: %s, group name: %s, healthy.",
                  tair_groups[i]->get_cluster_name().c_str(), tair_groups[i]->get_group_name().c_str());
              alive_count++;
            }
            else
            {
              log_error("cluster: %s, group name: %s, sick.",
                  tair_groups[i]->get_cluster_name().c_str(), tair_groups[i]->get_group_name().c_str());
            }
          }
        }
      }
      log_info("KeepAlive group count: %d, total: %lu", alive_count, tair_groups.size());

      //check config file
      int config_file_version_current = util::file_util::get_file_time(config_file_name.c_str());
      if (config_file_version_current > config_file_version || config_file_status != CONFIG_OK)
      {
        config_file_version = config_file_version_current;
        config_file_status = check_config();
        log_info("config version was changed, result: %d", config_file_status);
      }


      //rotate log
      log_rotate_time ++;
      if ((log_rotate_time % 86400) == 86340)
      {
        log_info("rotatelog end");
        TBSYS_LOGGER.rotateLog(NULL, "%d");
        log_info("rotatelog start");
      }
      if (((log_rotate_time % 3600) == 3000))
      {
        log_rotate_time = ((time(NULL) - timezone)) % 86400;
      }
    }
  }

  void InvalLoader::stop()
  {
    for (size_t i = 0; i < tair_groups.size(); ++i)
    {
      tair_client_impl *tair_client = tair_groups[i]->get_tair_client();
      if (tair_client != NULL)
      {
        tair_client->close();
      }
    }
  }

  //parse the cluster list, cluster name was separated by ','
  void InvalLoader::parse_cluster_list(const char *p_cluster_list, std::vector<std::string> &cluster_name_list)
  {
    size_t begin = 0;
    size_t end = 0;
    size_t pos = 0;
    cluster_name_list.clear();
    std::string cluster_list(p_cluster_list);
    while (pos != std::string::npos && pos < cluster_list.size())
    {
      begin = pos;
      pos = cluster_list.find_first_of(',', pos);
      if (pos == std::string::npos)
      {
        end = cluster_list.size();
      }
      else
      {
        end = pos;
        pos += 1;
      }
      //get the cluster name
      std::string tmp = cluster_list.substr(begin, end - begin);
      //remove the space character at the both ends of the `tmp
      string cluster_name = util::string_util::trim_str(tmp, " ");;
      if (!cluster_name.empty())
      {
        cluster_name_list.push_back(cluster_name);
        log_debug("got cluster name: %s", cluster_name.c_str());
      }
    }
  }

  void InvalLoader::fetch_cluster_infos()
  {
    log_info("start loading groupnames.");
    //collect all group names
    const char* p_cluster_list = TBSYS_CONFIG.getString(INVALSERVER_SECTION, "cluster_list", NULL);
    if (p_cluster_list != NULL)
    {
      //get the cluster name list from the config file
      vector<std::string> cluster_name_list;
      parse_cluster_list(p_cluster_list, cluster_name_list);
      for (size_t i = 0; i < cluster_name_list.size(); ++i)
      {
        //fetch info for cluster
        fetch_info(cluster_name_list[i]);
      }
    }
  }

  void InvalLoader::fetch_info(const std::string &cluster_name)
  {
    uint64_t master_id = 0;
    uint64_t slave_id = 0;
    int mode = LOCAL_MODE;
    int defaultPort = TBSYS_CONFIG.getInt(CONFSERVER_SECTION, TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
    const char *p_master = TBSYS_CONFIG.getString(cluster_name.c_str(), TAIR_INVAL_CLUSTER_MASTER_CFG);
    if (p_master != NULL)
    {
      //read master
      master_id = tbsys::CNetUtil::strToAddr(p_master, defaultPort);
      if (master_id != 0)
      {
        log_info("cluster: %s, got master configserver: %s",
            cluster_name.c_str(), tbsys::CNetUtil::addrToString(master_id).c_str());
        //read slave
        const char *p_slave = TBSYS_CONFIG.getString(cluster_name.c_str(), TAIR_INVAL_CLUSTER_SLAVE_CFG);
        if (p_slave != NULL)
        {
          slave_id = tbsys::CNetUtil::strToAddr(p_slave, defaultPort);
          log_info("cluster: %s, got slave configserver: %s",
              cluster_name.c_str(), tbsys::CNetUtil::addrToString(slave_id).c_str());
        }
        //read mode
        const char *p_mode = TBSYS_CONFIG.getString(cluster_name.c_str(), TAIR_INVAL_CLUSTER_MODE,
            TAIR_INVAL_CLUSTER_LOCAL_MODE);
        if (p_mode != NULL)
        {
          if (strncmp(p_mode, TAIR_INVAL_CLUSTER_REMOTE_MODE, strlen(TAIR_INVAL_CLUSTER_REMOTE_MODE)) == 0)
          {
            mode = REMOTE_MODE;
          }
        }

        ClusterInfo *ci = NULL;
        cluster_info_map_t::iterator it = clusters.find(cluster_name);
        if (it != clusters.end())
        {
          ci = it->second;
        }
        else
        {
          ci = new ClusterInfo();
        }

        if (ci != NULL)
        {
          ci->master = master_id;
          ci->slave = slave_id;
          ci->cluster_name = cluster_name;
          ci->mode = mode;
          if (it == clusters.end())
          {
            clusters.insert(cluster_info_map_t::value_type(cluster_name, ci));
          }
        }
      }
      else
      {
        log_error("not find the cluster: %s's config info.", cluster_name.c_str());
      }
    }
  }

  void InvalLoader::fetch_group_names(ClusterInfo &ci)
  {
    if (ci.group_name_list.empty())
    {
      std::string cluster_name = ci.cluster_name;
      log_debug("load group %s %s %s",
          cluster_name.c_str(),
          tbsys::CNetUtil::addrToString(ci.master).c_str(),
          tbsys::CNetUtil::addrToString(ci.slave).c_str());
      //got group names;
      tair_client_impl client;
      if (client.get_group_name_list(ci.master, ci.slave, ci.group_name_list) == false)
      {
        log_error("exception, when fetch group name from %s.",
            tbsys::CNetUtil::addrToString(ci.master).c_str());
        ci.group_name_list.clear();
      }
      else
      {
        log_info("cluster: %s, fetch group count: %lu, success.", cluster_name.c_str(), ci.group_name_list.size());
      }
    }
  }

  void InvalLoader::connect_cluster(ClusterInfo &ci)
  {
    if (!ci.group_name_list.empty() && ci.all_connected == false)
    {
      std::vector<std::string> &group_name_list = ci.group_name_list;
      int connected_group_count = 0;
      int group_name_count = 0;
      for (size_t i = 0; i < group_name_list.size(); ++i)
      {
        if (group_name_list[i].empty())
          continue;
        group_name_count++;
        //collect all group names for INVAL_STAT_HELPER
        std::vector<std::string>::iterator it = std::find(group_names.begin(),
            group_names.end(),group_name_list[i]);
        if (it == group_names.end())
        {
          group_names.push_back(group_name_list[i]);
        }

        group_info_map_t::iterator gt = ci.groups.find(group_name_list[i]);
        TairGroup *tair_group = NULL;
        if (gt == ci.groups.end())
        {
          TairGroup *tg = new TairGroup(ci.cluster_name, ci.master, ci.slave,
              group_name_list[i], max_failed_count, ci.mode);
          ci.groups.insert(group_info_map_t::value_type(group_name_list[i], tg));
          tair_group = tg;
        }
        else
        {
          tair_group = gt->second;
        }
        if (tair_group != NULL)
        {
          if (!tair_group->is_connected())
          {
            tair_client_impl * client = new tair_client_impl();
            if (client->startup(tbsys::CNetUtil::addrToString(ci.master).c_str(),
                  tbsys::CNetUtil::addrToString(ci.slave).c_str(),
                  (group_name_list[i]).c_str()) == false)
            {
              log_error("cannot connect to configserver %s.",
                  tbsys::CNetUtil::addrToString(ci.master).c_str());
              delete client;
              tair_group->disconnected();
            }
            else
            {
              log_debug("connected: %s => %s.",
                  tbsys::CNetUtil::addrToString(ci.master).c_str(),
                  group_name_list[i].c_str());
              int timeout = TBSYS_CONFIG.getInt("invalserver", "client_timeout", 1000);
              client->set_timeout(timeout);
              //create the instance of TairGroup
              tair_group->set_client(client);
              tair_groups.push_back(tair_group);
              client_helper_map[group_name_list[i].c_str()].push_back(tair_group);
            }
          } //end of gi->connected == false

          if (tair_group->is_connected())
          {
            connected_group_count++;
          }
        }
      }

      if (connected_group_count == group_name_count)
      {
        ci.all_connected = true;
      }
    }
  }

  //print cluster's info to `string
  std::string InvalLoader::get_info()
  {
    stringstream buffer;
    buffer << " inval loader, load config status: " << (load_failure ? "FAILURE" : "SUCCESS") << endl;
    int idx = 0;
    for (cluster_info_map_t::iterator it = clusters.begin(); it != clusters.end(); ++it)
    {
      if (it->second != NULL)
      {
        ClusterInfo &ci = *(it->second);
        buffer << " cluster# " << idx << " name: " << ci.cluster_name << endl;
        buffer << "  got group name: " << (!ci.group_name_list.empty() ? "YES" : "NO") << endl;
        buffer << "  all connected: " << (ci.all_connected ? "YES" : "NO") << endl;
        std::string mode_str = ci.mode == LOCAL_MODE ? TAIR_INVAL_CLUSTER_LOCAL_MODE :
          TAIR_INVAL_CLUSTER_REMOTE_MODE;
        buffer << "  mode: " << mode_str << endl;
        group_info_map_t &gi = ci.groups;
        int gidx = 0;
        for (group_info_map_t::iterator git = gi.begin(); git != gi.end(); ++git)
        {
          if (git->second != NULL)
          {
            TairGroup &g = *(git->second);
            buffer << "     group# " << gidx++ << " name: " << g.get_group_name() << endl;
            buffer << "     connected: " << (g.is_connected() ? "YES" : "NO") << endl;
            buffer << "     status: " << (g.is_healthy() ? "HEALTY" : "SICK") <<endl;
          }
        }
        buffer << " cluster# " << idx++ <<  " group count: " << ci.group_name_list.size() << endl;
      }
    }
    return buffer.str();
  }

  bool InvalLoader::is_base_section_name(const std::string &section_name)
  {
    return std::find(base_section_names.begin(), base_section_names.end(), section_name) != base_section_names.end();
  }

  bool InvalLoader::is_same_content(std::vector<std::string> &left, std::vector<std::string> &right)
  {
    if (left.size() != right.size())
    {
      return false;
    }
    else
    {
      //sort
      std::sort(left.begin(), left.end());
      std::sort(right.begin(), right.end());
      std::vector<std::string> intersection(std::max<size_t>(left.size(), right.size()));
      //the intersection
      std::vector<std::string>::const_iterator it = std::set_intersection(
          left.begin(), left.end(),
          right.begin(), right.end(),
          intersection.begin());
      intersection.resize(it - intersection.begin());
      return intersection.size() == left.size() && intersection.size() == right.size();
    }
  }

  //in the `inval_server's config file, there were two kinds of section, base section and cluster section.
  //that base section, such as [public] and [invalserver],  was difference from cluster section,
  //which name was registed in the `cluster_list item.
  //we will go over the follow conditions:
  //1) the config file must be exist;
  //2) the `cluster_list must be in the config file;
  //3) every cluster name registed in the `cluster_list must has config item;
  //4) every cluster config section's name must be registed in the `cluster_list;
  //5) every cluster name registed in the `cluster_list must has loaded by `inval_server. 
  int InvalLoader::check_config()
  {
    int ret = CONFIG_OK;
    tbsys::CConfig config;
    if (config.load(config_file_name.c_str()) == EXIT_FAILURE )
    {
      ret = CONFIG_FILE_NOT_EXIST;
      log_error("load config file %s error, code: %d", config_file_name.c_str(), ret);
    }
    if (ret == CONFIG_OK)
    {
      const char* p_cluster_list = config.getString(INVALSERVER_SECTION, "cluster_list", NULL);
      if (p_cluster_list != NULL)
      {
        //get the cluster name list from the config file
        std::vector<std::string> cluster_name_list;
        parse_cluster_list(p_cluster_list, cluster_name_list);
        //get the cluster section names.
        std::vector<std::string> section_name_list;
        config.getSectionName(section_name_list);
        std::vector<std::string> cluster_section_name_list;
        for (size_t i = 0; i < section_name_list.size(); ++i)
        {
          if (!is_base_section_name(section_name_list[i]))
          {
            cluster_section_name_list.push_back(section_name_list[i]);
          }
        }
        //check the cluster's name in the `cluster_list in the config file.
        if (!is_same_content(cluster_name_list, cluster_section_name_list))
        {
          ret = CONFIG_CLUSTER_NAME_ERROR;
          log_error("config file cluster name error, code: %d", ret);
        }

        if (ret == CONFIG_OK)
        {
          vector<std::string> cluster_name_list_registed;
          for (cluster_info_map_t::iterator it = clusters.begin(); it != clusters.end(); ++it)
          {
            if (it->second != NULL)
            {
              ClusterInfo &ci = *(it->second);
              cluster_name_list_registed.push_back(ci.cluster_name);
            }
          }

          //check the cluster's name loaded by `inval_server.
          if (!is_same_content(cluster_name_list, cluster_name_list_registed))
          {
            ret = CONFIG_CLUSTER_NAME_LIST_MODIFIED;
            log_error("cluster name list was modified, code: %d", ret);
          }
        }
      }
      else {
        ret = CONFIG_CLUSTER_NAME_LIST_NOT_EXIST;
        log_error("cluster name list was not exist, code: %d", ret);
      }
    }
    return ret;
  }
}
