#include "tair_client_api_impl.hpp"
#include "inval_loader.hpp"
#include <algorithm>
#include <functional>
#include <cstdlib>
#include "inval_group.hpp"
  namespace tair {
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

    InvalLoader::InvalLoader()
    {
      loading = true;
      max_failed_count = 0;
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

    void InvalLoader::setThreadParameter(int max_failed_count)
    {
      this->max_failed_count = max_failed_count;
    }

    bool InvalLoader::find_groups(const char *groupname, std::vector<TairGroup*>* &groups, int* p_local_cluster_count)
    {
      groups = NULL;
      if (groupname == NULL)
      {
        log_error("groupname is NULL!");
        return false;
      }
      if (loading)
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
      //connect to groups
      if (!_stop)
      {
        //read the cluster infos from the configeration file.
        fetch_cluster_infos();
        do_reload_work();
      }
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
        loading = false;
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

    void InvalLoader::run(tbsys::CThread *thread, void *arg)
    {
      load_group_name();
      if (!loading)
      {
        log_info("load groupname complete.");
      }

      const char *pkey = "invalid_server_counter";
      data_entry key(pkey, strlen(pkey), false);
      int value = 0;
      int ret = TAIR_RETURN_SUCCESS;

      int alive_count = 0;
      while (!_stop)
      {
        if (loading)
        {
          //loading
          do_reload_work();
        }

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
              tair_groups[i]->sampling(ret == TAIR_RETURN_SUCCESS);
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
          if (_stop)
          {
            break;
          }
        }
        log_info("KeepAlive group count: %d, total: %d", alive_count, tair_groups.size());
        TAIR_SLEEP(_stop, 5);
      }
    }

    void InvalLoader::stop()
    {
      _stop = true;
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
          log_info("cluster: %s, fetch group count: %d, success.", cluster_name.c_str(), ci.group_name_list.size());
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
      buffer << " inval loader, loading: " << (loading ? "YES" : "NO") << endl;
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
  }
