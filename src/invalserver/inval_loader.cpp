#include "tair_client_api_impl.hpp"
#include "inval_loader.hpp"
#include <algorithm>
#include <functional>
#include <cstdlib>
#include "inval_group.hpp"
  namespace tair {
    InvalLoader::ClusterInfo::ClusterInfo() : master(0), slave(0), all_connected(false)
    {
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

    std::vector<TairGroup*>* InvalLoader::find_groups(const char *groupname)
    {
      if (groupname == NULL)
      {
        log_error("groupname is NULL!");
        return NULL;
      }
      if (loading)
      {
        return NULL;
      }
      CLIENT_HELPER_MAP::iterator it = client_helper_map.find(groupname);
      if ( it == client_helper_map.end())
      {
        return NULL;
      }
      return &(it->second);
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
          if (!ci.group_name_list.empty())
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
                log_debug("cluster: %s, group name: %s, sick.",
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

    void InvalLoader::fetch_cluster_infos()
    {
      log_info("start loading groupnames.");
      int defaultPort = TBSYS_CONFIG.getInt(CONFSERVER_SECTION,
          TAIR_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
      //collect all group names
      std::vector<std::string> keylist;
      TBSYS_CONFIG.getSectionKey(INVALSERVER_SECTION, keylist);
      cluster_info_map_t::iterator it;
      for (size_t i = 0; i < keylist.size(); ++i)
      {
        const char *key = keylist[i].c_str();
        if (key == NULL || strncmp(key, "config_server", 13) != 0)
        {
          continue;
        }
        std::vector<const char*> strList = TBSYS_CONFIG.getStringList(INVALSERVER_SECTION, key);
        for (size_t j = 0; j < strList.size(); ++j)
        {
          uint64_t id = tbsys::CNetUtil::strToAddr(strList[j], defaultPort);
          log_info("got configserver: %s", tbsys::CNetUtil::addrToString(id).c_str());
          if (id == 0)
          {
            continue;
          }
          it = clusters.find(key);

          if (it == clusters.end())
          {
            //create the `cluster_info
            ClusterInfo *ci = new ClusterInfo();
            ci->master = id;
            ci->cluster_name = key;
            clusters.insert(cluster_info_map_t::value_type(key, ci));
          }
          else
          {
            it->second->slave = id;
          }
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
          log_debug("cluster: %s, fetch group count: %d, success.", cluster_name.c_str(), ci.group_name_list.size());
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
          if (group_name_list[i].size() == 0)
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
            TairGroup *tg = new TairGroup(ci.cluster_name, ci.master, ci.slave, group_name_list[i]);
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
      return "none implementation";
    }
  }
