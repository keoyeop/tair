/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * statistics impl
 *
 * Version: $Id: inval_stat_helper.cpp 949 2012-08-15 08:39:54Z fengmao.pj@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "inval_stat_helper.hpp"
namespace tair {
  InvalStatHelper InvalStatHelper::inval_stat_helper_instance;

  InvalStatHelper::InvalStatHelper()
  {
    last_update_time = tbsys::CTimeUtil::getTime();
    compressed_data = NULL;
    compressed_data_size = 0;
    group_count = 0;
    atomic_set(&work_now, WAIT);
    atomic_set(&need_compressed, DATA_UNCOMPRESSED);
    stat = NULL;
    current_stat = NULL;
  }

  InvalStatHelper::~InvalStatHelper()
  {
    _stop = true;
    wait();
    if (compressed_data != NULL)
    {
      delete [] compressed_data;
      compressed_data = NULL;
    }
    if (stat != NULL)
    {
      delete [] stat;
    }
    if (current_stat != NULL)
    {
      delete [] current_stat;
    }
    if (compressed_data_buffer != NULL)
    {
      delete [] compressed_data_buffer;
    }
  }

  void InvalStatHelper::run(tbsys::CThread *thread, void *arg)
  {
    log_info("inval_stat_helper starts working...");
    while (_stop == false)
    {
      if (atomic_read(&work_now) == WAIT)
      {
        TAIR_SLEEP(_stop, 10);
        log_debug("waiting for group names...");
        if (_stop)
        {
          break;
        }
        continue;
      }
      //reset the state.
      reset();
      //sleep
      TAIR_SLEEP(_stop, 10);
    }
  }

  // threadsafe
  void InvalStatHelper::reset()
  {
    //copy the stistics data from `current_stat to `stat, and reset the `current_stat. these operations were thread-safe.
    for (size_t i = 0; i < group_count; ++i)
    {
      inval_group_stat *group_stat_value = current_stat + i;
      group_stat_value->copy_and_reset(stat);

    }

    //compute the ratio.
    uint64_t now = tbsys::CTimeUtil::getTime();
    uint64_t interval = now - last_update_time;
    last_update_time = now;
    interval /= 1000000; // conver to second
    if (interval == 0) interval = 1;
    log_info("inval server start calculate ratio, interval: %"PRI64_PREFIX"u", interval);
    //for every group, every area and every type.
    for (size_t i = 0; i < group_count; i++)
    {
      inval_group_stat *gs = stat + i;
      for (size_t k = 0; k < TAIR_MAX_AREA_COUNT; ++k)
      {
        inval_area_stat  &as = gs->get_area_stat(k);
        for (size_t i = 0; i < inval_area_stat::STAT_ELEM_COUNT; ++i)
        {
          as.set_invalid_count(i, as.get_invalid_count(i) / interval);
          as.set_hide_count(i, as.get_hide_count(i) / interval);
          as.set_prefix_invalid_count(i, as.get_prefix_invalid_count(i) / interval);
          as.set_prefix_hide_count(i, as.get_prefix_hide_count(i) / interval);
        }
      }
    }
    //clean the compressed flag
    atomic_set(&need_compressed,DATA_UNCOMPRESSED);
  }

  bool InvalStatHelper::do_compress()
  {
    bool ret = true;
    //lock, protect the buffer
    tbsys::CThreadGuard guard(&mutex);
    //other thread has invoked this function already, just return true.
    if(atomic_read(&need_compressed) == DATA_COMPRESSED)
    {
      return true;
    }
    if (compressed_data != NULL)
    {
      compressed_data_size = 0;
      delete [] compressed_data;
    }
    unsigned long data_size = compressed_data_buffer_size;
    unsigned long uncompressed_data_size = group_count * sizeof(inval_group_stat);
    if (compress(compressed_data_buffer, &data_size,
        (unsigned char*)stat, uncompressed_data_size) == Z_OK)
    {
      compressed_data = new char [data_size];
      memcpy(compressed_data, compressed_data_buffer, data_size);
      compressed_data_size = data_size;
      log_info("compress inval server stats done (%lu=>%u)", uncompressed_data_size, compressed_data_size);
    }
    else
    {
      log_error("failed to uncompress the data.");
      ret = false;
    }
    if (ret)
    {
      //set flag
      atomic_set(&need_compressed,DATA_COMPRESSED);
    }
    return ret;
  }

  // the buffer was allocated by `get_stat_buffer,freed by invaker.
  // return true, if successful; otherwise, return false.
  bool InvalStatHelper::get_stat_buffer(char*& buffer, unsigned long &buffer_size,
      unsigned long & uncompressed_data_size, int &group_count_out)
  {
    bool ret = true;
    if (atomic_read(&work_now) == WAIT)
    {
      buffer_size = 0;
      if (buffer != NULL)
      {
        delete [] buffer;
        buffer = NULL;
      }
      log_error("wait for group names.");
      ret = false;
    }
    if (ret && atomic_read(&need_compressed) == DATA_UNCOMPRESSED)
    {
      if (do_compress() == false)
      {
        log_error("failed to compress the statistic data!");
        ret = false;
      }
    }
    if (ret)
    {
      //lock
      tbsys::CThreadGuard guard(&mutex);
      //copy stat data into user's buffer.
      if (compressed_data == NULL)
      {
        buffer_size = 0;
        buffer = NULL;
        uncompressed_data_size = 0;
        ret = false;
      }
      else
      {
        buffer = new char [compressed_data_size];
        memcpy(buffer, compressed_data, compressed_data_size);
        buffer_size = compressed_data_size;
        uncompressed_data_size = compressed_data_buffer_size;
        group_count_out = group_count;
      }
      //unlock
    }
    return ret;
  }

  bool InvalStatHelper::setThreadParameter(const std::vector<std::string> &group_names_input)
  {
    bool ret = true;
    if (group_names_input.empty() )
    {
      log_error("group_count must be more than 0.");
      ret = false;
    }
    std::vector<std::string> group_names;
    if (ret)
    {
      atomic_set(&work_now, WAIT);
      for (size_t i = 0; i < group_names_input.size(); i++)
      {
        //group names must be unique
        if (group_names_input[i].size() > 0)
        {
          if (std::find(group_names.begin(), group_names.end(), group_names_input[i]) == group_names.end())
          {
            group_names.push_back(group_names_input[i]);
          }
          else
          {
            //duplicate group names.
            log_error("group names: %s, exist.", group_names_input[i].c_str());
          }

        }
        else
        {
          log_debug("got the bad group name: %s", group_names_input[i].c_str());
        }
      }
      group_count = group_names.size();
      if (group_count == 0)
      {
        log_error("group names list was empty.");
        ret = false;
      }
    }
    if (ret)
    {
      //allocate memory
      stat  = new inval_group_stat[group_count];
      current_stat = new inval_group_stat[group_count];

      //alloc memroy for compressing stat data
      unsigned long stat_data_len = group_count* sizeof(inval_group_stat);
      compressed_data_buffer_size = compressBound(stat_data_len);
      compressed_data_buffer = new unsigned char [compressed_data_buffer_size];

      //build the map
      group_stat_map.clear();
      for (size_t i = 0; i < group_count; ++i)
      {
        inval_group_stat* group_stat_value = current_stat + i;
        //set group's name.
        group_stat_value->set_group_name(group_names[i]);
        group_stat_map.insert(group_stat_map_t::value_type(group_names[i], group_stat_value));
        group_stat_value = stat + i;
        group_stat_value->set_group_name(group_names[i]);
      }

      if (group_stat_map.empty())
      {
        log_error("the group stat map was empty.");
        ret = false;
      }
    }
    if (ret)
    {
      // invalid compressed stat data
      atomic_set(&need_compressed, DATA_UNCOMPRESSED);
      atomic_set(&work_now, WORK);
    }
    else
    {
      atomic_set(&work_now, WAIT);
    }
    return ret;
  }

  void InvalStatHelper::statistcs(const uint32_t operation_name,
      const std::string& group_name,
      const uint32_t area,
      const uint32_t op_type)
  {
    group_stat_map_t::iterator it = group_stat_map.find(group_name);
    if (it != group_stat_map.end())
    {
      switch(operation_name)
      {
        case INVALID:
          it->second->get_area_stat(area).inc_invalid_count(op_type);
          break;
        case PREFIX_INVALID:
          it->second->get_area_stat(area).inc_prefix_invalid_count(op_type);
          break;
        case HIDE:
          it->second->get_area_stat(area).inc_hide_count(op_type);
          break;
        case PREFIX_HIDE:
          it->second->get_area_stat(area).inc_prefix_hide_count(op_type);
          break;
        default:
          log_error("unknown operation name, group name: %s, area: %d,opname: %d, optype: %d",
              group_name.c_str(), area, operation_name, op_type);
      }
    } //end of if
    else
    {
      log_error("can't find the group name: %s, in stat. area: %d, opname: %d, optype: %d",
          group_name.c_str(), area, operation_name, op_type);
    }
  }
}
