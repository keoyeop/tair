/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * statistics header file
 *
 * Version: $Id: inval_stat_helper.hpp 28 2012-08-17 05:18:09Z fengmao.pj@taobao.com $
 *
 * Authors:
 *   fengmao <fengmao.pj@taobao.com>
 *
 */
#ifndef INVAL_STAT_HELPER_H
#define INVAL_STAT_HELPER_H

#include <zlib.h>
#include <vector>
#include <string>

#include <tbnet.h>
#include "inval_stat.hpp"
#include "inval_periodic_worker.hpp"
namespace tair {
#define TAIR_INVAL_STAT tair::InvalStatHelper::inval_stat_helper_instance

  class CThreadMutex;
  class InvalStatHelper : public PeriodicTask {
  public:
    //inval_stat_helper needs nessary parameters(group names) to start working.
    //it will wait(sleep) until obaining the parameters.
    //the variable 'work_now' = WORK or WAIT.
    enum { WORK = 0, WAIT = 1 };

    //When `need_compressed == DATA_UNCOMPRESSED, do_compress(function) will be invoked
    //to compress the statistic data, and set the `need_compressed DATA_COMPRESSED.
    //In the run(funcation), the `need_compressed will be set DATA_UNCOMPRESSED periodically(10s).
    enum { DATA_COMPRESSED = 0, DATA_UNCOMPRESSED = 1 };

    //operation's name ->{invalid,preifx_invalid,hide,prefix_hide}
    enum {INVALID = 0, PREFIX_INVALID = 1, HIDE = 2, PREFIX_HIDE = 3 };

    InvalStatHelper();
    ~InvalStatHelper();

    void runTimerTask();

    //get the compressed statistics data of the group(s).
    //it will allocate the needed memory,
    //witch shoud be freed by user.
    bool get_stat_buffer(char* & stat_compressed_buffer, unsigned long &buffer_size,
        unsigned long &uncompressed_data_size, int &group_count);

    //invoked by invalid server to pass parameters `group_names.
    bool setThreadParameter(const std::vector<std::string> &group_names);

    //invoked by invalid server only.
    void statistcs(const uint32_t operation_name,
        const std::string& group_name,
        const uint32_t area,
        const uint32_t op_type);
  private:

    // compress the statistics data
    bool do_compress();
    // clean up
    void reset();
  public:
    //glabol instance
    static InvalStatHelper inval_stat_helper_instance;
  private:
    //<group_name, inval_group_stat*>
    typedef __gnu_cxx::hash_map<std::string, inval_group_stat*, tbsys::str_hash> group_stat_map_t;
    group_stat_map_t  group_stat_map;

    inval_group_stat *stat;
    inval_group_stat *current_stat;
    uint32_t group_count;

    uint64_t last_update_time;

    char *compressed_data;
    int compressed_data_size;

    unsigned long compressed_data_buffer_size;
    unsigned char *compressed_data_buffer;
    atomic_t work_now;
    atomic_t need_compressed;

    tbsys::CThreadMutex mutex;

    static const int STAT_HELPER_SLEEP_TIME = 10;
  }; //end of class
} // end of namespace

#endif
