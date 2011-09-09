/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * leveldb storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "common/define.hpp"
#include "common/util.hpp"
#include "ldb_bucket.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      using namespace tair::common;

      LdbBucket::LdbBucket() : db_(NULL)
      {
        db_path_[0] = '\0';
      }

      LdbBucket::~LdbBucket()
      {
        stop();        
      }

      bool LdbBucket::start(int bucket_number)
      {
        bool ret = true;
        // enable to config multi path.. ?
        const char* data_dir = TBSYS_CONFIG.getString(TAIRLDB_SECTION, LDB_DATA_DIR, LDB_DEFAULT_DATA_DIR);

        if (NULL == data_dir)
        {
          log_error("ldb data dir path not config, item: %s.%s", TAIRLDB_SECTION, LDB_DATA_DIR);
          ret = false;
        }
        else
        {
          // leveldb data path
          snprintf(db_path_, PATH_MAX, "%s/tair_ldb_%06d", data_dir, bucket_number);

          // TODO: config options ..
          // write_buffer_size, max_open_files, block_cache etc.
          leveldb::Options options;
          options.create_if_missing = true;
          options.paranoid_checks = true;
          options.error_if_exists = false; // exist is ok
          options.max_open_files = 65535;

          leveldb::Status status = leveldb::DB::Open(options, db_path_, &db_);
          if (!status.ok())
          {
            log_error("ldb init database fail, error: %s", status.ToString().c_str());
            ret = false;
          }
          else
          {
            log_debug("ldb init database %d ok", bucket_number);
          }
        }

        return ret;
      }

      void LdbBucket::stop()
      {
        log_debug("stop ldb %s", db_path_);
        if (db_ != NULL)
        {
          delete db_;
          db_ = NULL;
        }
      }

      void LdbBucket::destory()
      {
        stop();

        leveldb::Status status = leveldb::DestroyDB(db_path_, leveldb::Options());
        if (!status.ok())
        {
          log_error("remove ldb database fail. path: %s, error: %s", db_path_, status.ToString().c_str());
        }
        else
        {
          log_debug("destroy ldb database ok: %s", db_path_);
        }
      }

      int LdbBucket::put(data_entry& key, data_entry& value, bool version_care, uint32_t expire_time)
      {
        assert(db_ != NULL);
        kdb_item item;

        int cdate = 0;
        int mdate = 0;
        int edate = 0;
        // int stat_data_size = 0;
        int rc = TAIR_RETURN_SUCCESS;

        if(key.data_meta.cdate == 0 || version_care)
        {
          cdate = time(NULL);
          mdate = cdate;
          if(expire_time > 0)
          {
            edate = expire_time > static_cast<uint32_t>(mdate) ? expire_time : mdate + expire_time;
          }
        }
        else
        {
          cdate = key.data_meta.cdate;
          mdate = key.data_meta.mdate;
          edate = key.data_meta.edate;
        }

        // int li = util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
        // if(!locks->lock(li, true)) {
        //   log_error("acquire lock failed");
        //   return TAIR_RETURN_FAILED;
        // }

        std::string old_value;
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.get_data(), key.get_size()),
                                          &old_value);

        if (status.ok())
        {
          // key already exist
          item.full_value = const_cast<char*>(old_value.data());
          item.full_value_size = old_value.length();
          item.decode();
          cdate = item.meta.cdate; // set back the create time

          if (item.is_expired())
          {
            item.meta.version = 0;
          }
          else if (version_care)
          {
            // item is not expired & care version, check version
            if (key.data_meta.version != 0
                && key.data_meta.version != item.meta.version)
            {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }

          item.full_value = NULL;
          item.full_value_size = 0;

          // if (rc == TAIR_RETURN_SUCCESS)
          // {
          //   stat_data_size -= val_size + key.get_size();
          // }
        }
        else if (!status.IsNotFound()) // occure error, but not key NotFound
        {
          log_error("get old item from ldb database fail. %s", status.ToString().c_str());
          rc = TAIR_RETURN_FAILED;
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          item.meta.flag = value.data_meta.flag;
          item.meta.cdate = cdate;
          item.meta.mdate = mdate;
          item.meta.edate = edate;
          if (version_care)
          {
            item.meta.version++;
          }
          else
          {
            item.meta.version = key.data_meta.version;
          }

          item.value = value.get_data();
          item.value_size = value.get_size();

          item.encode();
          // stat_data_size += item.full_value_size + key.get_size();
          
          // TODO: option specified ..
          status = db_->Put(leveldb::WriteOptions(), leveldb::Slice(key.get_data(), key.get_size()),
                            leveldb::Slice(item.full_value, item.full_value_size));

          if (!status.ok())
          {
            log_error("update ldb item fail. %s", status.ToString().c_str());
            rc = TAIR_RETURN_FAILED;
          }
          else
          {
            // stat_mgr.stat_add(key.area, stat_data_size, stat_data_size);
          }

          // free encoded value
          item.free_full_value();

          //update key's meta info
          key.data_meta.flag = item.meta.flag;
          key.data_meta.cdate = item.meta.cdate;
          key.data_meta.edate = item.meta.edate;
          key.data_meta.mdate = item.meta.mdate;
          key.data_meta.version = item.meta.version;
          key.data_meta.keysize = key.get_size();
          key.data_meta.valsize = item.value_size;
        }

        // locks->unlock(li);
        log_debug("ldb::put %s, key len: %d, value len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size(), value.get_size());
        return rc;
      }

      int LdbBucket::get(data_entry& key, data_entry& value)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        std::string old_value;
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.get_data(), key.get_size()),
                                          &old_value);

        if (status.IsNotFound())
        {
          log_debug("get ldb item not found");
          rc = TAIR_RETURN_DATA_NOT_EXIST;
        }
        else if (status.ok())
        {
          kdb_item item;
          item.full_value = const_cast<char*>(old_value.data());
          item.full_value_size = old_value.length();
          item.decode();

          if (item.is_expired())
          {
            log_debug("remove expire data return: %d", remove(key, false));
            rc = TAIR_RETURN_DATA_EXPIRED;
          }
          else
          {
            value.set_data(item.value, item.value_size);
            
            //update meta info
            value.data_meta.flag = item.meta.flag;
            value.data_meta.cdate = item.meta.cdate;
            value.data_meta.edate = item.meta.edate;
            value.data_meta.mdate = item.meta.mdate;
            value.data_meta.version = item.meta.version;
            value.data_meta.valsize = item.value_size;
            value.data_meta.keysize = key.get_size();

            key.data_meta.flag = item.meta.flag;
            key.data_meta.cdate = item.meta.cdate;
            key.data_meta.edate = item.meta.edate;
            key.data_meta.mdate = item.meta.mdate;
            key.data_meta.version = item.meta.version;
            key.data_meta.keysize = key.get_size();
            key.data_meta.valsize = item.value_size;
          }
        }
        else                    // get occur error
        {
          log_error("get ldb item fail: %s", status.ToString().c_str());
          rc = TAIR_RETURN_FAILED;
        }

        log_debug("ldb::get %s, key len: %d, value len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size(), value.get_size());

        return rc;
      }

      int LdbBucket::remove(data_entry& key, bool version_care)
      {
        assert(db_ != NULL);

        int rc = TAIR_RETURN_SUCCESS;

        // int stat_data_size = 0;

        // int li = util::string_util::mur_mur_hash(key.get_data(), key.get_size()) % LOCKER_SIZE;
        // if(!locks->lock(li, true)) {
        //   log_error("acquire lock failed");
        //   return TAIR_RETURN_FAILED;
        // }

        leveldb::Status status;
        if (version_care)
        {
          std::string old_value;
          status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.get_data(), key.get_size()),
                            &old_value);

          if (status.ok())
          {
            kdb_item item;
            item.full_value = const_cast<char*>(old_value.data());
            item.full_value_size = old_value.length();
            // stat_data_size = val_size + key.get_size();
            item.decode();

            if (key.data_meta.version != 0
                && key.data_meta.version != item.meta.version)
            {
              rc = TAIR_RETURN_VERSION_ERROR;
            }
          }
          else                    // get occur error
          {
            log_error("get ldb item fail: %s", status.ToString().c_str());
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
          }
        }

        if (rc == TAIR_RETURN_SUCCESS)
        {
          status = db_->Delete(leveldb::WriteOptions(), leveldb::Slice(key.get_data(), key.get_size()));
          if (!status.ok())
          {
            log_error("remove ldb item fail: %s", status.ToString().c_str()); // ignore return status
            rc = status.IsNotFound() ? TAIR_RETURN_DATA_NOT_EXIST : TAIR_RETURN_FAILED;
          }
          else
          {
            // stat_mgr.stat_sub(key.area, stat_data_size, stat_data_size);
          }
        }

        // locks->unlock(li);
        log_debug("ldb::get %s, key len: %d", TAIR_RETURN_SUCCESS == rc ? "ok" : "fail",
                  key.get_size());

        return rc;
      }

      bool LdbBucket::begin_scan()
      {
        return true;
      }

      bool LdbBucket::end_scan()
      {
        return true;
      }

      int LdbBucket::get_next_item(item_data_info* &data)
      {
        return 0;
      }


    }
  }
}
