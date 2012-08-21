/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * some common RecordLogger implementation
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include <unistd.h>
#include <sys/mman.h>

#include "data_entry.hpp"
#include "file_op.hpp"
#include "record_logger_util.hpp"

namespace tair
{
  namespace common
  {
    //////////////////////////////////
    // RingBufferRecordLogger
    //////////////////////////////////
    const int32_t RingBufferRecordLogger::TAIR_LOGGER_SKIP_TYPE = TAIR_REMOTE_SYNC_TYPE_MAX + 16;
    const int32_t RingBufferRecordLogger::HEADER_SIZE = sizeof(int64_t)*2;

    RingBufferRecordLogger::RingBufferRecordLogger(const char* file_path, int64_t mem_size)
      : file_path_(file_path != NULL ? file_path : ""), fd_(-1), mem_size_(mem_size),
        base_(NULL), w_offset_(NULL), r_offset_(NULL)
    {
      // one writer
      writer_count_ = 1;
      // one reader
      reader_count_ = 1;
    }

    RingBufferRecordLogger::~RingBufferRecordLogger()
    {
      if (base_ != NULL)
      {
        ::munmap(base_ - HEADER_SIZE, mem_size_ + HEADER_SIZE);
        base_ = NULL;
      }

      if (fd_ > 0)
      {
        ::fdatasync(fd_);
        ::close(fd_);
      }
    }

    int RingBufferRecordLogger::init()
    {
      int ret = TAIR_RETURN_FAILED;

      if (file_path_.empty())
      {
        log_error("empty logger file path");
      }
      else if (mem_size_ <= 0)
      {
        log_error("invalid max mem length: %"PRI64_PREFIX"d", mem_size_);
      }
      else if ((ret = cleanup()) != TAIR_RETURN_SUCCESS)
      {
        log_error("cleanup earlier logger fail: ret: %d", ret);
      }
      else
      {
        fd_ = ::open(file_path_.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd_ < 0)
        {
          log_error("open file %s faile: %s", file_path_.c_str(), strerror(errno));
        }
        else if (::ftruncate(fd_, HEADER_SIZE + mem_size_) < 0)
        {
          log_error("truncate file %s fail: %s", file_path_.c_str(), strerror(errno));
        }
        else if ((base_ = (char*)mmap(0, HEADER_SIZE + mem_size_, PROT_WRITE | PROT_READ, MAP_SHARED, fd_, 0)) == MAP_FAILED)
        {
          log_error("mmap file %s fail: %s", file_path_.c_str(), strerror(errno));
        }
        else
        {
          ret = TAIR_RETURN_SUCCESS;
          // TODO: dirty data may exist when not msynced.
          w_offset_ = reinterpret_cast<int64_t*>(base_);
          r_offset_ = reinterpret_cast<int64_t*>(base_ + sizeof(int64_t));
          if (*w_offset_ >= mem_size_ || *r_offset_ >= mem_size_)
          {
            log_warn("invalid w_offset: %"PRI64_PREFIX"d, r_offset: %"PRI64_PREFIX"d",
                     *w_offset_, *r_offset_);
            *w_offset_ = 0;
            *r_offset_ = 0;
          }
          // skip index
          base_ += HEADER_SIZE;

          log_warn("init ring buffer record logger success, base_: %lx, w_offset: %"PRI64_PREFIX"d, r_offset: %"PRI64_PREFIX"d",
                   base_, *w_offset_, *r_offset_);
        }
      }

      if (ret != TAIR_RETURN_SUCCESS && fd_ > 0)
      {
        ::close(fd_);
        fd_ = -1;
      }

      return ret;
    }

    int RingBufferRecordLogger::add_record(int32_t index, int32_t type,
                                           data_entry* key, data_entry* value)
    {
      UNUSED(index);
      tbsys::CThreadGuard guard(&mutex_);
      log_debug("@@ ab %ld %ld", *w_offset_, *r_offset_);
      int ret = TAIR_RETURN_SUCCESS;
      char* buf = NULL;
      int total_size = RecordLogger::common_encode_record(buf, type, key, value);

      char* mem_pos = calc_mem_pos(total_size);
      if (mem_pos == NULL)
      {
        log_debug("@@ not space left");
        ret = TAIR_RETURN_FAILED;
      }
      else
      {
        log_debug("@@ %lx pos: %lx %d %ld %ld", base_, mem_pos, total_size, *w_offset_, *r_offset_);
        memcpy(mem_pos, buf, total_size);
        // fill tailer
        if (mem_pos == base_ && mem_size_ - *w_offset_ < total_size)
        {
          char skip[sizeof(int32_t) * 2];

          // total size
          tair::util::coding_util::encode_fixed32(skip, sizeof(int32_t)*2 + mem_size_ - *w_offset_);
          tair::util::coding_util::encode_fixed32(skip + sizeof(int32_t), TAIR_LOGGER_SKIP_TYPE);
          memcpy(base_ + *w_offset_, skip, sizeof(skip));
        }

        *w_offset_ = mem_pos - base_ + total_size;
      }

      if (buf != NULL)
      {
        delete buf;
      }
      log_debug("@@ ae %ld %ld", *w_offset_, *r_offset_);
      return ret;
    }

    int RingBufferRecordLogger::get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                                        data_entry*& key, data_entry*& value,
                                        bool& force_reget)
    {
      UNUSED(index);
      bucket_num = -1;
      key = value = NULL;

      tbsys::CThreadGuard guard(&mutex_);
      log_debug("@@ gb %ld %ld", *w_offset_, *r_offset_);
      int ret = TAIR_RETURN_SUCCESS;
      int32_t size = 0;
      char* pos = base_ + *r_offset_;
      bool found = false;

      // r_offset_/w_offset should be aligned to one record boundry
      while (!found && *r_offset_ != *w_offset_)
      {
        // check skip first
        size = tair::util::coding_util::decode_fixed32(pos);
        type = tair::util::coding_util::decode_fixed32(pos + sizeof(int32_t));
        if (type == TAIR_LOGGER_SKIP_TYPE)
        {
          log_debug("@@ skip %lx %d", pos, size);
          // skip trailer
          pos += size;
          if (pos >= base_ + mem_size_)
          {
            pos = base_;
          }
        }
        else
        {
          log_debug("@@ found %lx", pos);
          pos += common_decode_record(pos, type, key, value);
          found = true;
        }

        *r_offset_ = pos - base_;
      }

      log_debug("@@ ge %ld %ld %d %lx", *w_offset_, *r_offset_, type, key);
      return ret;
    }

    char* RingBufferRecordLogger::calc_mem_pos(int32_t size)
    {
      char* pos = NULL;
      if (*w_offset_ >= *r_offset_)
      {
        if (mem_size_ - *w_offset_ >= size)
        {
          pos = base_ + *w_offset_;
        }
        else if (*r_offset_ >= size)
        {
          pos = base_;  
        }
      }
      else
      {
        if (*r_offset_ - *w_offset_ > size)
        {
          pos = base_ + *w_offset_;
        }
      }
      return pos;
    }

    int RingBufferRecordLogger::cleanup()
    {
      int ret = TAIR_RETURN_SUCCESS;

      if (::access(file_path_.c_str(), F_OK) == 0)
      {
        struct stat s;
        if (::stat(file_path_.c_str(), &s) != 0)
        {
          log_error("stat file fail: %s", strerror(errno));
          ret = TAIR_RETURN_FAILED;
        }
        else if (s.st_size != mem_size_ + HEADER_SIZE) // mmap size conflict, backup
        {
          log_warn("mem size conflict with earlier one, backup. old: %"PRI64_PREFIX"d <> now: %"PRI64_PREFIX"d",
                   s.st_size, mem_size_ + HEADER_SIZE);
          char buf[32];
          buf[0] = '.';
          tbsys::CTimeUtil::timeToStr(time(NULL), buf + 1);
          if (::rename(file_path_.c_str(), (file_path_ + buf).c_str()) != 0)
          {
            log_error("backup earlier logger fail: %s", strerror(errno));
            ret = TAIR_RETURN_FAILED;
          }
        }
      }
      return ret;
    }

    //////////////////////////////////
    // SequentialFileRecordLogger
    //////////////////////////////////
    SequentialFileRecordLogger::SequentialFileRecordLogger(const char* file_path, int64_t max_file_size, bool auto_rotate)
      : file_path_(file_path != NULL ? file_path : ""), file_(NULL), max_file_size_(max_file_size),
        auto_rotate_(auto_rotate), w_offset_(0), r_offset_(0)
    {
    }

    SequentialFileRecordLogger::~SequentialFileRecordLogger()
    {
      if (file_ != NULL)
      {
        delete file_;
      }
    }

    int SequentialFileRecordLogger::init()
    {
      tbsys::CThreadGuard guard(&mutex_);
      if (file_ != NULL)
      {
        delete file_;
      }

      file_ = new FileOperation(file_path_);
      int ret = file_->open_file();
      if (ret < 0)
      {
        log_error("init file %s fail, ret: %d", file_path_.c_str(), ret);
        ret = TAIR_RETURN_FAILED;
      }
      else
      {
        ret = TAIR_RETURN_SUCCESS;
        int64_t file_size = file_->get_file_size();
        if (file_size < 0)
        {
          log_error("get file %s size fail, ret: %"PRI64_PREFIX"d", file_path_.c_str(), file_size);
          ret = TAIR_RETURN_FAILED;
        }
        else
        {
          w_offset_ = file_size;
          r_offset_ = 0;
        }
      }

      return TAIR_RETURN_SUCCESS;
    }

    int SequentialFileRecordLogger::add_record(int32_t index, int32_t type,
                                               data_entry* key, data_entry* value)
    {
      UNUSED(index);
      tbsys::CThreadGuard guard(&mutex_);
      int ret = TAIR_RETURN_SUCCESS;
      char* buf = NULL;
      int32_t total_size = common_encode_record(buf, type, key, value);
      ret = reserve_file_size(w_offset_ + total_size);

      if (ret == TAIR_RETURN_SUCCESS)
      {
        int64_t w_size = file_->pwrite_file(buf, total_size, w_offset_);
        if (w_size != total_size)
        {
          log_error("write data fail, offset: %"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d", w_offset_, total_size);
        }
        else
        {
          w_offset_ += w_size;
        }
      }

      if (buf != NULL)
      {
        delete buf;
      }

      return ret;
    }

    int SequentialFileRecordLogger::get_record(int32_t index, int32_t& type, int32_t& bucket_num,
                                               data_entry*& key, data_entry*& value, bool& force_reget)
    {
      UNUSED(index);
      int ret = TAIR_RETURN_SUCCESS;
      bucket_num = -1;
      key = value = NULL;
      ret = reserve_one_record();

      if (ret == TAIR_RETURN_SUCCESS && last_data_.size() > 0)
      {
        int32_t size = common_decode_record(last_data_.data(), type, key, value);
        r_offset_ += size;
        last_data_.erase(size);
      }
      return ret;
    }

    int SequentialFileRecordLogger::reserve_file_size(int64_t size)
    {
      int ret = TAIR_RETURN_SUCCESS;
      if (size > max_file_size_)
      {
        if (auto_rotate_)
        {
          char buf[32];
          buf[0] = '.';
          tbsys::CTimeUtil::timeToStr(time(NULL), buf + 1);
          ret = file_->rename_file((file_path_ + buf).c_str());
          if (ret != TAIR_RETURN_SUCCESS)
          {
            log_error("rename file fail %s => %s", file_path_.c_str(), (file_path_ + buf).c_str());
          }
          else
          {
            delete file_;
            file_ = new FileOperation(file_path_);
            w_offset_ = r_offset_ = 0;
          }
        }
        else
        {
          log_warn("no space left");
          ret = TAIR_RETURN_FAILED;
        }
      }
      return ret;
    }

    int SequentialFileRecordLogger::reserve_one_record()
    {
      int ret = TAIR_RETURN_SUCCESS;
      bool need_read = false;
      int32_t record_size = 0;

      do
      {
        if (last_data_.size() < sizeof(int32_t))
        {
          need_read = true;
        }
        else
        {
          if (record_size == 0)
          {
            record_size = tair::util::coding_util::decode_fixed32(last_data_.data());
          }
          need_read = (record_size > static_cast<int32_t>(last_data_.size()));
        }

        if (need_read)
        {
          static const int32_t IO_BUFFER_SIZE = 4096;
          static char buf[IO_BUFFER_SIZE];

          int64_t read_size = file_->pread_file(buf, IO_BUFFER_SIZE, r_offset_ + last_data_.size());
          if (read_size < 0)       // read fail
          {
            ret = TAIR_RETURN_FAILED;
            break;
          }
          if (read_size == 0)      // read over
          {
            last_data_.clear();
            ret = TAIR_RETURN_SUCCESS;
            break;
          }

          last_data_.append(buf, read_size);
        }
      } while (need_read);

      return ret;
    }

  }
}

