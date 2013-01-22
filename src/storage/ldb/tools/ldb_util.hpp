/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ldb tool utility
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#ifndef TAIR_STORAGE_LDB_TOOLS_LDB_UTIL_H
#define TAIR_STORAGE_LDB_TOOLS_LDB_UTIL_H
namespace leveldb
{
  class Comparator;
  class VersionSet;
}

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      leveldb::Comparator* new_comparator(const char* cmp_desc);
      void print_range(const leveldb::VersionSet& versions);      
    }
  }
}
#endif
