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

#include "db/version_set.h"

#include "common/util.hpp"

#include "ldb_define.hpp"
#include "ldb_comparator.hpp"
#include "ldb_util.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {

      leveldb::Comparator* new_comparator(const char* cmp_desc)
      {
        leveldb::Comparator* cmp = NULL;
        if (cmp_desc == NULL)
        {
          cmp = NULL;
        }
        else if (strcmp(cmp_desc, "bitcmp") == 0)
        {
          cmp = new BitcmpLdbComparatorImpl(NULL);
        }
        else if (strncmp(cmp_desc, "numeric", strlen("numeric")) == 0)
        {
          std::vector<std::string> strs;
          tair::util::string_util::split_str(cmp_desc, ", ", strs);
          if (strs.size() != 3)
          {
            fprintf(stderr, "numeric description error: %s, format like: numeric,:,2\n", cmp_desc);
            cmp = NULL;
          }
          else
          {
            cmp = new NumericalComparatorImpl(NULL, strs[1].c_str()[0], atoi(strs[2].c_str()));
          }
        }
        else
        {
          fprintf(stderr, "unkonwn cmp description: %s\n", cmp_desc);
        }
        return cmp;
      }

      void print_range(const leveldb::VersionSet& versions)
      {
        char buf[128];
        snprintf(buf, sizeof(buf), "sequence: %lu, filenumber: %lu, lognumber: %lu, filecount: %ld\n",
                 versions.LastSequence(), versions.NextFileNumber(), versions.LogNumber(), versions.NumFiles());
        std::string result;
        result.append(buf);
        versions.current()->GetAllRange(result, ldb_key_printer);
        fprintf(stderr, "%s\n", result.c_str());
      }

    }
  }
}
