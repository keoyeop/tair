/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *  Tool: HashBucketIndexer to MapBucketIndexer
 *
 * Version: $Id$
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *
 */

#include "ldb_manager.hpp"
using namespace tair::storage::ldb;

void print_help(const char* name)
{
  fprintf(stderr, "%s: convert buckets from hash_bucket_indexer to map_bucket_indexer\n"
          "\t-c index_count -b buckets (like: 1,2,3) -f output_file\n", name);
}

int main(int argc, char* argv[])
{
  int i = 0;
  char* buckets_str = NULL;
  int32_t index_count = 0;
  char* file = NULL;
  while ((i = getopt(argc, argv, "b:c:f:")) != EOF)
  {
    switch (i)
    {
    case 'b':
      buckets_str = optarg;
      break;
    case 'c':
      index_count = atoi(optarg);
      break;
    case 'f':
      file = optarg;
      break;
    default:
      print_help(argv[0]);
      break;
    }
  }

  if (buckets_str == NULL || index_count <= 0 || file == NULL)
  {
    print_help(argv[0]);
    return 1;
  }

  std::vector<std::string> bucket_strs;
  std::set<int32_t> uniq_buckets;
  tair::util::string_util::split_str(buckets_str, ", ", bucket_strs);
  for (size_t i = 0; i < bucket_strs.size(); ++i)
  {
    uniq_buckets.insert(atoi(bucket_strs[i].c_str()));
  }

  HashBucketIndexer* hash_indexer = new HashBucketIndexer();
  std::vector<int32_t> buckets(uniq_buckets.begin(), uniq_buckets.end());
  BucketIndexer::INDEX_BUCKET_MAP index_map;
  bool update;
  hash_indexer->sharding_bucket(index_count, buckets, index_map, update);

  BucketIndexer::BUCKET_INDEX_MAP bucket_map;
  bool recheck;
  for (std::vector<int32_t>::iterator it = buckets.begin(); it != buckets.end(); ++it)
  {
    bucket_map[*it] = hash_indexer->bucket_to_index(*it, recheck);
  }

  std::string index_str = BucketIndexer::to_string(index_count, bucket_map);
  FILE* f = fopen(file, "w");
  if (f == NULL)
  {
    fprintf(stderr, "open output file fail: %s, error: %s\n", file, strerror(errno));
    return 1;
  }
  fprintf(f, "%s", index_str.c_str());
  fclose(f);

  return 0;
}
