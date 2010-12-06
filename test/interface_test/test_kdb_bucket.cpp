#include "kdb_bucket.h"
#include "test_kdb_base.h"
#include "data_entry.hpp"

using namespace tair::common;

TEST_F(TestKdbBase, TestKdbBucketAll)
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  ASSERT_EQ(bucket.start(1), true);
  TestKdbData db;
  if(compareDataValue(*(db.get_test_key(1)), *(db.get_test_value(1))))
    printf("hello!\n");
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
