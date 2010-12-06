#include "kdb_manager.h"
#include "test_kdb_base.h"

TEST_F(TestKdbBase, TestKdbBucketAll)
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  ASSERT_EQ(bucket.start(1), true);
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
