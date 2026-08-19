#include "keylane/storage/tx_cleaner.h"

#include "gtest/gtest.h"

namespace keylane::storage::internal {
namespace {

TEST(TxCleanerTest, ReclaimsOnlyACompletelySettledGeneration) {
  TxGenerationReadiness ready{.sealed_and_durable_ = true};
  EXPECT_TRUE(CanReclaimTxGeneration(ready));

  ready.active_transactions_ = 1;
  EXPECT_FALSE(CanReclaimTxGeneration(ready));
  ready.active_transactions_ = 0;
  ready.live_tagged_bytes_ = 1;
  EXPECT_FALSE(CanReclaimTxGeneration(ready));
  ready.live_tagged_bytes_ = 0;
  ready.dependency_pins_ = 1;
  EXPECT_FALSE(CanReclaimTxGeneration(ready));
  ready.dependency_pins_ = 0;
  ready.sealed_and_durable_ = false;
  EXPECT_FALSE(CanReclaimTxGeneration(ready));
}

}  // namespace
}  // namespace keylane::storage::internal
