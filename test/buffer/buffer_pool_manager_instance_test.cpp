//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstdio>
#include <random>
#include <string>
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }

  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, HardTestZ) {
  const std::string db_name = "test.db";
  remove("test.db");  // Clean file if it doesn't exist
  const size_t buffer_pool_size = 10;

  const int n_workers = 10;
  const int max_page = n_workers * buffer_pool_size;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto u_bpm = std::make_unique<BufferPoolManagerInstance>(buffer_pool_size, disk_manager.get());
  auto *bpm = u_bpm.get();

  char random_binary_data[PAGE_SIZE];
  // Create 50 pages of Random data
  std::map<int, std::string> generated;
  for (auto idx = 0; idx < max_page; ++idx) {
    // Fill the page with random data
    std::generate_n(random_binary_data, PAGE_SIZE, [&]() { return uniform_dist(rng); });
    // Insert terminal characters both in the middle and at end
    random_binary_data[PAGE_SIZE / 2] = '\0';
    random_binary_data[PAGE_SIZE - 1] = '\0';
    // Emplace it into the generated map as a string
    auto gen = std::string(random_binary_data, PAGE_SIZE);
    generated.emplace(idx, gen);  // Use the char array constructor
  }
  LOG_INFO("generated all pages");

  std::mutex write;
  std::map<int, page_id_t> id_map;  // Map from [0...50) to created pg_ids

  std::vector<std::thread> workers;
  workers.reserve(n_workers);
  for (auto id = 0; id < n_workers; ++id) {
    workers.emplace_back([id, bpm, &id_map, &write, &generated]() {
      int lo = 10 * id;
      int hi = 10 * id + 10;
      //      LOG_INFO("WORKER: %d, [%d, %d)", id, lo, hi);
      for (auto i = lo; i < hi; ++i) {
        // Try to emplace a new page
        page_id_t pg_id;
        Page *pg;
        // Create Page, pinning it
        while ((pg = bpm->NewPage(&pg_id)) == nullptr) {
        }  // Retry until Page is created
           //        LOG_INFO("created (idx, pg_id, pg) = (%d, %d, %p)", i, pg_id, pg);

        {  // Page Write Latch
          pg->WLatch();
          auto *data = pg->GetData();
          // Write the data to the page
          for (auto c = 0; c < PAGE_SIZE; ++c) {
            data[c] = generated[i][c];
          }
          pg->WUnlatch();
        }
        {  // Record the actual page id used for the page
          std::lock_guard w(write);
          id_map[i] = pg_id;
        }
        bpm->UnpinPage(pg_id, true);  // Release Page, mark dirty
      }
    });
  }

  LOG_INFO("joining...");
  // Wait for workers to join
  for (auto &w : workers) {
    w.join();
  }
  LOG_INFO("==================joined. checking results===============");
  LOG_INFO("=================Checking Pin Counts & Replacer Size================");
  // Check that the Pin Count of all Frames in the buffer pool is zero
  for (auto i = 0UL; i < buffer_pool_size; ++i) {
    ASSERT_EQ(0, bpm->GetPages()[i].GetPinCount());
  }
  LOG_INFO("==============Pin Counts OK============");
  // Read all pages from the DB and make sure they're correct
  for (auto id = 0; id < max_page; ++id) {
    auto pg_id = id_map[id];
    auto *page = bpm->FetchPage(pg_id);  // Get Page
                                         //    LOG_INFO("Checking %d mapped to %d", id, id_map[id]);
    if (page == nullptr) {               // IF statement for breakpoint in case conditional break doesn't work
      ASSERT_NE(nullptr, page);          // Page Should be valid
    }
    ASSERT_EQ(true, std::string(page->GetData(), PAGE_SIZE) == generated[id]);
    //    LOG_INFO("PG OK: %d", id);
    bpm->UnpinPage(pg_id, false);  // Release Page
  }

  LOG_INFO("shutting down.");
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

}  // namespace bustub
