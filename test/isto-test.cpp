//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "../isto.h"
#include <gtest/gtest.h>
#include <numeric> // std::iota
#include <boost/filesystem.hpp>

namespace {

    class IstoTest : public ::testing::Test {
    protected:
        IstoTest() {
            // You can do set-up work for each test here.

            // Set the directory name so that we shouldn't ever delete any real data.
#ifdef WIN32
            configuration.baseDirectory = ".\\test-data";
#else // WIN32
            configuration.baseDirectory = "./test-data";
#endif // WIN32

            const double byteInGiB = 1.0 / 1024 / 1024 / 1024;
            configuration.maxRotatingDataToKeepInGiB = 1024 * byteInGiB; // 1 kB

            // Clean up existing databases, if any.
            boost::filesystem::remove_all(configuration.baseDirectory);

            storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

            {
                std::vector<unsigned char> sampleData(256);
                std::iota(sampleData.begin(), sampleData.end(), 0);
                
                sampleDataItem = std::unique_ptr<isto::DataItem>(new isto::DataItem(sampleDataId, sampleData));
            }
        }

        virtual ~IstoTest() {
            // You can do clean-up work that doesn't throw exceptions here.
        }

        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right
            // before each test).
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
        }

        void SaveSequentialData(int count) {
            for (int i = 0; i < count; ++i) {
                std::ostringstream oss;
                oss << sequentialDataCounter << ".bin";

                storage->SaveData(isto::DataItem(oss.str(), sampleDataItem->data));

                ++sequentialDataCounter;
            }
        }

        isto::Configuration configuration;
        std::unique_ptr<isto::Storage> storage;
        const char* sampleDataId = "asdf.bin";
        std::unique_ptr<isto::DataItem> sampleDataItem;

        uintmax_t sequentialDataCounter = 0;
    };

    // Tests that an image storage can be set up.
    TEST_F(IstoTest, CanBeSetUp) {
    }

    TEST_F(IstoTest, CannotCreateDuplicateInstance) {
        ASSERT_THROW(new isto::Storage(configuration), std::exception);
    }

    TEST_F(IstoTest, SavesAndReadsData) {
        storage->SaveData(*sampleDataItem);

        const isto::DataItem retrievedDataItem = storage->GetData(sampleDataId);

        EXPECT_EQ(retrievedDataItem.id, sampleDataItem->id);
        EXPECT_EQ(retrievedDataItem.data, sampleDataItem->data);
        EXPECT_EQ(retrievedDataItem.isPermanent, sampleDataItem->isPermanent);
        EXPECT_EQ(retrievedDataItem.isValid, sampleDataItem->isValid);

        // The timestamp may have been rounded.
        EXPECT_LT(std::chrono::duration_cast<std::chrono::microseconds>(retrievedDataItem.timestamp - sampleDataItem->timestamp).count(), 1);
    }

    TEST_F(IstoTest, DoesNotInsertDuplicateData) {
        EXPECT_NO_THROW(storage->SaveData(*sampleDataItem));
        EXPECT_THROW(storage->SaveData(*sampleDataItem), std::exception);
    }

    TEST_F(IstoTest, MakesPermanentAndRotating) {
        storage->SaveData(*sampleDataItem);
        EXPECT_TRUE(storage->MakePermanent(sampleDataItem->id));
        EXPECT_EQ(storage->GetData(sampleDataItem->id).isPermanent, true);
        EXPECT_TRUE(storage->MakeRotating(sampleDataItem->id));
        EXPECT_EQ(storage->GetData(sampleDataItem->id).isPermanent, false);
    }

    TEST_F(IstoTest, PersistsData) {
        storage->SaveData(*sampleDataItem);
        storage.reset(); // destruct the storage object
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));
        const isto::DataItem retrievedDataItem = storage->GetData(sampleDataId);
        EXPECT_TRUE(retrievedDataItem.isValid);
        EXPECT_EQ(retrievedDataItem.id, sampleDataItem->id);
    }    

    TEST_F(IstoTest, RemovesExcessData) {
        SaveSequentialData(10);

        EXPECT_FALSE(storage->GetData("0.bin").isValid);
        EXPECT_FALSE(storage->GetData("1.bin").isValid);
        EXPECT_TRUE(storage->GetData("8.bin").isValid);
        EXPECT_TRUE(storage->GetData("9.bin").isValid);
    }

    TEST_F(IstoTest, DoesNotFillHardDisk) {
        const auto initialSpace = boost::filesystem::space(boost::filesystem::path(configuration.baseDirectory));

        // First fill up the database
        SaveSequentialData(20);

        const auto afterSaving1 = boost::filesystem::space(boost::filesystem::path(configuration.baseDirectory));
        EXPECT_LT(afterSaving1.free, initialSpace.free);

        // Set up new, tight limits
        configuration.maxRotatingDataToKeepInGiB = 1.0;
        configuration.minFreeDiskSpaceInGiB = (afterSaving1.free - 4096) / 1024.0 / 1024.0 / 1024.0;

        // Take the updated configuration in use
        storage.reset();
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

        SaveSequentialData(20);

        const auto afterSaving2 = boost::filesystem::space(boost::filesystem::path(configuration.baseDirectory));

        EXPECT_EQ(afterSaving1.free, afterSaving2.free);

        EXPECT_FALSE(storage->GetData("0.bin").isValid);
        EXPECT_FALSE(storage->GetData("1.bin").isValid);
        EXPECT_FALSE(storage->GetData("10.bin").isValid);
        EXPECT_FALSE(storage->GetData("11.bin").isValid);
        EXPECT_TRUE(storage->GetData("38.bin").isValid);
        EXPECT_TRUE(storage->GetData("39.bin").isValid);
    }

}  // namespace
