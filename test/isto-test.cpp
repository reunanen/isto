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
            std::string rotatingDirectory = ".\\test-data\\rotating";
            std::string permanentDirectory = ".\\test-data\\permanent";
#else // WIN32
            std::string rotatingDirectory = ".//test-data//rotating";
            std::string permanentDirectory = ".//test-data//permanent";
#endif // WIN32

            // Clean up existing databases, if any.
            boost::filesystem::remove_all(configuration.rotatingDirectory);
            boost::filesystem::remove_all(configuration.permanentDirectory);

            storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

            {
                std::vector<unsigned char> sampleData(4096);
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
        // Set up new, tight limits
        configuration.maxRotatingDataToKeepInGiB = 8.0 / 1024 / 1024; // 8 kiB

        // Take the updated configuration in use
        storage.reset();
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

        SaveSequentialData(10);

        EXPECT_FALSE(storage->GetData("0.bin").isValid);
        EXPECT_FALSE(storage->GetData("1.bin").isValid);
        EXPECT_TRUE(storage->GetData("8.bin").isValid);
        EXPECT_TRUE(storage->GetData("9.bin").isValid);
    }

    TEST_F(IstoTest, DoesNotFillHardDisk) {
        const auto initialSpace = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        // First fill up the database
        SaveSequentialData(20);

        EXPECT_TRUE(storage->MakePermanent("3.bin"));

        const auto afterSaving1 = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        // Set up new, tight limits
        configuration.maxRotatingDataToKeepInGiB = 1.0;
        configuration.minFreeDiskSpaceInGiB = (afterSaving1.free - 2 * 4096) / 1024.0 / 1024.0 / 1024.0;

        // Take the updated configuration in use
        storage.reset();
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

        SaveSequentialData(20);

        const auto afterSaving2 = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        EXPECT_FALSE(storage->GetData("0.bin").isValid);
        EXPECT_FALSE(storage->GetData("1.bin").isValid);
        EXPECT_FALSE(storage->GetData("2.bin").isValid);
        EXPECT_TRUE(storage->GetData("3.bin").isValid);
        EXPECT_FALSE(storage->GetData("4.bin").isValid);
        EXPECT_FALSE(storage->GetData("5.bin").isValid);
        EXPECT_TRUE(storage->GetData("38.bin").isValid);
        EXPECT_TRUE(storage->GetData("39.bin").isValid);
    }

    TEST_F(IstoTest, DoesNotSaveRotatingIfHardDiskAlreadyFull) {
        const auto space = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        configuration.maxRotatingDataToKeepInGiB = 1.0;
        configuration.minFreeDiskSpaceInGiB = space.free / 1024.0 / 1024.0 / 1024.0;

        // Take the updated configuration in use
        storage.reset();
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

        storage->SaveData(*sampleDataItem);

        EXPECT_FALSE(storage->GetData(sampleDataId).isValid);
    }

    TEST_F(IstoTest, DoesSavePermanentEvenIfRotatingAlreadyFull) {
        const auto space = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        configuration.maxRotatingDataToKeepInGiB = 1.0;
        configuration.minFreeDiskSpaceInGiB = space.free / 1024.0 / 1024.0 / 1024.0;

        // Take the updated configuration in use
        storage.reset();
        storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));

        std::vector<unsigned char> data(4096);

        isto::DataItem permanentDataItem("perm.bin", data, isto::now(), true);

        storage->SaveData(permanentDataItem);

        EXPECT_TRUE(storage->GetData(permanentDataItem.id).isValid);
    }

    TEST_F(IstoTest, GetsLatestData) {
        SaveSequentialData(10);

        const isto::DataItem latestDataItem = storage->GetData();

        EXPECT_TRUE(latestDataItem.isValid);
        EXPECT_EQ(latestDataItem.id, "9.bin");
    }

    TEST_F(IstoTest, GetsPreviousAndNextData) {

        const auto now = isto::now();

        const isto::DataItem dataItem1("1.bin", sampleDataItem->data, now - std::chrono::microseconds(20));
        const isto::DataItem dataItem2("2.bin", sampleDataItem->data, now - std::chrono::microseconds(15));
        const isto::DataItem dataItem3("3.bin", sampleDataItem->data, now - std::chrono::microseconds(12));
        const isto::DataItem dataItem4("4.bin", sampleDataItem->data, now - std::chrono::microseconds(10));
        const isto::DataItem dataItem5("5.bin", sampleDataItem->data, now - std::chrono::microseconds(5));

        storage->SaveData(dataItem1);
        storage->SaveData(dataItem2);
        storage->SaveData(dataItem3);
        storage->SaveData(dataItem4);
        storage->SaveData(dataItem5);

        const auto nowMinus7us = now - std::chrono::microseconds(7);
        const auto nowMinus11us = now - std::chrono::microseconds(11);
        const auto nowMinus30us = now - std::chrono::microseconds(30);

        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">").id, "4.bin");
            EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<").id, "2.bin");
            EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">=").id, "3.bin");
            EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<=").id, "3.bin");

            EXPECT_EQ(storage->GetData(nowMinus7us, ">=").id, "5.bin");
            EXPECT_EQ(storage->GetData(nowMinus7us, ">").id, "5.bin");
            EXPECT_EQ(storage->GetData(nowMinus7us, "<=").id, "4.bin");
            EXPECT_EQ(storage->GetData(nowMinus7us, "<").id, "4.bin");
            EXPECT_EQ(storage->GetData(nowMinus7us, "~").id, "5.bin");
            EXPECT_FALSE(storage->GetData(nowMinus7us, "==").isValid);

            const auto tie = storage->GetData(nowMinus11us, "~");
            EXPECT_TRUE(tie.id == "3.bin" || tie.id == "4.bin");
            EXPECT_FALSE(storage->GetData(nowMinus11us, "==").isValid);

            EXPECT_EQ(storage->GetData(nowMinus30us, ">=").id, "1.bin");
            EXPECT_EQ(storage->GetData(nowMinus30us, ">").id, "1.bin");
            EXPECT_EQ(storage->GetData(nowMinus30us, "~").id, "1.bin");
            EXPECT_FALSE(storage->GetData(nowMinus30us, "<=").isValid);
            EXPECT_FALSE(storage->GetData(nowMinus30us, "<").isValid);
            EXPECT_FALSE(storage->GetData(nowMinus30us, "==").isValid);

            EXPECT_EQ(storage->GetData(now, "<=").id, "5.bin");
            EXPECT_EQ(storage->GetData(now, "<").id, "5.bin");
            EXPECT_EQ(storage->GetData(now, "~").id, "5.bin");
            EXPECT_FALSE(storage->GetData(now, ">=").isValid);
            EXPECT_FALSE(storage->GetData(now, ">").isValid);
            EXPECT_FALSE(storage->GetData(now, "==").isValid);

            // move items around
            if (i == 0) {
                EXPECT_TRUE(storage->MakePermanent(dataItem3.id));
            }
            else if (i == 1) {
                EXPECT_TRUE(storage->MakePermanent(dataItem1.id));
                EXPECT_TRUE(storage->MakePermanent(dataItem4.id));
            }
            else if (i == 2) {
                EXPECT_TRUE(storage->MakeRotating(dataItem1.id));
            }
            else if (i == 3) {
                EXPECT_TRUE(storage->MakeRotating(dataItem3.id));
                EXPECT_TRUE(storage->MakePermanent(dataItem5.id));
            }
        }
    }

}  // namespace
