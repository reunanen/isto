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
            configuration.rotatingDirectory = ".\\test-data\\rotating";
            configuration.permanentDirectory = ".\\test-data\\permanent";
#else // WIN32
            configuration.rotatingDirectory = "./test-data/rotating";
            configuration.permanentDirectory = "./test-data/permanent";
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

        void RecreateStorageWithUpdatedConfiguration()
        {
            storage.reset();
            storage = std::unique_ptr<isto::Storage>(new isto::Storage(configuration));
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
        EXPECT_EQ(retrievedDataItem.timestamp, sampleDataItem->timestamp);
    }

    TEST_F(IstoTest, SavesAndReadsTags) {
        configuration.tags.push_back("test");
        configuration.tags.push_back("test2");

        RecreateStorageWithUpdatedConfiguration();

        isto::tags_t tags;
        tags["test"] = "foo";
        tags["test2"] = "bar";

        const isto::DataItem taggedItem(sampleDataItem->id, sampleDataItem->data, sampleDataItem->timestamp, false, tags);
        storage->SaveData(taggedItem);

        const isto::DataItem readItem = storage->GetData(sampleDataItem->id);

        EXPECT_EQ(readItem.tags, taggedItem.tags);
    };

    TEST_F(IstoTest, DoesNotAllowSpacesInTagNames) {
        configuration.tags.push_back("test tag");
        EXPECT_THROW(RecreateStorageWithUpdatedConfiguration(), std::exception);
    }

    TEST_F(IstoTest, DoesNotInsertDuplicateData) {
        EXPECT_NO_THROW(storage->SaveData(*sampleDataItem));
        EXPECT_THROW(storage->SaveData(*sampleDataItem), std::exception);
    }

    TEST_F(IstoTest, DoesInsertDuplicateDataWhenExplicitlyRequested) {
        EXPECT_NO_THROW(storage->SaveData(*sampleDataItem));
        EXPECT_EQ(storage->GetIdsSortedByAscendingTimestamp().size(), 1);
        EXPECT_EQ(storage->GetData(sampleDataId).data, sampleDataItem->data);

        std::vector<unsigned char> newData(99);
        newData[5] = '5';
        isto::DataItem newDataItem(sampleDataId, newData);
        EXPECT_NO_THROW(storage->SaveData(newDataItem, true));

        EXPECT_EQ(storage->GetIdsSortedByAscendingTimestamp().size(), 1);
        EXPECT_EQ(storage->GetData(sampleDataId).data, newData);
        EXPECT_NE(storage->GetData(sampleDataId).data, sampleDataItem->data);
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

    TEST_F(IstoTest, ServesIdsOfSavedData) {
        SaveSequentialData(10);
        const auto ids = storage->GetIdsSortedByAscendingTimestamp();
        EXPECT_EQ(ids.size(), 10);
        if (!ids.empty()) {
            EXPECT_EQ(ids.front(), "0.bin");
            EXPECT_EQ(ids.back(), "9.bin");
        }
    }

    TEST_F(IstoTest, RemovesExcessData) {
        // Set up new, tight limits
        configuration.maxRotatingDataToKeepInGiB = 8.0 / 1024 / 1024; // 8 kiB
        RecreateStorageWithUpdatedConfiguration();

        SaveSequentialData(10);

        EXPECT_FALSE(storage->GetData("0.bin").isValid);
        EXPECT_FALSE(storage->GetData("1.bin").isValid);
        EXPECT_TRUE(storage->GetData("8.bin").isValid);
        EXPECT_TRUE(storage->GetData("9.bin").isValid);
    }

    TEST_F(IstoTest, AllowsApplicationToDetectThatExcessDataIsRemoved) {
        // Set up new, tight limits
        configuration.maxRotatingDataToKeepInGiB = 8.0 / 1024 / 1024; // 8 kiB
        RecreateStorageWithUpdatedConfiguration();

        int itemsDeleted = 0;

        storage->SetRotatingDataDeletedCallback([&itemsDeleted](const std::string& id) { ++itemsDeleted; });

        SaveSequentialData(10);

        EXPECT_GT(itemsDeleted, 0);
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
        RecreateStorageWithUpdatedConfiguration();

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
        RecreateStorageWithUpdatedConfiguration();

        storage->SaveData(*sampleDataItem);

        EXPECT_FALSE(storage->GetData(sampleDataId).isValid);
    }

    TEST_F(IstoTest, DoesSavePermanentEvenIfRotatingAlreadyFull) {
        const auto space = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory));

        configuration.maxRotatingDataToKeepInGiB = 1.0;
        configuration.minFreeDiskSpaceInGiB = space.free / 1024.0 / 1024.0 / 1024.0;
        RecreateStorageWithUpdatedConfiguration();

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

    TEST_F(IstoTest, GetsLatestDataByTags) {
        configuration.tags.push_back("test");
        configuration.tags.push_back("test2");

        RecreateStorageWithUpdatedConfiguration();

        isto::tags_t tags1, tags2;
        tags1["test"] = "foo";
        tags2["test"] = "bar";
        tags1["test2"] = "foo2";
        tags2["test2"] = "bar2";

        const auto now = isto::now();

        const isto::DataItem dataItem1("1.bin", sampleDataItem->data, now - std::chrono::microseconds(20), false, tags1);
        const isto::DataItem dataItem2("2.bin", sampleDataItem->data, now - std::chrono::microseconds(15), false, tags2);
        const isto::DataItem dataItem3("3.bin", sampleDataItem->data, now - std::chrono::microseconds(12), false, tags1);
        const isto::DataItem dataItem4("4.bin", sampleDataItem->data, now - std::chrono::microseconds(10), false, tags2);
        const isto::DataItem dataItem5("5.bin", sampleDataItem->data, now - std::chrono::microseconds(5), false, tags1);

        storage->SaveData(dataItem1);
        storage->SaveData(dataItem2);
        storage->SaveData(dataItem3);
        storage->SaveData(dataItem4);
        storage->SaveData(dataItem5);

        const isto::DataItem latestDataItem = storage->GetData(now, "~", tags2);

        EXPECT_TRUE(latestDataItem.isValid);
        EXPECT_EQ(latestDataItem.id, "4.bin");
        EXPECT_EQ(latestDataItem.timestamp, dataItem4.timestamp);
        EXPECT_EQ(latestDataItem.tags, tags2);
    }

    TEST_F(IstoTest, GetsPreviousAndNextDataByTags) {

        configuration.tags.push_back("test");
        configuration.tags.push_back("test2");

        RecreateStorageWithUpdatedConfiguration();

        isto::tags_t allTags1, allTags2, partialTags1, partialTags2;
        allTags1["test"] = "foo";
        allTags2["test"] = "bar";
        allTags1["test2"] = "foo2";
        allTags2["test2"] = "bar2";
        partialTags1["test"] = "foo";
        partialTags2["test2"] = "bar2";

        const auto now = isto::now();

        const isto::DataItem dataItem1("1.bin", sampleDataItem->data, now - std::chrono::microseconds(20), false, allTags1);
        const isto::DataItem dataItem2("2.bin", sampleDataItem->data, now - std::chrono::microseconds(15), false, allTags2);
        const isto::DataItem dataItem3("3.bin", sampleDataItem->data, now - std::chrono::microseconds(12), false, allTags1);
        const isto::DataItem dataItem4("4.bin", sampleDataItem->data, now - std::chrono::microseconds(10), false, allTags2);
        const isto::DataItem dataItem5("5.bin", sampleDataItem->data, now - std::chrono::microseconds(5), false, allTags1);

        storage->SaveData(dataItem1);
        storage->SaveData(dataItem2);
        storage->SaveData(dataItem3);
        storage->SaveData(dataItem4);
        storage->SaveData(dataItem5);

        const auto nowMinus30us = now - std::chrono::microseconds(30);

        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 2; ++j) {
                const bool usePartialTags = j > 0;
                const auto tags1 = usePartialTags ? partialTags1 : allTags1;
                const auto tags2 = usePartialTags ? partialTags2 : allTags2;

                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">", tags1).id, "5.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<", tags1).id, "1.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">=", tags1).id, "3.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<=", tags1).id, "3.bin");

                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<", tags2).id, "2.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">=", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<=", tags2).id, "2.bin");

                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">", tags1).id, "5.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<", tags1).id, "1.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">=", tags1).id, "3.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<=", tags1).id, "3.bin");

                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<", tags2).id, "2.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, ">=", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(dataItem3.timestamp, "<=", tags2).id, "2.bin");

                EXPECT_EQ(storage->GetData(nowMinus30us, ">=", tags1).id, "1.bin");
                EXPECT_EQ(storage->GetData(nowMinus30us, ">", tags1).id, "1.bin");
                EXPECT_EQ(storage->GetData(nowMinus30us, "~", tags1).id, "1.bin");
                EXPECT_FALSE(storage->GetData(nowMinus30us, "<=", tags1).isValid);
                EXPECT_FALSE(storage->GetData(nowMinus30us, "<", tags1).isValid);
                EXPECT_FALSE(storage->GetData(nowMinus30us, "==", tags1).isValid);

                EXPECT_EQ(storage->GetData(nowMinus30us, ">=", tags2).id, "2.bin");
                EXPECT_EQ(storage->GetData(nowMinus30us, ">", tags2).id, "2.bin");
                EXPECT_EQ(storage->GetData(nowMinus30us, "~", tags2).id, "2.bin");
                EXPECT_FALSE(storage->GetData(nowMinus30us, "<=", tags2).isValid);
                EXPECT_FALSE(storage->GetData(nowMinus30us, "<", tags2).isValid);
                EXPECT_FALSE(storage->GetData(nowMinus30us, "==", tags2).isValid);

                EXPECT_EQ(storage->GetData(now, "<=", tags1).id, "5.bin");
                EXPECT_EQ(storage->GetData(now, "<", tags1).id, "5.bin");
                EXPECT_EQ(storage->GetData(now, "~", tags1).id, "5.bin");
                EXPECT_FALSE(storage->GetData(now, ">=", tags1).isValid);
                EXPECT_FALSE(storage->GetData(now, ">", tags1).isValid);
                EXPECT_FALSE(storage->GetData(now, "==", tags1).isValid);

                EXPECT_EQ(storage->GetData(now, "<=", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(now, "<", tags2).id, "4.bin");
                EXPECT_EQ(storage->GetData(now, "~", tags2).id, "4.bin");
                EXPECT_FALSE(storage->GetData(now, ">=", tags2).isValid);
                EXPECT_FALSE(storage->GetData(now, ">", tags2).isValid);
                EXPECT_FALSE(storage->GetData(now, "==", tags2).isValid);
            }

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

    TEST_F(IstoTest, GetsMultipleDataItemsWithSingleQuery) {

        const auto now = isto::now();

        const int totalItemCount = 10;
        for (int i = 0; i < totalItemCount; ++i) {
            const isto::DataItem dataItem(std::to_string(i + 1) + ".bin", sampleDataItem->data, now - std::chrono::microseconds(totalItemCount - i));
            storage->SaveData(dataItem);
        }

        { // gets any items with default params
            const auto dataItems = storage->GetDataItems();
            EXPECT_EQ(dataItems.size(), totalItemCount);
        }

        { // gets any five items
            const auto dataItems = storage->GetDataItems(isto::timestamp_t(), std::chrono::high_resolution_clock::now(), isto::tags_t(), 5, isto::Order::DontCare);
            EXPECT_EQ(dataItems.size(), 5);
        }

        { // gets zero items
            const auto dataItems = storage->GetDataItems(isto::timestamp_t(), std::chrono::high_resolution_clock::now(), isto::tags_t(), 0);
            EXPECT_EQ(dataItems.size(), 0);
        }

        const auto timestampCompare = [](const isto::DataItem& lhs, const isto::DataItem& rhs) {
            return lhs.timestamp < rhs.timestamp;
        };

        { // gets the first five items
            const auto dataItems = storage->GetDataItems(isto::timestamp_t(), std::chrono::high_resolution_clock::now(), isto::tags_t(), 5, isto::Order::Ascending);
            EXPECT_EQ(dataItems.size(), 5);
            if (!dataItems.empty()) {
                EXPECT_EQ(dataItems.front().id, "1.bin");
            }
            EXPECT_TRUE(std::is_sorted(dataItems.begin(), dataItems.end(), timestampCompare));
        }

        { // gets the last five items
            const auto dataItems = storage->GetDataItems(isto::timestamp_t(), std::chrono::high_resolution_clock::now(), isto::tags_t(), 5, isto::Order::Descending);
            EXPECT_EQ(dataItems.size(), 5);
            if (!dataItems.empty()) {
                EXPECT_EQ(dataItems.front().id, std::to_string(totalItemCount) + ".bin");
            }
            EXPECT_TRUE(std::is_sorted(dataItems.rbegin(), dataItems.rend(), timestampCompare));
        }

        { // gets the middle items by exact range
            const auto startTime = now - std::chrono::microseconds(7);
            const auto endTime = now - std::chrono::microseconds(3);
            const auto dataItems = storage->GetDataItems(startTime, endTime);
            EXPECT_EQ(dataItems.size(), 5);
        }
    }

    TEST_F(IstoTest, WorksReasonablyWhenPermanentAndRotatingPointToSameDirectory) {
        isto::Configuration sharedConfiguration;

#ifdef WIN32
        const std::string sharedDirectory = ".\\test-data-shared";
#else // WIN32
        const std::string sharedDirectory = "./test-data-shared";
#endif // WIN32

        fs::remove_all(sharedDirectory);

        sharedConfiguration.rotatingDirectory = sharedDirectory;
        sharedConfiguration.permanentDirectory = sharedDirectory;

        isto::Storage sharedStorage(sharedConfiguration);

        sharedStorage.SaveData(*sampleDataItem);
        EXPECT_TRUE(sharedStorage.MakePermanent(sampleDataItem->id));
        EXPECT_EQ(sharedStorage.GetData(sampleDataItem->id).isPermanent, true);
        EXPECT_TRUE(sharedStorage.MakeRotating(sampleDataItem->id));
        EXPECT_EQ(sharedStorage.GetData(sampleDataItem->id).isPermanent, false);
    }

}  // namespace
