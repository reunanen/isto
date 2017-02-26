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

            // Clean up existing databases, if any.
            isto::Configuration defaultConfiguration;
            boost::filesystem::remove_all(defaultConfiguration.baseDirectory);

            storage = std::unique_ptr<isto::Storage>(new isto::Storage(defaultConfiguration));
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

        std::unique_ptr<isto::Storage> storage;
    };

    // Tests that an image storage can be set up.
    TEST_F(IstoTest, CanBeSetUp) {
    }

    TEST_F(IstoTest, CannotCreateDuplicateInstance) {
        ASSERT_THROW(isto::Storage(), std::exception);
    }

    TEST_F(IstoTest, SavesData) {
        std::vector<unsigned char> data(256);
        std::iota(data.begin(), data.end(), 0);

        const isto::DataItem dataItem("asdf.bin", data);

        storage->SaveData(dataItem);

        const isto::DataItem retrievedDataItem = storage->GetData("asdf.bin");

        EXPECT_EQ(retrievedDataItem.id, dataItem.id);
        EXPECT_EQ(retrievedDataItem.data, dataItem.data);
        EXPECT_EQ(retrievedDataItem.isPermanent, dataItem.isPermanent);
        EXPECT_EQ(retrievedDataItem.isValid, dataItem.isValid);

        // The timestamp may have been rounded.
        EXPECT_LT(std::chrono::duration_cast<std::chrono::microseconds>(retrievedDataItem.timestamp - dataItem.timestamp).count(), 1);

        // Can't insert a duplicate entry
        EXPECT_THROW(storage->SaveData(dataItem), std::exception);
    }

}  // namespace
