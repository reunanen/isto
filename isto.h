//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef ISTO_H
#define ISTO_H

#include <chrono>
#include <vector>
#include <deque>
#include <unordered_map>
#include <functional>

namespace isto {
    
    typedef std::chrono::system_clock::time_point timestamp_t;
    typedef std::unordered_map<std::string, std::string> tags_t;

    typedef std::function<void(const std::string&)> rotating_data_deleted_callback_t;

    timestamp_t now();

    struct DataItem {
        DataItem(const std::string& id, const char* dataBegin, const char* dataEnd, const timestamp_t& timestamp = now(), bool isPermanent = false, const tags_t& tags = tags_t());
        DataItem(const std::string& id, const std::vector<unsigned char>& data, const timestamp_t& timestamp = now(), bool isPermanent = false, const tags_t& tags = tags_t());
        DataItem(const std::string& id, const std::string& data, const timestamp_t& timestamp = now(), bool isPermanent = false, const tags_t& tags = tags_t());

        static DataItem Invalid();

        const std::string id; // for example, a guid (NB: should qualify as a filename too!)
        const std::vector<unsigned char> data;
        const timestamp_t timestamp;
        const bool isPermanent;
        const bool isValid;

        const tags_t tags;

    private:
        DataItem();
    };

    typedef std::vector<DataItem> DataItems;

    struct Configuration {
#ifdef _WIN32
        std::string rotatingDirectory = ".\\data\\rotating";
        std::string permanentDirectory = ".\\data\\permanent";
#else // _WIN32
        std::string rotatingDirectory = "./data/rotating";
        std::string permanentDirectory = "./data/permanent";
#endif // _WIN32

        double maxRotatingDataToKeepInGiB = 100.0;
        double minFreeDiskSpaceInGiB = 0.5;

        std::vector<std::string> tags; // tags are string values like "camera": "1" or "detected_size": "too large"

        unsigned int deletionFlushInterval = 1000;
    };

    enum class Order {
        DontCare,
        Ascending,
        Descending
    };

    class Storage {
    public:
        Storage(const Configuration& configuration = Configuration());
        ~Storage();
        
        void SaveData(const DataItem& dataItem, bool upsert = false);

        void SaveData(const DataItems& dataItems, bool upsert = false);

        // Get data by id
        DataItem GetData(const std::string& id);

        // Get data by timestamp
        // - supported comparison operators: "<", "<=", "==", ">=", ">", "~" (nearest)
        DataItem GetData(const timestamp_t& timestamp = std::chrono::system_clock::now(), const std::string& comparisonOperator = "~", const tags_t& tags = tags_t());

        // Get potentially multiple data items by start time and end time (which are both inclusive)
        DataItems GetDataItems(
            const timestamp_t& startTime = timestamp_t(),
            const timestamp_t& endTime = std::chrono::high_resolution_clock::now(),
            const tags_t& tags = tags_t(),
            const size_t maxItems = 1000,
            Order order = Order::DontCare
        );

        // Keep a certain data item forever
        // - for example, if manually labeled in a supervised training setting
        bool MakePermanent(const std::string& id);

        // Unmake permanent
        bool MakeRotating(const std::string& id);

        // Leave timestamps empty in order not to limit the search
        std::deque<std::string> GetIdsSortedByAscendingTimestamp(const std::string& timestampBegin = "", const std::string& timestampEnd = "") const;

        void SetRotatingDataDeletedCallback(const rotating_data_deleted_callback_t& callback);

    private:
        class Impl;
        Impl* impl;
    };

};

#endif // ISTO_H