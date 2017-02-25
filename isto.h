//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef ISTO_H
#define ISTO_H

#include <chrono>
#include <vector>

namespace isto {
    
    struct DataItem {
        DataItem(const std::string& id, const char* dataBegin, const char* dataEnd,
            std::chrono::high_resolution_clock::time_point& timestamp = std::chrono::high_resolution_clock::now());

        DataItem(const std::string& id, const std::vector<unsigned char>& data,
            std::chrono::high_resolution_clock::time_point& timestamp = std::chrono::high_resolution_clock::now());

        static DataItem Invalid();

        const std::chrono::high_resolution_clock::time_point timestamp = std::chrono::high_resolution_clock::now();
        const std::string id; // for example, a guid
        const std::vector<unsigned char> data;
        const bool isValid = false;

    private:
        DataItem();
    };

    struct Configuration {
        std::string baseDirectory = "./data";

        double maxRotatingDataToKeepInGiB = 100.0;
        double minFreeDiskSpaceInGiB = 0.5;
    };

    class Storage {
    public:
        Storage(const Configuration& configuration = Configuration());
        ~Storage();
        
        void SaveData(const DataItem& dataItem);

        // Get data by id
        DataItem GetData(const std::string& id);

        // Get data by timestamp
        // - supported comparison operators: "<", "<=", "==", ">=", ">"
        DataItem GetData(const std::chrono::high_resolution_clock::time_point& timestamp = std::chrono::high_resolution_clock::now(), const std::string& comparisonOperator = "<=");

        // Keep a certain data item forever
        // - for example, if manually labeled in a supervised training setting
        bool MakePermanent(const std::string& id);

    private:
        class Impl;
        Impl* impl;
    };

};

#endif // ISTO_H