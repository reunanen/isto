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
    
    typedef std::chrono::system_clock::time_point timestamp_t;

    timestamp_t now();

    struct DataItem {
        DataItem(const std::string& id, const char* dataBegin, const char* dataEnd, const timestamp_t& timestamp = now(), bool isPermanent = false);
        DataItem(const std::string& id, const std::vector<unsigned char>& data, const timestamp_t& timestamp = now(), bool isPermanent = false);

        static DataItem Invalid();

        const std::string id; // for example, a guid (NB: should qualify as a filename too!)
        const std::vector<unsigned char> data;
        const timestamp_t timestamp;
        const bool isPermanent;
        const bool isValid;

    private:
        DataItem();
    };

    struct Configuration {
        std::string baseDirectory = 
#ifdef _WIN32
            ".\\data"
#else // _WIN32
            "./data"
#endif // _WIN32
            ;

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
        DataItem GetData(const timestamp_t& timestamp = std::chrono::high_resolution_clock::now(), const std::string& comparisonOperator = "<=");

        // Keep a certain data item forever
        // - for example, if manually labeled in a supervised training setting
        bool MakePermanent(const std::string& id);

        // Unmake permanent
        bool MakeRotating(const std::string& id);

    private:
        class Impl;
        Impl* impl;
    };

};

#endif // ISTO_H