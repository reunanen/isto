//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto.h"
#include <SQLiteCpp/Database.h>

namespace isto {
    
    class Storage::Impl {
    public:
        Impl(const Configuration& configuration);

        void SaveData(const DataItem& dataItem);
        DataItem GetData(const std::string& id);
        DataItem GetData(const std::chrono::high_resolution_clock::time_point& timestamp = std::chrono::high_resolution_clock::now(), const std::string& comparisonOperator = "<=");
        bool MakePermanent(const std::string& id);

    private:
        Configuration configuration;
        std::string baseDirectory;
        SQLite::Database dbRotating;
        SQLite::Database dbPermanent;
    };

};
