//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto_impl.h"
#include "SQLiteCpp/sqlite3/sqlite3.h"
//#include <boost/filesystem.hpp>

namespace isto {
    Storage::Impl::Impl(const Configuration& configuration)
        : configuration(configuration)
        //, baseDirectory(boost::filesystem::path(configuration.baseDirectory)).string()
        , baseDirectory(configuration.baseDirectory)
        , dbRotating(baseDirectory + "/isto_rotating.sqlite", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
        , dbPermanent(baseDirectory + "/isto_permanent.sqlite", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
    {
        dbRotating.exec("BEGIN EXCLUSIVE");
        dbPermanent.exec("BEGIN EXCLUSIVE");
    }

    void Storage::Impl::SaveData(const DataItem& dataItem)
    {
    }

    DataItem Storage::Impl::GetData(const std::string& id)
    {
        return DataItem::Invalid();
    }

    DataItem Storage::Impl::GetData(const std::chrono::high_resolution_clock::time_point& timestamp, const std::string& comparisonOperator)
    {
        return DataItem::Invalid();
    }

    bool Storage::Impl::MakePermanent(const std::string& id)
    {
        return false;
    }
}