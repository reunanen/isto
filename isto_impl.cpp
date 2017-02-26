//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto_impl.h"
#include "SQLiteCpp/sqlite3/sqlite3.h"
#include <boost/filesystem.hpp>
#include "system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"
#include <sstream>

namespace isto {
    Storage::Impl::Impl(const Configuration& configuration)
        : configuration(configuration)
    {
        CreateDirectoriesThatDoNotExist();

        const boost::filesystem::path basePath(configuration.baseDirectory);

        dbRotating = std::unique_ptr<SQLite::Database>(new SQLite::Database((basePath / "rotating" / "isto_rotating.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));
        dbPermanent = std::unique_ptr<SQLite::Database>(new SQLite::Database((basePath / "permanent" / "isto_permanent.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));

        dbRotating->exec("BEGIN EXCLUSIVE");
        dbPermanent->exec("BEGIN EXCLUSIVE");

        CreateTablesThatDoNotExist();
        CreateStatements();
    }

    void Storage::Impl::SaveData(const DataItem& dataItem)
    {
        auto& insert = dataItem.isPermanent ? insertPermanent : insertRotating;

        std::string timestamp = system_clock_time_point_string_conversion::to_string(dataItem.timestamp);

        std::string path;

        insert->bind(1, dataItem.id);
        insert->bind(2, timestamp);
        insert->bind(3, path);
        insert->bind(4, dataItem.data.size());

        insert->executeStep();
        insert->clearBindings();
        insert->reset();
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

    //SQLite::Database& Storage::Impl::GetDatabase(bool isPermanent)
    //{
    //    return isPermanent ? dbPermanent : dbRotating;
    //}

    void Storage::Impl::CreateDirectoriesThatDoNotExist()
    {
        const boost::filesystem::path basePath(configuration.baseDirectory);

        boost::filesystem::create_directories(basePath / "rotating");
        boost::filesystem::create_directories(basePath / "permanent");
    }

    void Storage::Impl::CreateTablesThatDoNotExist()
    {
        std::string createTableStatement = "create table if not exists DataItems (id text primary key, timestamp text, path text, size integer)";

        dbRotating->exec(createTableStatement);
        dbPermanent->exec(createTableStatement);
    }

    void Storage::Impl::CreateStatements()
    {
        insertRotating = std::unique_ptr<SQLite::Statement>(new SQLite::Statement(*dbRotating, "insert into DataItems values (@id, @timestamp, @path, @size)"));
        insertPermanent = std::unique_ptr<SQLite::Statement>(new SQLite::Statement(*dbPermanent, "insert into DataItems values (@id, @timestamp, @path, @size)"));
    }

    void Storage::Impl::DeleteExcessRotatingData()
    {

    }
}