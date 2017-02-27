//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto_impl.h"
#include "SQLiteCpp/sqlite3/sqlite3.h"
//#include "SQLiteCpp/include/SQLiteCpp/Transaction.h"
#include "system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"
#include <boost/filesystem.hpp>
#include <sstream>

namespace isto {
    Storage::Impl::Impl(const Configuration& configuration)
        : configuration(configuration)
    {
        CreateDirectoriesThatDoNotExist();
        CreateDatabases();
        CreateTablesThatDoNotExist();
        CreateStatements();
    }

    void Storage::Impl::SaveData(const DataItem& dataItem)
    {
        auto& insert = dataItem.isPermanent ? insertPermanent : insertRotating;

        const std::string timestamp = system_clock_time_point_string_conversion::to_string(dataItem.timestamp);

        boost::filesystem::create_directories(GetDirectory(dataItem.isPermanent, dataItem.timestamp));

        const std::string path = GetPath(dataItem.isPermanent, dataItem.timestamp, dataItem.id);

        {
            // rewrites an already existing file, if any
            std::ofstream out(path, std::ios::binary);
            out.write(reinterpret_cast<const char*>(dataItem.data.data()), dataItem.data.size());
        }

        insert->bind(1, dataItem.id);
        insert->bind(2, timestamp);
        insert->bind(3, path);
        insert->bind(4, dataItem.data.size());

        insert->executeStep();
        insert->clearBindings();
        insert->reset();

        Flush(GetDatabase(dataItem.isPermanent));
    }

    DataItem Storage::Impl::GetData(const std::string& id)
    {
        // always try permanent first, because probably we have less permanent data
        DataItem permanentDataItem = GetPermanentData(id);
        if (permanentDataItem.isValid) {
            return permanentDataItem;
        }
        DataItem rotatingDataItem = GetRotatingData(id);
        if (rotatingDataItem.isValid) {
            return rotatingDataItem;
        }
        return DataItem::Invalid();
    }

    DataItem Storage::Impl::GetPermanentData(const std::string& id)
    {
        return GetData(GetDatabase(true), id);
    }
    
    DataItem Storage::Impl::GetRotatingData(const std::string& id)
    {
        return GetData(GetDatabase(false), id);
    }

    DataItem Storage::Impl::GetData(std::unique_ptr<SQLite::Database>& db, const std::string& id)
    {
        const std::string select = "select timestamp, path, size from DataItems where id = '" + id + "'";

        SQLite::Statement query(*db, select);

        if (query.executeStep()) {
            const std::string timestampString = query.getColumn(0);
            const std::string path = query.getColumn(1);
            const size_t size = query.getColumn(2);

            std::vector<unsigned char> data(size);

            if (size > 0) {
                std::ifstream in(path, std::ios::binary);
                in.read(reinterpret_cast<char*>(&data[0]), size);
            }

            assert(!query.executeStep()); // we don't expect there's another item

            const auto timestamp = system_clock_time_point_string_conversion::from_string(timestampString);

            return DataItem(id, data, timestamp, db == dbPermanent);
        }
        else {
            return DataItem::Invalid();
        }
    }

    DataItem Storage::Impl::GetData(const std::chrono::high_resolution_clock::time_point& timestamp, const std::string& comparisonOperator)
    {
        return DataItem::Invalid();
    }

    std::string Storage::Impl::GetDirectory(bool isPermanent, const timestamp_t& timestamp) const
    {
        const std::string timestampString = system_clock_time_point_string_conversion::to_string(timestamp);
        return (boost::filesystem::path(GetSubDir(isPermanent)) / timestampString.substr(0, 10) / timestampString.substr(11, 2) / timestampString.substr(14, 2)).string();
    }

    std::string Storage::Impl::GetPath(bool isPermanent, const timestamp_t& timestamp, const std::string& id) const
    {
        // note that the id doubles as a filename
        return (boost::filesystem::path(GetDirectory(isPermanent, timestamp)) / id).string();
    }

    bool Storage::Impl::MakePermanent(const std::string& id)
    {
        return MoveDataItem(false, true, id);
    }

    bool Storage::Impl::MakeRotating(const std::string& id)
    {
        return MoveDataItem(true, false, id);
    }

    bool Storage::Impl::MoveDataItem(bool sourceIsPermanent, bool destinationIsPermanent, const std::string& id)
    {
        assert(sourceIsPermanent != destinationIsPermanent);

        // TODO: could be optimized by moving the file (see boost::filesystem::rename) - instead of reading, writing, and deleting

        std::unique_ptr<SQLite::Database>& dbSource = GetDatabase(sourceIsPermanent);

        const DataItem dataItem = GetData(dbSource, id);
        if (!dataItem.isValid) {
            return false;
        }
        else {
            assert(dataItem.isPermanent != destinationIsPermanent);
            const DataItem newDataItem(dataItem.id, dataItem.data, dataItem.timestamp, destinationIsPermanent);
            SaveData(newDataItem);            
            DeleteItem(sourceIsPermanent, dataItem.timestamp, dataItem.id);
            return true;
        }
    }

    void Storage::Impl::DeleteItem(bool isPermanent, const timestamp_t& timestamp, const std::string& id)
    {
        boost::filesystem::path sourcePath = GetPath(isPermanent, timestamp, id);
        boost::filesystem::remove(sourcePath);

        int deleted = GetDatabase(isPermanent)->exec("delete from DataItems where id = '" + id + "'");

        Flush(GetDatabase(isPermanent));
    }

    void Storage::Impl::Flush(std::unique_ptr<SQLite::Database>& db)
    {
        db->exec("commit");
        db->exec("begin exclusive");
    }

    std::unique_ptr<SQLite::Database>& Storage::Impl::GetDatabase(bool isPermanent)
    {
        return isPermanent ? dbPermanent : dbRotating;
    }

    std::string Storage::Impl::GetSubDir(bool isPermanent) const
    {
        return (boost::filesystem::path(configuration.baseDirectory) / (isPermanent ? "permanent" : "rotating")).string();
    }

    void Storage::Impl::CreateDirectoriesThatDoNotExist()
    {
        const boost::filesystem::path basePath(configuration.baseDirectory);

        boost::filesystem::create_directories(GetSubDir(false));
        boost::filesystem::create_directories(GetSubDir(true));
    }

    void Storage::Impl::CreateDatabases()
    {
        const boost::filesystem::path basePath(configuration.baseDirectory);

        dbRotating = std::unique_ptr<SQLite::Database>(new SQLite::Database((boost::filesystem::path(GetSubDir(false)) / "isto_rotating.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));
        dbPermanent = std::unique_ptr<SQLite::Database>(new SQLite::Database((boost::filesystem::path(GetSubDir(true)) / "isto_permanent.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));

        dbRotating->exec("begin exclusive");
        dbPermanent->exec("begin exclusive");
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