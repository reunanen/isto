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
        InitializeCurrentDataItemBytes();
    }

    bool Storage::Impl::SaveData(const DataItem& dataItem)
    {
        if (!dataItem.isPermanent) {
            if (!DeleteExcessRotatingData(dataItem.data.size())) {
                return false;
            }
            currentRotatingDataItemBytes += dataItem.data.size();
        }

        auto& insert = dataItem.isPermanent ? insertPermanent : insertRotating;

        const std::string timestamp = system_clock_time_point_string_conversion::to_string(dataItem.timestamp);

        boost::filesystem::create_directories(GetDirectory(dataItem.isPermanent, dataItem.timestamp));

        const std::string path = GetPath(dataItem.isPermanent, dataItem.timestamp, dataItem.id);

        {
            // rewrites an already existing file, if any
            std::ofstream out(path, std::ios::binary);
            out.write(reinterpret_cast<const char*>(dataItem.data.data()), dataItem.data.size());
        }

        int index = 0;
        insert->bind(++index, dataItem.id);
        insert->bind(++index, timestamp);
        insert->bind(++index, path);
        insert->bind(++index, dataItem.data.size());

        auto tags = dataItem.tags;

        for (const std::string& tag : configuration.tags) {
            tags[tag]; // initialize possibly missing tags
        }

        for (const std::string& tag : configuration.tags) {
            insert->bind(++index, tags[tag]);
        }

        insert->executeStep();
        insert->clearBindings();
        insert->reset();

        Flush(GetDatabase(dataItem.isPermanent));

        return true;
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
        std::ostringstream select;
        select << "select timestamp, path, size";
            
        for (const std::string& tag : configuration.tags) {
            select << ", " << tag;
        }

        select << " from DataItems where id = '" + id + "'";

        SQLite::Statement query(*db, select.str());

        if (query.executeStep()) {
            int index = 0;
            const std::string timestampString = query.getColumn(index++);
            const std::string path = query.getColumn(index++);
            const size_t size = query.getColumn(index++);

            tags_t tags;
            for (const std::string& tag : configuration.tags) {
                tags[tag] = query.getColumn(index++);
            }

            std::vector<unsigned char> data(size);

            if (size > 0) {
                std::ifstream in(path, std::ios::binary);
                in.read(reinterpret_cast<char*>(&data[0]), size);
            }

            assert(!query.executeStep()); // we don't expect there's another item

            const auto timestamp = system_clock_time_point_string_conversion::from_string(timestampString);

            return DataItem(id, data, timestamp, db == dbPermanent, tags);
        }
        else {
            return DataItem::Invalid();
        }
    }

    DataItem Storage::Impl::GetData(const timestamp_t& timestamp, const std::string& comparisonOperator, const tags_t& tags)
    {
        const auto matchedTimestampAndCorrespondingDatabase = FindMatchingTimestampAndCorrespondingDatabase(timestamp, comparisonOperator, tags);

        if (matchedTimestampAndCorrespondingDatabase.first.empty()) {
            assert(matchedTimestampAndCorrespondingDatabase.second.get() == nullptr);
            return DataItem::Invalid();
        }

        std::string select = "select id from DataItems where timestamp = '" + matchedTimestampAndCorrespondingDatabase.first + "'";

        for (const auto& tag : tags) {
            select += " and " + tag.first + " = '" + tag.second + "'";
        }

        SQLite::Statement query(*matchedTimestampAndCorrespondingDatabase.second, select);

        if (query.executeStep()) {
            const std::string id = query.getColumn(0);
            return GetData(matchedTimestampAndCorrespondingDatabase.second, id);
        }
        else {
            return DataItem::Invalid();
        }
    }

    std::pair<std::string, std::unique_ptr<SQLite::Database>&> Storage::Impl::FindMatchingTimestampAndCorrespondingDatabase(
        const std::chrono::high_resolution_clock::time_point& timestamp,
        const std::string& comparisonOperator,
        const tags_t& tags)
    {
        const std::string timestampString = system_clock_time_point_string_conversion::to_string(timestamp);

        const auto noResult = [this]() {
            return std::pair<std::string, std::unique_ptr<SQLite::Database>&>("", dbNone);
        };

        const auto getRotatingAndPermanentTimestamps = [&](const std::string& select) {
            std::string rotatingTimestamp, permanentTimestamp;
            SQLite::Statement queryRotating(*dbRotating, select);
            SQLite::Statement queryPermanent(*dbPermanent, select);
            if (queryRotating.executeStep()) {
                rotatingTimestamp = queryRotating.getColumn(0);
            }
            if (queryPermanent.executeStep()) {
                permanentTimestamp = queryPermanent.getColumn(0);
            }
            return std::pair<std::string, std::string>(rotatingTimestamp, permanentTimestamp);
        };

        if (comparisonOperator == "<" || comparisonOperator == "<=" || comparisonOperator == ">=" || comparisonOperator == ">") {
            std::string select = "select ";
            if (comparisonOperator == "<" || comparisonOperator == "<=") {
                select += "max(timestamp)";
            }
            else {
                select += "min(timestamp)";
            }
            select += " from DataItems where timestamp " + comparisonOperator + "'" + timestampString + "'";

            for (const auto& tag : tags) {
                select += " and " + tag.first + " = '" + tag.second + "'";
            }

            const auto matchedTimestamps = getRotatingAndPermanentTimestamps(select);
            if (!matchedTimestamps.first.empty() && !matchedTimestamps.second.empty()) {
                const auto rotatingTimestamp = system_clock_time_point_string_conversion::from_string(matchedTimestamps.first);
                const auto permanentTimestamp = system_clock_time_point_string_conversion::from_string(matchedTimestamps.second);
                if (abs((rotatingTimestamp - timestamp).count()) < abs((permanentTimestamp - timestamp).count())) {
                    return std::pair<std::string, std::unique_ptr<SQLite::Database>&>(matchedTimestamps.first, dbRotating);
                }
                else {
                    return std::pair<std::string, std::unique_ptr<SQLite::Database>&>(matchedTimestamps.second, dbPermanent);
                }
            }
            else if (!matchedTimestamps.first.empty()) {
                return std::pair<std::string, std::unique_ptr<SQLite::Database>&>(matchedTimestamps.first, dbRotating);
            }
            else if (!matchedTimestamps.second.empty()) {
                return std::pair<std::string, std::unique_ptr<SQLite::Database>&>(matchedTimestamps.second, dbPermanent);
            }
            else {
                return noResult();
            }
        }
        else if (comparisonOperator == "==") {
            const auto bestPrevious = FindMatchingTimestampAndCorrespondingDatabase(timestamp, "<=", tags);
            if (bestPrevious.first == timestampString) {
                return bestPrevious;
            }
            else {
                return noResult();
            }
        }
        else if (comparisonOperator == "~") {
            const auto bestPrevious = FindMatchingTimestampAndCorrespondingDatabase(timestamp, "<=", tags);
            const auto bestNext = FindMatchingTimestampAndCorrespondingDatabase(timestamp, ">=", tags);
            if (!bestPrevious.first.empty() && !bestNext.first.empty()) {
                const auto previousTimestamp = system_clock_time_point_string_conversion::from_string(bestPrevious.first);
                const auto nextTimestamp = system_clock_time_point_string_conversion::from_string(bestNext.first);
                if (abs((previousTimestamp - timestamp).count()) <= abs((nextTimestamp - timestamp).count())) {
                    return bestPrevious;
                }
                else {
                    return bestNext;
                }
            }
            else if (!bestPrevious.first.empty()) {
                return bestPrevious;
            }
            else if (!bestNext.first.empty()) {
                return bestNext;
            }
            else {
                return noResult();
            }
        }
        else {
            return noResult();
        }
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
            const DataItem newDataItem(dataItem.id, dataItem.data, dataItem.timestamp, destinationIsPermanent, dataItem.tags);
            if (SaveData(newDataItem)) {
                DeleteItem(sourceIsPermanent, dataItem.timestamp, dataItem.id);
                Flush(GetDatabase(sourceIsPermanent));
                if (!sourceIsPermanent) {
                    assert(currentRotatingDataItemBytes >= dataItem.data.size());
                    currentRotatingDataItemBytes -= dataItem.data.size();
                }
                return true;
            }
            else {
                return false;
            }
        }
    }

    void Storage::Impl::DeleteItem(bool isPermanent, const timestamp_t& timestamp, const std::string& id)
    {
        boost::filesystem::path sourcePath = GetPath(isPermanent, timestamp, id);
        boost::filesystem::remove(sourcePath);

        int deleted = GetDatabase(isPermanent)->exec("delete from DataItems where id = '" + id + "'");
        assert(deleted == 1);
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
        return boost::filesystem::path(isPermanent ? configuration.permanentDirectory : configuration.rotatingDirectory).string();
    }

    void Storage::Impl::CreateDirectoriesThatDoNotExist()
    {
        boost::filesystem::create_directories(GetSubDir(false));
        boost::filesystem::create_directories(GetSubDir(true));
    }

    void Storage::Impl::CreateDatabases()
    {
        dbRotating = std::unique_ptr<SQLite::Database>(new SQLite::Database((boost::filesystem::path(GetSubDir(false)) / "isto_rotating.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));
        dbPermanent = std::unique_ptr<SQLite::Database>(new SQLite::Database((boost::filesystem::path(GetSubDir(true)) / "isto_permanent.sqlite").string(), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));

        dbRotating->exec("begin exclusive");
        dbPermanent->exec("begin exclusive");
    }

    void Storage::Impl::CreateTablesThatDoNotExist()
    {
        std::ostringstream createTableStatement;
        createTableStatement << "create table if not exists DataItems (id text primary key, timestamp text, path text, size integer";

        for (const std::string& tag : configuration.tags) {
            if (tag.find_first_of(" \t\n") != std::string::npos) {
                throw std::runtime_error("Tag names must not contain whitespace");
            }
            createTableStatement << ", " << tag << " text";
        }

        createTableStatement << ")";

        dbRotating->exec(createTableStatement.str());
        dbPermanent->exec(createTableStatement.str());
    }

    void Storage::Impl::CreateStatements()
    {
        std::ostringstream insertStatement;
        insertStatement << "insert into DataItems values (@id, @timestamp, @path, @size";

        for (const std::string& tag : configuration.tags) {
            insertStatement << ", @" << tag;
        }

        insertStatement << ")";

        insertRotating = std::unique_ptr<SQLite::Statement>(new SQLite::Statement(*dbRotating, insertStatement.str()));
        insertPermanent = std::unique_ptr<SQLite::Statement>(new SQLite::Statement(*dbPermanent, insertStatement.str()));
    }

    void Storage::Impl::InitializeCurrentDataItemBytes()
    {
        const std::string select = "select sum(size) from DataItems";
        SQLite::Statement query(*dbRotating, select);

        if (query.executeStep()) {
            currentRotatingDataItemBytes = query.getColumn(0);
        }
        else {
            throw std::runtime_error("Unable to initialize current data item bytes");
        }
    }

    bool Storage::Impl::DeleteExcessRotatingData(size_t sizeToBeInserted)
    {
        auto hardDiskFreeBytes = boost::filesystem::space(boost::filesystem::path(configuration.rotatingDirectory)).free;

        const auto hasExcessData = [&]() {
            return currentRotatingDataItemBytes + sizeToBeInserted > configuration.maxRotatingDataToKeepInGiB * 1024 * 1024 * 1024
                || hardDiskFreeBytes - sizeToBeInserted < configuration.minFreeDiskSpaceInGiB * 1024 * 1024 * 1024;
        };

        if (hasExcessData()) {
            const std::string select = "select id, timestamp, size from DataItems order by timestamp asc";
            SQLite::Statement query(*dbRotating, select);
            while (hasExcessData() && query.executeStep()) {
                const std::string id = query.getColumn(0);
                const std::string timestampString = query.getColumn(1);
                const size_t size = query.getColumn(2);

                assert(currentRotatingDataItemBytes >= size);

                const auto timestamp = system_clock_time_point_string_conversion::from_string(timestampString);
                DeleteItem(false, timestamp, id);

                currentRotatingDataItemBytes -= size;
                hardDiskFreeBytes += size;
            }
        }

        return !hasExcessData();
    }
}