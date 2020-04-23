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
#include <numeric> // std::accumulate
#include <sstream>
#include <unordered_set>

namespace isto {
    Storage::Impl::Impl(const Configuration& configuration)
        : configuration(configuration)
    {
        CreateDirectoriesThatDoNotExist();
        CreateDatabases();
        CreateTablesThatDoNotExist();
        CreateIndexesThatDoNotExist();
        CreateStatements();
        InitializeCurrentDataItemBytes();
    }

    bool Storage::Impl::SaveData(const DataItem& dataItem, bool upsert)
    {
        return SaveData(&dataItem, 1, upsert);
    }

    bool Storage::Impl::SaveData(const DataItems& dataItems, bool upsert)
    {
        if (dataItems.empty()) {
            return false;
        }
        return SaveData(&dataItems[0], dataItems.size(), upsert);
    }

    bool Storage::Impl::SaveData(const DataItem* dataItems, size_t dataItemCount, bool upsert)
    {
        { // Make sure we have enough space - TODO: when upserting, subtract from the total needed size the sizes of the files that will now be overwritten
            const size_t totalRotatingSizeNeeded = std::accumulate(dataItems, dataItems + dataItemCount, static_cast<size_t>(0),
                [](size_t total, const DataItem& dataItem) {
                    return total + (dataItem.isPermanent ? 0 : dataItem.data.size());
                });

            if (!DeleteExcessRotatingData(totalRotatingSizeNeeded)) {
                return false;
            }
        }

        bool flushPermanent = false;
        bool flushRotating = false;

        std::vector<std::string> directories(dataItemCount), paths(dataItemCount);
        std::unordered_set<std::string> uniqueDirectories;

        for (size_t i = 0; i < dataItemCount; ++i) {
            const DataItem& dataItem = dataItems[i];
            const std::string directory = GetDirectory(dataItem.isPermanent, dataItem.timestamp, configuration.directoryStructureResolution);
            directories[i] = directory;
            uniqueDirectories.insert(directory);

            paths[i] = GetPath(dataItem.isPermanent, dataItem.timestamp, dataItem.id, configuration.directoryStructureResolution);
        }

        std::unordered_set<std::string> createdDirectories;

        for (const auto& directory : uniqueDirectories) {
            if (!boost::filesystem::exists(directory)) {
                boost::filesystem::create_directories(directory);
                createdDirectories.insert(directory);
            }
        }

        assert(createdDirectories.size() <= uniqueDirectories.size());

        typedef std::unique_ptr<uintmax_t> FileSizeIfAny;
        typedef std::future<FileSizeIfAny> GetExistingFileSizeOperation;

        std::vector<std::unique_ptr<GetExistingFileSizeOperation>> getExistingFileSizeOperations(dataItemCount);

        const auto getFileSize = [&](size_t i) {
            if (boost::filesystem::exists(paths[i])) {
                return std::make_unique<uintmax_t>(boost::filesystem::file_size(paths[i]));
            }
            else {
                return std::unique_ptr<uintmax_t>();
            }
        };

        for (size_t i = 0; i < dataItemCount; ++i) {
            const bool directoryExistedBefore = createdDirectories.find(directories[i]) == createdDirectories.end();

            auto getFileSizeOperation = directoryExistedBefore
                ? std::async(std::launch::async, getFileSize, i)
                : std::async(std::launch::deferred, []() { return std::unique_ptr<uintmax_t>(); });

            getExistingFileSizeOperations[i] = std::make_unique<GetExistingFileSizeOperation>(std::move(getFileSizeOperation));
        }

        std::vector<std::unique_ptr<std::future<void>>> fileWriteOperations(dataItemCount);

        const auto writeFile = [&](size_t i) {
            const DataItem& dataItem = dataItems[i];

            std::ofstream out(paths[i], std::ios::binary);
            out.write(reinterpret_cast<const char*>(dataItem.data.data()), dataItem.data.size());
        };

        std::deque<std::string> filesThatAlreadyExistWhenNotUpserting;

        for (size_t i = 0; i < dataItemCount; ++i) {

            const auto startFileWriteOperation = [&]() {
                fileWriteOperations[i] = std::make_unique<std::future<void>>(std::async(std::launch::async, writeFile, i));
            };

            const auto existingFileSize = getExistingFileSizeOperations[i]->get();
            if (existingFileSize.get()) {
                if (upsert) {
                    // file exists, but we're upserting
                    currentRotatingDataItemBytes -= *existingFileSize;
                    startFileWriteOperation();
                }
                else {
                    // file exists and not upserting - this is an error
                    filesThatAlreadyExistWhenNotUpserting.push_back(paths[i]);
                }
            }
            else {
                // the file did not exist before
                startFileWriteOperation();
            }
        }

        for (size_t i = 0; i < dataItemCount; ++i) {

            const bool fileWriteOperationWasActuallyStarted = fileWriteOperations[i].get() != nullptr;

            if (fileWriteOperationWasActuallyStarted) { // was a file write operation actually started?
                const DataItem& dataItem = dataItems[i];

                InsertDataItem(dataItem);

                if (dataItem.isPermanent) {
                    flushPermanent = true;
                }
                else {
                    flushRotating = true;
                }

                currentRotatingDataItemBytes += dataItem.data.size();
            }
        }

        if (flushPermanent) {
            FlushPermanent();
        }

        if (flushRotating) {
            FlushRotating();
        }

        for (auto& fileWriteOperation : fileWriteOperations) {
            if (fileWriteOperation.get()) {
                fileWriteOperation->get(); // wait for the operation to complete
            }
        }

        if (!filesThatAlreadyExistWhenNotUpserting.empty()) {
            assert(!upsert);
            std::string error;
            if (filesThatAlreadyExistWhenNotUpserting.size() == 1) {
                error = "File " + filesThatAlreadyExistWhenNotUpserting[0] + " already exists";
            }
            else {
                error = "Files that already exist:";
                for (const auto& filename : filesThatAlreadyExistWhenNotUpserting) {
                    error += "\n" + filename;
                }
            }
            throw std::runtime_error(error);
        }

        return true;
    }

    void Storage::Impl::InsertDataItem(const DataItem& dataItem)
    {
        const std::string timestamp = system_clock_time_point_string_conversion::to_string(dataItem.timestamp);
        const std::string path = GetPath(dataItem.isPermanent, dataItem.timestamp, dataItem.id, configuration.directoryStructureResolution);

        auto& insert = dataItem.isPermanent ? insertPermanent : insertRotating;

        int index = 0;
        insert->bind(++index, dataItem.id);
        insert->bind(++index, timestamp);
        insert->bind(++index, path);

#if SIZE_MAX > 0xffffffff
        // 64-bit system
        insert->bind(++index, static_cast<int64_t>(dataItem.data.size()));
#else
        // 32-bit system
        insert->bind(++index, dataItem.data.size());
#endif

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

    DataItem FromFuture(std::future<std::unique_ptr<DataItem>>& future)
    {
        return DataItem(*future.get().get());
    }

    DataItem Storage::Impl::GetPermanentData(const std::string& id)
    {
        return FromFuture(GetData(GetDatabase(true), id, std::launch::deferred));
    }
    
    DataItem Storage::Impl::GetRotatingData(const std::string& id)
    {
        return FromFuture(GetData(GetDatabase(false), id, std::launch::deferred));
    }

    std::future<std::unique_ptr<DataItem>> Storage::Impl::GetData(std::unique_ptr<SQLite::Database>& db, const std::string& id, std::launch preferredLaunchMode)
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

#if SIZE_MAX > 0xffffffff
            // 64-bit system
            const size_t size = query.getColumn(index++).getInt64();
#else
            // 32-bit system
            const size_t size = query.getColumn(index++);
#endif

            tags_t tags;
            for (const std::string& tag : configuration.tags) {
                tags[tag] = query.getColumn(index++);
            }

            const bool isPermanent = (db == dbPermanent);

            assert(!query.executeStep()); // we don't expect there's another item

            return std::async(std::launch::async, [id, timestampString, path, size, tags, isPermanent]() {
                std::vector<unsigned char> data(size);

                if (size > 0) {
                    std::ifstream in(path, std::ios::binary);
                    in.read(reinterpret_cast<char*>(&data[0]), size);
                }

                const auto timestamp = system_clock_time_point_string_conversion::from_string(timestampString);

                return std::make_unique<DataItem>(DataItem(id, data, timestamp, isPermanent, tags));
            });
        }
        else {
            return std::async(std::launch::deferred, []() { return std::make_unique<DataItem>(DataItem::Invalid()); });
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
            return FromFuture(GetData(matchedTimestampAndCorrespondingDatabase.second, id, std::launch::deferred));
        }
        else {
            return DataItem::Invalid();
        }
    }

    DataItems Storage::Impl::GetDataItems(const timestamp_t& startTime, const timestamp_t& endTime, const tags_t& tags, size_t maxItems, Order order)
    {
        DataItems rotatingDataItems  = GetDataItems(dbRotating,  startTime, endTime, tags, maxItems, order);
        DataItems permanentDataItems = GetDataItems(dbPermanent, startTime, endTime, tags, maxItems, order);

        if (permanentDataItems.empty()) { return rotatingDataItems;  }
        if (rotatingDataItems.empty())  { return permanentDataItems; }

        std::vector<const DataItem*> allDataItems;
        allDataItems.reserve(rotatingDataItems.size() + permanentDataItems.size());

        for (const auto& i : rotatingDataItems)  { allDataItems.push_back(&i); }
        for (const auto& i : permanentDataItems) { allDataItems.push_back(&i); }

        const auto timestampCompare = [](const DataItem* lhs, const DataItem* rhs) {
            return lhs->timestamp < rhs->timestamp;
        };

        if (order == Order::Ascending) {
            std::sort(allDataItems.begin(), allDataItems.end(), timestampCompare);
        }
        else if (order == Order::Descending) {
            std::sort(allDataItems.rbegin(), allDataItems.rend(), timestampCompare);
        }

        const size_t resultSize = std::min(allDataItems.size(), maxItems);

        DataItems result;
        result.reserve(resultSize);

        for (size_t i = 0; i < resultSize; ++i) {
            result.push_back(std::move(*allDataItems[i]));
        }

        return result;
    }

    DataItems Storage::Impl::GetDataItems(std::unique_ptr<SQLite::Database>& db, const timestamp_t& startTime, const timestamp_t& endTime, const tags_t& tags, size_t maxItems, Order order)
    {
        std::string select = "select id from DataItems where "
            "timestamp >= '" + system_clock_time_point_string_conversion::to_string(startTime) + "' and "
            "timestamp <= '" + system_clock_time_point_string_conversion::to_string(endTime) + "'";

        for (const auto& tag : tags) {
            select += " and " + tag.first + " = '" + tag.second + "'";
        }

        if (order == Order::Ascending) {
            select += " order by timestamp asc";
        }
        else if (order == Order::Descending) {
            select += " order by timestamp desc";
        }

        select += " limit " + std::to_string(maxItems);

        SQLite::Statement query(*db, select);

        std::deque<std::future<std::unique_ptr<DataItem>>> futures;

        while (query.executeStep()) {
            const std::string id = query.getColumn(0);
            const auto preferredLaunchMode = futures.empty() ? std::launch::deferred : std::launch::async;
            futures.emplace_back(GetData(db, id, preferredLaunchMode));
        }

        DataItems dataItems;
        dataItems.reserve(futures.size());

        for (auto& future : futures) {
            dataItems.emplace_back(FromFuture(future));
        }

        return dataItems;
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

    std::string Storage::Impl::GetDirectory(bool isPermanent, const timestamp_t& timestamp, Configuration::DirectoryStructureResolution resolution) const
    {
        const std::string timestampString = system_clock_time_point_string_conversion::to_string(timestamp);

        const auto getDaysDirectory = [&]() {
            return boost::filesystem::path(GetSubDir(isPermanent)) / timestampString.substr(0, 10);
        };
        const auto getHoursDirectory = [&]() {
            return getDaysDirectory() / timestampString.substr(11, 2);
        };
        const auto getMinutesDirectory = [&]() {
            return getHoursDirectory() / timestampString.substr(14, 2);
        };

        switch (resolution) {
        case Configuration::DirectoryStructureResolution::Minutes: return getMinutesDirectory().string();
        case Configuration::DirectoryStructureResolution::Hours:   return getHoursDirectory()  .string();
        case Configuration::DirectoryStructureResolution::Days:    return getDaysDirectory()   .string();
        default: throw std::runtime_error("Unknown directory structure resolution: " + std::to_string(static_cast<int>(resolution)));
        }
    }

    std::string Storage::Impl::GetPath(bool isPermanent, const timestamp_t& timestamp, const std::string& id, Configuration::DirectoryStructureResolution resolution) const
    {
        // note that the id doubles as a filename
        return (boost::filesystem::path(GetDirectory(isPermanent, timestamp, resolution)) / id).string();
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

        const DataItem dataItem = FromFuture(GetData(dbSource, id, std::launch::deferred));
        if (!dataItem.isValid) {
            return false;
        }
        else {
            assert(dataItem.isPermanent != destinationIsPermanent);
            DeleteItem(sourceIsPermanent, dataItem.timestamp, dataItem.id);
            const DataItem newDataItem(dataItem.id, dataItem.data, dataItem.timestamp, destinationIsPermanent, dataItem.tags);
            if (SaveData(newDataItem, false)) {
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
        std::future<void> fileDeleteOperation = std::async(std::launch::async, [&]() {

            boost::filesystem::path sourcePath = GetPath(isPermanent, timestamp, id, configuration.directoryStructureResolution);
            boost::filesystem::remove(sourcePath);

            try {
                while (sourcePath.has_parent_path()) {
                    sourcePath = sourcePath.parent_path();
                    if (boost::filesystem::is_empty(sourcePath)) {
                        boost::filesystem::remove(sourcePath);
                    }
                    else {
                        break;
                    }
                }
            }
            catch (std::exception& e) {
                e;
            }
        });

        int deleted = GetDatabase(isPermanent)->exec("delete from DataItems where id = '" + id + "'");
        assert(deleted == 1);

        fileDeleteOperation.get(); // wait until the file and the empty subdirs (if any) have really been deleted
    }

    void Storage::Impl::FlushRotating()
    {
        Flush(GetDatabase(false));
    }

    void Storage::Impl::FlushPermanent()
    {
        Flush(GetDatabase(true));
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

    void Storage::Impl::CreateIndexesThatDoNotExist()
    {
        const std::string createIndexOnTimestamp = "create index if not exists timestamp_index on DataItems(timestamp)";

        dbRotating->exec(createIndexOnTimestamp);
        dbPermanent->exec(createIndexOnTimestamp);
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
        insertStatement << "insert or replace into DataItems values (@id, @timestamp, @path, @size";

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
            currentRotatingDataItemBytes = query.getColumn(0).getInt64();
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
            unsigned int deleteCounter = 0;

            while (hasExcessData() && query.executeStep()) {
                const std::string id = query.getColumn(0);
                const std::string timestampString = query.getColumn(1);
#if SIZE_MAX > 0xffffffff
                // 64-bit system
                const size_t size = query.getColumn(2).getInt64();
#else
                // 32-bit system
                const size_t size = query.getColumn(2);
#endif

                assert(currentRotatingDataItemBytes >= size);

                const auto timestamp = system_clock_time_point_string_conversion::from_string(timestampString);
                DeleteItem(false, timestamp, id);

                currentRotatingDataItemBytes -= size;
                hardDiskFreeBytes += size;

                if (rotatingDataDeletedCallback != nullptr) {
                    rotatingDataDeletedCallback(id);
                }

                if (++deleteCounter >= configuration.deletionFlushInterval) {
                    FlushRotating();
                    deleteCounter = 0;
                }
            }

            if (deleteCounter > 0) {
                FlushRotating();
            }
        }

        return !hasExcessData();
    }

    std::deque<std::string> Storage::Impl::GetIdsSortedByAscendingTimestamp(const std::string& timestampBegin, const std::string& timestampEnd) const
    {
        std::deque<std::string> ids;

        std::ostringstream select;
        select << "select id from DataItems";

        bool whereAlreadyAdded = false;
        const auto addWhereOrAnd = [&]() {
            if (!whereAlreadyAdded) {
                select << " where";
                whereAlreadyAdded = true;
            }
            else {
                select << " and";
            }
        };

        if (!timestampBegin.empty()) {
            addWhereOrAnd();
            select << " timestamp >= '" + timestampBegin + "'";
        }
        if (!timestampEnd.empty()) {
            addWhereOrAnd();
            select << " timestamp < '" + timestampEnd + "'";
        }

        select << " order by timestamp asc";

        SQLite::Statement query(*dbRotating, select.str());
        while (query.executeStep()) {
            const std::string id = query.getColumn(0);
            ids.push_back(id);
        }
        return ids;
    }

    void Storage::Impl::SetRotatingDataDeletedCallback(const rotating_data_deleted_callback_t& callback)
    {
        rotatingDataDeletedCallback = callback;
    }

}