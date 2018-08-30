//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto.h"
#include <SQLiteCpp/Database.h>
#include <SQLiteCpp/Statement.h>
#include <memory>
#include <future>

namespace isto {
    
    class Storage::Impl {
    public:
        Impl(const Configuration& configuration);

        bool SaveData(const DataItem& dataItem, bool upsert);
        bool SaveData(const DataItems& dataItems, bool upsert);

        DataItem GetData(const std::string& id);
        DataItem GetPermanentData(const std::string& id);
        DataItem GetRotatingData(const std::string& id);
        
        DataItem GetData(const timestamp_t& timestamp, const std::string& comparisonOperator, const tags_t& tags);
        DataItems GetDataItems(const timestamp_t& startTime, const timestamp_t& endTime, const tags_t& tags, size_t maxItems, Order order);

        bool MakePermanent(const std::string& id);
        bool MakeRotating(const std::string& id);

        std::deque<std::string> GetIdsSortedByAscendingTimestamp(const std::string& timestampBegin, const std::string& timestampEnd) const;

        void SetRotatingDataDeletedCallback(const rotating_data_deleted_callback_t& callback);

    private:
        bool SaveData(const DataItem* dataItems, size_t dataItemCount, bool upsert);
        void InsertDataItem(const DataItem& dataItem);

        std::unique_ptr<SQLite::Database>& GetDatabase(bool isPermanent);
        std::future<std::unique_ptr<DataItem>> GetData(std::unique_ptr<SQLite::Database>& db, const std::string& id, std::launch preferredLaunchMode);
        DataItems GetDataItems(std::unique_ptr<SQLite::Database>& db, const timestamp_t& startTime, const timestamp_t& endTime, const tags_t& tags, size_t maxItems, Order order);

        std::string GetSubDir(bool isPermanent) const;
        std::string GetDirectory(bool isPermanent, const timestamp_t& timestamp) const;
        std::string GetPath(bool isPermanent, const timestamp_t& timestamp, const std::string& id) const;

        void CreateDirectoriesThatDoNotExist();
        void CreateDatabases();
        void CreateTablesThatDoNotExist();
        void CreateIndexesThatDoNotExist();
        void CreateStatements();
        void InitializeCurrentDataItemBytes();

        // returns true if ok to save
        bool DeleteExcessRotatingData(size_t sizeToBeInserted);

        bool MoveDataItem(bool sourceIsPermanent, bool destinationIsPermanent, const std::string& id);
        void DeleteItem(bool isPermanent, const timestamp_t& timestamp, const std::string& id);

        void FlushRotating();
        void FlushPermanent();
        void Flush(std::unique_ptr<SQLite::Database>& db);

        std::pair<std::string, std::unique_ptr<SQLite::Database>&> FindMatchingTimestampAndCorrespondingDatabase(
            const std::chrono::high_resolution_clock::time_point& timestamp,
            const std::string& comparisonOperator,
            const tags_t& tags);

        Configuration configuration;
        std::unique_ptr<SQLite::Database> dbRotating;
        std::unique_ptr<SQLite::Database> dbPermanent;
        std::unique_ptr<SQLite::Database> dbNone;

        std::unique_ptr<SQLite::Statement> insertRotating;
        std::unique_ptr<SQLite::Statement> insertPermanent;

        uintmax_t currentRotatingDataItemBytes = -1;

        rotating_data_deleted_callback_t rotatingDataDeletedCallback;
    };

};
