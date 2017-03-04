//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto.h"
#include <SQLiteCpp/Database.h>
#include <SQLiteCpp/Statement.h>
#include <memory>

namespace isto {
    
    class Storage::Impl {
    public:
        Impl(const Configuration& configuration);

        bool SaveData(const DataItem& dataItem);
        DataItem GetData(const std::string& id);
        DataItem GetPermanentData(const std::string& id);
        DataItem GetRotatingData(const std::string& id);
        
        DataItem GetData(const timestamp_t& timestamp = std::chrono::high_resolution_clock::now(), const std::string& comparisonOperator = "<=",
            const std::unordered_map<std::string, std::string>& tags = std::unordered_map<std::string, std::string>());

        bool MakePermanent(const std::string& id);
        bool MakeRotating(const std::string& id);

    private:
        std::unique_ptr<SQLite::Database>& GetDatabase(bool isPermanent);
        DataItem GetData(std::unique_ptr<SQLite::Database>& db, const std::string& id);

        std::string GetSubDir(bool isPermanent) const;
        std::string GetDirectory(bool isPermanent, const timestamp_t& timestamp) const;
        std::string GetPath(bool isPermanent, const timestamp_t& timestamp, const std::string& id) const;

        void CreateTablesThatDoNotExist();
        void CreateDirectoriesThatDoNotExist();
        void CreateDatabases();
        void CreateStatements();
        void InitializeCurrentDataItemBytes();

        // returns true if ok to save
        bool DeleteExcessRotatingData(size_t sizeToBeInserted);

        bool MoveDataItem(bool sourceIsPermanent, bool destinationIsPermanent, const std::string& id);
        void DeleteItem(bool isPermanent, const timestamp_t& timestamp, const std::string& id);

        void Flush(std::unique_ptr<SQLite::Database>& db);

        std::pair<std::string, std::unique_ptr<SQLite::Database>&> FindMatchingTimestampAndCorrespondingDatabase(const std::chrono::high_resolution_clock::time_point& timestamp, const std::string& comparisonOperator);

        Configuration configuration;
        std::unique_ptr<SQLite::Database> dbRotating;
        std::unique_ptr<SQLite::Database> dbPermanent;
        std::unique_ptr<SQLite::Database> dbNone;

        std::unique_ptr<SQLite::Statement> insertRotating;
        std::unique_ptr<SQLite::Statement> insertPermanent;

        size_t currentRotatingDataItemBytes = -1;
    };

};
