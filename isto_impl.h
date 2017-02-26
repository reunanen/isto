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

        void SaveData(const DataItem& dataItem);
        DataItem GetData(const std::string& id);
        DataItem GetPermanentData(const std::string& id);
        DataItem GetRotatingData(const std::string& id);
        DataItem GetData(const timestamp_t& timestamp = std::chrono::high_resolution_clock::now(), const std::string& comparisonOperator = "<=");
        bool MakePermanent(const std::string& id);

    private:
        std::unique_ptr<SQLite::Database>& GetDatabase(bool isPermanent);
        DataItem GetData(std::unique_ptr<SQLite::Database>& db, const std::string& id);

        std::string GetSubDir(bool isPermanent) const;
        void CreateTablesThatDoNotExist();
        void CreateDirectoriesThatDoNotExist();
        void CreateDatabases();
        void CreateStatements();
        void DeleteExcessRotatingData();

        Configuration configuration;
        std::unique_ptr<SQLite::Database> dbRotating;
        std::unique_ptr<SQLite::Database> dbPermanent;
        std::unique_ptr<SQLite::Statement> insertRotating;
        std::unique_ptr<SQLite::Statement> insertPermanent;
    };

};