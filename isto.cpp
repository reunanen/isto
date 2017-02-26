//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto.h"
#include "isto_impl.h"

#include <assert.h>

namespace isto {
    timestamp_t now() { return std::chrono::high_resolution_clock::now(); }

    DataItem::DataItem(const std::string& id, const char* dataBegin, const char* dataEnd, const timestamp_t& timestamp, bool isPermanent)
        : DataItem(id, std::vector<unsigned char>(dataBegin, dataEnd), timestamp)
    {}

    DataItem::DataItem(const std::string& id, const std::vector<unsigned char>& data, const timestamp_t& timestamp, bool isPermanent)
        : id(id)
        , data(data)
        , timestamp(timestamp)
        , isPermanent(isPermanent)
        , isValid(true)
    {}

    DataItem DataItem::Invalid()
    {
        return DataItem();
    }

    DataItem::DataItem()
        : isPermanent(false)
        , isValid(false)
    {}

    Storage::Storage(const Configuration& configuration)
        : impl(new Storage::Impl(configuration))
    {
    }

    Storage::~Storage()
    {
        assert(impl != nullptr);
        delete impl;
    }

    void Storage::SaveData(const DataItem& dataItem)
    {
        impl->SaveData(dataItem);
    }

    DataItem Storage::GetData(const std::string& id)
    {
        return impl->GetData(id);
    }

    DataItem Storage::GetData(const std::chrono::high_resolution_clock::time_point& timestamp, const std::string& comparisonOperator)
    {
        return impl->GetData(timestamp, comparisonOperator);
    }

    bool Storage::MakePermanent(const std::string& id)
    {
        return impl->MakePermanent(id);
    }

}