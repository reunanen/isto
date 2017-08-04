//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "isto.h"
#include "isto_impl.h"

#include "system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"

#include <assert.h>

namespace isto {
    timestamp_t now() { return std::chrono::system_clock::now(); }

    timestamp_t RoundToUsedPrecision(const timestamp_t timestamp) {
        const timestamp_t rounded = system_clock_time_point_string_conversion::from_string(system_clock_time_point_string_conversion::to_string(timestamp));
        assert(abs(std::chrono::duration_cast<std::chrono::microseconds>(timestamp - rounded).count()) < 1);
        return system_clock_time_point_string_conversion::from_string(system_clock_time_point_string_conversion::to_string(timestamp));
    }

    DataItem::DataItem(const std::string& id, const char* dataBegin, const char* dataEnd, const timestamp_t& timestamp, bool isPermanent, const tags_t& tags)
        : DataItem(id, std::vector<unsigned char>(dataBegin, dataEnd), timestamp, isPermanent, tags)
    {}

    DataItem::DataItem(const std::string& id, const std::vector<unsigned char>& data, const timestamp_t& timestamp, bool isPermanent, const tags_t& tags)
        : id(id)
        , data(data)
        , timestamp(RoundToUsedPrecision(timestamp))
        , isPermanent(isPermanent)
        , isValid(true)
        , tags(tags)
    {}

    DataItem::DataItem(const std::string& id, const std::string& data, const timestamp_t& timestamp, bool isPermanent, const tags_t& tags)
        : DataItem(id, std::vector<unsigned char>(data.begin(), data.end()), timestamp, isPermanent, tags)
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

    void Storage::SaveData(const DataItem& dataItem, bool upsert)
    {
        impl->SaveData(dataItem, upsert);
    }

    DataItem Storage::GetData(const std::string& id)
    {
        return impl->GetData(id);
    }

    DataItem Storage::GetData(const std::chrono::system_clock::time_point& timestamp, const std::string& comparisonOperator, const tags_t& tags)
    {
        return impl->GetData(timestamp, comparisonOperator, tags);
    }

    bool Storage::MakePermanent(const std::string& id)
    {
        return impl->MakePermanent(id);
    }

    bool Storage::MakeRotating(const std::string& id)
    {
        return impl->MakeRotating(id);
    }

    std::deque<std::string> Storage::GetIdsSortedByAscendingTimestamp(const std::string& timestampBegin, const std::string& timestampEnd) const
    {
        return impl->GetIdsSortedByAscendingTimestamp(timestampBegin, timestampEnd);
    }

    void Storage::SetRotatingDataDeletedCallback(const rotating_data_deleted_callback_t& callback)
    {
        return impl->SetRotatingDataDeletedCallback(callback);
    }
}