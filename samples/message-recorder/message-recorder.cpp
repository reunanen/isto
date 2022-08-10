//               Copyright 2022 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <isto.h>

#include "../system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"
#include "../zip/src/zip.h"

#include <messaging/claim/PostOffice.h>
#include <messaging/claim/AttributeMessage.h>
#include <messaging/claim/MessageStreaming.h>
#include <numcfc/Logger.h>

#include <sstream>
#include <assert.h>

std::vector<std::string> Tokenize(const std::string& input)
{
    std::vector<std::string> tokens;
    std::istringstream iss(input);
    while (iss) {
        std::string s;
        iss >> s;
        if (!s.empty()) {
            tokens.push_back(s);
        }
    }
    return tokens;
}

int main(int argc, char* argv[])
try {
	numcfc::Logger::LogAndEcho("message-recorder initializing...");

	numcfc::IniFile iniFile("message-recorder.ini");

    const auto subscribe = Tokenize(iniFile.GetSetValue("MessageTypes", "Subscribe", "*", "Space-separated list of message type patterns to subscribe"));
    const auto ignore = Tokenize(iniFile.GetSetValue("MessageTypes", "Ignore", "__claim_MsgStatus", "Space-separated list of message types to ignore"));

    if (subscribe.empty()) {
        throw std::runtime_error("Nothing to subscribe");
    }

    isto::Configuration configuration;
    {
        configuration.rotatingDirectory = iniFile.GetSetValue("Storage", "RotatingDirectory", configuration.rotatingDirectory);
        configuration.permanentDirectory = iniFile.GetSetValue("Storage", "PermanentDirectory", configuration.permanentDirectory);
        configuration.maxRotatingDataToKeepInGiB = iniFile.GetSetValue("Storage", "MaxRotatingDataToKeepInGiB", configuration.maxRotatingDataToKeepInGiB);
        configuration.minFreeDiskSpaceInGiB = iniFile.GetSetValue("Storage", "MinFreeDiskSpaceInGiB", configuration.minFreeDiskSpaceInGiB);
    }

    const auto compressionEnabled = iniFile.GetSetValue("Storage", "Compress", 1) > 0;

    isto::Storage storage(configuration);

    claim::PostOffice postOffice;
    postOffice.Initialize(iniFile, "mrec");

    if (iniFile.IsDirty()) {
        numcfc::Logger::LogAndEcho("Saving the ini file...");
        iniFile.Save();
    }

    for (const auto& messageType : subscribe) {
        postOffice.Subscribe(messageType);
    }

    numcfc::Logger::LogAndEcho("Listening...");

    slaim::Message msg;

    while (true) {
        uintmax_t messagesReceived = 0;
        uintmax_t bytesReceived = 0;
        double timeout_s = 1.0;
        std::ostringstream oss;
        while (postOffice.Receive(msg, timeout_s)) {
            timeout_s = 0.0;
            const auto i = std::find(
                ignore.begin(),
                ignore.end(),
                msg.GetType()
            );
            if (i == ignore.end()) {
                ++messagesReceived;
                bytesReceived += msg.GetSize();
                claim::WriteMessageToStream(oss, msg);
            }
        }
        if (messagesReceived > 0) {
            const isto::timestamp_t now = std::chrono::system_clock::now();
            const std::string timestamp = system_clock_time_point_string_conversion::to_string(now);
            std::string id = timestamp;
            std::replace(id.begin(), id.end(), ':', '_');

            const auto uncompressedData = oss.str();

            const isto::DataItem dataItem = [&] {
                if (!compressionEnabled) {
                    return isto::DataItem(id + ".msg", uncompressedData, now);
                }

                auto* zip = zip_stream_open(NULL, 0, ZIP_DEFAULT_COMPRESSION_LEVEL, 'w');
                if (!zip) {
                    throw std::runtime_error("Unable to open zip stream for writing");
                }
                try {
                    const auto zipEntryOpenResult = zip_entry_open(zip, (id + ".msg").c_str());
                    if (zipEntryOpenResult) {
                        throw std::runtime_error("Unable to open zip entry, return value = " + std::to_string(zipEntryOpenResult));
                    }
                    const auto zipEntryWriteResult = zip_entry_write(zip, uncompressedData.data(), uncompressedData.size());
                    if (zipEntryWriteResult) {
                        throw std::runtime_error("Unable to write zip entry, return value = " + std::to_string(zipEntryWriteResult));
                    }
                    const auto zipEntryCloseResult = zip_entry_close(zip);
                    if (zipEntryCloseResult) {
                        throw std::runtime_error("Unable to close zip entry, return value = " + std::to_string(zipEntryCloseResult));
                    }

                    // copy compressed stream into outbuf
                    char* buffer = NULL;
                    size_t bufferSize = 0;
                    const auto bytesCopied = zip_stream_copy(zip, reinterpret_cast<void**>(&buffer), &bufferSize);
                    assert(bytesCopied == bufferSize);
                    zip_stream_close(zip);
                    try {
                        std::string compressedData(buffer, bufferSize);
                        free(buffer);
                        return isto::DataItem(id + ".msg.zip", compressedData, now);
                    }
                    catch (std::exception&) {
                        free(buffer);
                        throw;
                    }
                }
                catch (std::exception&) {
                    zip_stream_close(zip);
                    throw;
                }
            }();

            storage.SaveData(dataItem, false);
            numcfc::Logger::LogAndEcho(
                "Stored: " + timestamp + ", "
                + std::to_string(messagesReceived) + " message" + (messagesReceived > 1 ? "s" : "") + ", "
                + std::to_string(bytesReceived) + " bytes",
                "log_received_messages"
            );
        }
    }
}
catch (std::exception& e) {
    numcfc::Logger::LogAndEcho("Fatal error: " + std::string(e.what()), "log_fatal_error");
}