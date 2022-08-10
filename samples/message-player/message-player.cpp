//               Copyright 2022 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "../system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"
#include "../zip/src/zip.h"

#include <messaging/claim/PostOffice.h>
#include <messaging/claim/AttributeMessage.h>
#include <messaging/claim/MessageStreaming.h>
#include <numcfc/Logger.h>

#include <sstream>
#include <deque>
#include <filesystem>
#include <thread>
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
	numcfc::Logger::LogAndEcho("message-player initializing...");

	numcfc::IniFile iniFile("message-player.ini");

    const auto directory = iniFile.GetSetValue("Storage", "Directory", "data");

    const double speedFactor = iniFile.GetSetValue("Playback", "SpeedFactor", 1.0);
    const bool loop = iniFile.GetSetValue("Playback", "Loop", 0) > 0;

    const auto ignore = Tokenize(iniFile.GetSetValue("MessageTypes", "Ignore", "__claim_MsgStatus", "Space-separated list of message types to ignore"));

    claim::PostOffice postOffice;
    postOffice.Initialize(iniFile, "mplr");

    if (iniFile.IsDirty()) {
        numcfc::Logger::LogAndEcho("Saving the ini file...");
        iniFile.Save();
    }

    namespace fs = std::experimental::filesystem;

    uintmax_t messagesSent = 0;

    std::unique_ptr<std::chrono::steady_clock::time_point> prevMessageSentTime;
    std::chrono::system_clock::time_point prevMessageOriginalTime;

    std::string uncompressedDataBuffer;

    // recursive_directory_iterator would be nice, but the order isn't guaranteed

    const std::function<void(const fs::path&)> iterate = [&](const fs::path& path) {
        std::deque<fs::path> entries;
        for (const auto& entry : fs::directory_iterator(path)) {
            if (fs::is_directory(entry) || entry.path().extension() == ".msg") {
                entries.push_back(entry);
            }
            else if (entry.path().extension() == ".zip" && entry.path().stem().extension() == ".msg") {
                entries.push_back(entry);
            }
        }
        std::sort(entries.begin(), entries.end());
        for (const auto& entry : entries) {
            if (fs::is_directory(entry)) {
                numcfc::Logger::LogAndEcho("Entering directory: " + entry.string());
                iterate(entry);
            }
            else {
                numcfc::Logger::LogAndEcho("Reading file: " + entry.string());
                assert(entry.extension() == ".msg" || entry.extension() == ".zip");
                const bool isCompressed = entry.extension() == ".zip";

                if (speedFactor > 0) {
                    const auto id = entry.stem().string();
                    std::string timestamp = isCompressed
                        ? entry.stem().stem().string()
                        : entry.stem().string();
                    std::replace(timestamp.begin(), timestamp.end(), '_', ':');
                    const auto originalMessageTime = system_clock_time_point_string_conversion::from_string(timestamp);
                    if (prevMessageSentTime) {
                        const auto originalInterval = originalMessageTime - prevMessageOriginalTime;
                        const auto durationToWait = originalInterval / speedFactor;
                        *prevMessageSentTime += std::chrono::duration_cast<std::chrono::nanoseconds>(durationToWait);
                        std::this_thread::sleep_until(*prevMessageSentTime);
                    }
                    else {
                        prevMessageSentTime = std::make_unique<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
                    }
                    prevMessageOriginalTime = originalMessageTime;
                }

                std::unique_ptr<std::ifstream> uncompressedFileStream;
                std::unique_ptr<std::istringstream> uncompressedMemoryStream;

                if (!isCompressed) {
                    uncompressedFileStream = std::make_unique<std::ifstream>(entry, std::ios::binary);
                }
                else {
                    struct zip_t *zip = zip_open(entry.string().c_str(), 0, 'r');
                    try {
                        const auto zipEntryName = fs::path(entry).stem().string();
                        const auto zipEntryOpenResult = zip_entry_open(zip, zipEntryName.c_str());
                        if (zipEntryOpenResult) {
                            throw std::runtime_error("Unable to open zip entry " + zipEntryName + ", return value = " + std::to_string(zipEntryOpenResult));
                        }
                        char* buffer = NULL;
                        size_t bufferSize = 0;
                        const auto bytesRead = zip_entry_read(zip, reinterpret_cast<void**>(&buffer), &bufferSize);
                        assert(bytesRead == bufferSize);
                        try {
                            uncompressedDataBuffer = std::string(buffer, bytesRead);
                            free(buffer);
                            uncompressedMemoryStream = std::make_unique<std::istringstream>(uncompressedDataBuffer);
                        }
                        catch (std::exception&) {
                            free(buffer);
                            throw;
                        }
                    }
                    catch (std::exception&) {
                        zip_close(zip);
                        throw;
                    }
                }

                slaim::Message msg;

                const auto readMessageFromStream = [&]() {
                    return isCompressed
                        ? claim::ReadMessageFromStream(*uncompressedMemoryStream, msg)
                        : claim::ReadMessageFromStream(*uncompressedFileStream, msg);
                };

                uintmax_t messagesRead = 0;
                while (readMessageFromStream()) {
                    ++messagesRead;
                    const auto i = std::find(
                        ignore.begin(),
                        ignore.end(),
                        msg.GetType()
                    );
                    if (i == ignore.end()) {
                        postOffice.Send(msg);
                        ++messagesSent;
                    }
                }
                if (messagesRead == 0) {
                    numcfc::Logger::LogAndEcho("Warning: no messages read from file: " + entry.string(), "log_warnings");
                }
            }
        }
    };

    do {
        numcfc::Logger::LogAndEcho("Playing from: " + directory);
        iterate(directory);
    } while (loop);

    numcfc::Logger::LogAndEcho("Done - sent a grand total of " + std::to_string(messagesSent) + " messages");
}
catch (std::exception& e) {
    numcfc::Logger::LogAndEcho("Fatal error: " + std::string(e.what()), "log_fatal_error");
}