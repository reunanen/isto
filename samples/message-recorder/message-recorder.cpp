//               Copyright 2022 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <isto.h>
#include "../system_clock_time_point_string_conversion/system_clock_time_point_string_conversion.h"

#include <messaging/claim/PostOffice.h>
#include <messaging/claim/AttributeMessage.h>
#include <messaging/claim/MessageStreaming.h>
#include <numcfc/Logger.h>

#include <sstream>

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
            isto::DataItem dataItem(id + ".msg", oss.str(), now);
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