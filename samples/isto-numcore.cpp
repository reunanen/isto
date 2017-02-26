//               Copyright 2017 Juha Reunanen
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <messaging/claim/PostOffice.h>
#include <messaging/claim/AttributeMessage.h>
#include <numcfc/Logger.h>
#include <numcfc/Time.h>

#include <unordered_map>

int main(int argc, char* argv[])
{
	numcfc::Logger::LogAndEcho("isto-numcore starting - initializing...");

	numcfc::IniFile iniFile("isto-numcore.ini");

    claim::PostOffice postOffice;
    postOffice.Initialize(iniFile, "isto");

    postOffice.Subscribe("ImageData");

    if (iniFile.IsDirty()) {
        numcfc::Logger::LogAndEcho("Saving the ini file...");
        iniFile.Save();
    }


    while (true) {
        slaim::Message msg;

        while (postOffice.Receive(msg)) {
            if (msg.GetType() == "") {
            }
        }
    }
}