#ifndef LICENSE_SERIALIZATION_H
#define LICENSE_SERIALIZATION_H

#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <algorithm>
#include <cctype>
#include <locale>

// Serialize license data to a string
// The license data (map) is serialized to a string as follows: "licenseVersion=1.0|softwareVersion=2.5|licenseId=123456|endDate=2024-12-31"
// Each key-value pair is separated by a pipe (|) and each key is linked to its value with an equals sign (=).
std::string serializeLicense(const std::map<std::string, std::string>& licenseData) {
    std::ostringstream out;
    for (const auto& pair : licenseData) {
        if (out.tellp() > 0) {  // Check if not the first element to avoid leading delimiter
            out << "|";
        }
        out << pair.first << "=" << pair.second;
    }
    return out.str();
}

// Deserialize license string to a map
std::map<std::string, std::string> deserializeLicense(const std::string& licenseString) {
    std::map<std::string, std::string> licenseData;
    std::istringstream in(licenseString);
    std::string entry;
    while (std::getline(in, entry, '|')) {
        auto pos = entry.find('=');
        if (pos != std::string::npos) {
            std::string key = entry.substr(0, pos);
            std::string value = entry.substr(pos + 1);
            licenseData[key] = value;
        }
    }
    return licenseData;
}


// Trim from start (left)
std::string lstrip(const std::string &s) {
    std::string str = s;
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    return str;
}

// Trim from end (right)
std::string rstrip(const std::string &s) {
    std::string str = s;
    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), str.end());
    return str;
}

// Trim from both ends
std::string strip(const std::string &s) {
    return lstrip(rstrip(s));
}

// Function to print contents of a std::map<std::string, std::string>
void printLicenseInfo(const std::map<std::string, std::string>& licenseInfo) {
    for (const auto& pair : licenseInfo) {
        printf("%s: %s\n", pair.first.c_str(), pair.second.c_str());
    }
    printf("\n");
}

#endif // LICENSE_SERIALIZATION_H
