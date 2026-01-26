#ifndef LICENSE_VALIDATION_H
#define LICENSE_VALIDATION_H

#include "crypto_utils.h" // For cryptographic functions
#include "license_serialization.h" // For serializing and deserializing the license data

#include <stdio.h>
#include <time.h>


#include <iostream>
#include <map>
#include <string>

// Function to load and validate the license information
std::map<std::string, std::string> validateAndLoadLicense(const std::string& keyFile, const std::string& ivFile,
                const std::string& publicKeyPath, const std::string& licenseFile,
                const std::string& signatureFile) {

        std::string key = readFile(keyFile);
        std::string iv = readFile(ivFile);

        RSA* publicRSA = loadRSAKey(publicKeyPath, false);
        if (publicRSA == nullptr) {
                fprintf(stderr, "Failed to load public key.\n");
                throw std::runtime_error("Public key loading failed.");
        }

        std::string encryptedLicense = strip(readFile(licenseFile));
        std::string signature = strip(readFile(signatureFile));

        std::string encryptedLicenseBinary = hex2bin(encryptedLicense);
        std::string signatureBinary = hex2bin(signature);

        bool isValid = verifySignature(encryptedLicenseBinary, signatureBinary, publicRSA);
        if (!isValid) {
                fprintf(stderr, "Signature validation failed.\n");
                throw std::runtime_error("Signature validation failed.");
        }

        std::string decryptedLicenseString = decrypt(encryptedLicenseBinary, key, iv);

        // RSA_free(publicRSA); // Ensure to free the RSA structure, commenting out because this causes Seg Fault for some reason

        return deserializeLicense(decryptedLicenseString);
}


// Convert a date string "YYYY-MM-DD HH:MM:SS" to time_t
time_t string_to_time_t(const std::string& date_string) {
        std::tm tm = {};
        std::istringstream ss(date_string);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S"); // Parse date and time
        return mktime(&tm);  // Convert tm structure to time_t
}


int check_license_expired_target_date(time_t expire_time) {
    time_t now;
    time(&now);

    struct tm now_tm = *gmtime(&now);
    struct tm expire_tm = *gmtime(&expire_time);

    // Print current date and time in the format: YYYY-MM-DD HH:MM:SS
    printf("Current Date and Time: %04d-%02d-%02d %02d:%02d:%02d\n",
        now_tm.tm_year + 1900, now_tm.tm_mon + 1, now_tm.tm_mday,
        now_tm.tm_hour, now_tm.tm_min, now_tm.tm_sec);

    // Print expiration date and time in the format: YYYY-MM-DD HH:MM:SS
    printf("Expiration Date and Time: %04d-%02d-%02d %02d:%02d:%02d\n",
        expire_tm.tm_year + 1900, expire_tm.tm_mon + 1, expire_tm.tm_mday,
        expire_tm.tm_hour, expire_tm.tm_min, expire_tm.tm_sec);

    if (difftime(expire_time, now) < 0) {
        return 1; // License has expired
    }
    return 0; // License is valid
}

#endif // LICENSE_VALIDATION_H
