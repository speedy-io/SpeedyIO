#include "crypto_utils.h"
#include "license_serialization.h"

int main() {
    // Load key and IV from files
    std::string keyFile = "key.txt";
    std::string ivFile = "iv.txt";

    std::string key = readFile(keyFile);
    std::string iv = readFile(ivFile);

    std::map<std::string, std::string> licenseInfo = {
        {"licenseVersion", "1.0"},
        {"softwareVersion", "1.0"},
        {"licenseId", "goldman"},

        // endDate should be YYYY-MM-DD HH:MM:SS
        {"endDate", "2024-07-26 12:00:00"}
    };
    std::cout << std::endl << "Raw License Information:" << std::endl;
    printLicenseInfo(licenseInfo);

    std::string licenseString = serializeLicense(licenseInfo);
    std::cout << "LicenseString = " << licenseString << std::endl;

    std::string privateKeyPath = "private.pem";
    RSA* privateRSA = loadRSAKey(privateKeyPath, true);

    if (privateRSA == nullptr) {
        std::cerr << "Failed to load private key." << std::endl;
        return 1; // Exit if private key is not loaded properly
    }


    std::string encryptedLicense = encrypt(licenseString, key, iv);
    std::string signature = signData(encryptedLicense, privateRSA);

    std::string encryptedLicenseHex = bin2hex(encryptedLicense);
    std::string signatureHex = bin2hex(signature);

    std::cout << "Encrypted License: " << encryptedLicenseHex << std::endl;
    std::cout << std::endl;
    std::cout << "Signature: " << signatureHex << std::endl << std::endl;

    // RSA_free(privateRSA);  // commenting out because this causes Seg Fault for some reason

    return 0;
}
