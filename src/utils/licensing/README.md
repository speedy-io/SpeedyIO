# Project Files Overview

**IMPORTANT: Do not commit any files other than code files in this folder. The files containing strings which 
store keys, initialziation vector, public keys, private keys etc should NOT be committed.**

These files have been tested on gcc 11. They run for openssl versions OpenSSL 1.0.2k-fips on RHEL7 and OpenSSL 1.1.1f on Ubuntu 20.04

This folder contains the files necessary for the encryption, decryption, signing, and verification of 
software licenses for speedyio. Here is a brief overview of each file included in the project:

### `generate_license.cpp`
- **Purpose**: Utility program for generating encrypted and signed license files.
- **Functionality**: Generates an encrypted and signed license file using the provided license data.

### `LicenseValidation.h`
- **Purpose**: Contains functions for validating the expiration of software licenses using cryptographic methods.
- **Functionality**: Provides a function to validate and load license information from encrypted and signed formats.

### `crypto_utils.h`
- **Purpose**: Utility functions for cryptographic operations.
- **Functionality**: Includes functions for encryption, decryption, and conversion between binary and hexadecimal string formats.

### `license_check.cpp`
- **Purpose**: Contains the main application logic for checking the validity of a license.
- **Functionality**: Uses cryptographic utilities to check if the current date is beyond the license's expiration.

### `license_serialization.h`
- **Purpose**: Handles the serialization and deserialization of license data.
- **Functionality**: Provides functions to serialize license information into a string and deserialize it back into a structured format.


**The following files are needed for the code to work, but SHOULD NOT BE COMMITTED TO GITHUB:**

### `iv.txt`
- **Purpose**: Stores the initialization vector (IV) for the AES encryption algorithm. 
               This will need to be shipped with the software (used for encryption and decryption).
- **Content**: Contains a hexadecimal string used as an IV in encryption processes. This can be any random string (eg. abcdef9876543210abcdef9876543210)

### `key.txt`
- **Purpose**: Stores the encryption key.
               This will need to be shipped with the software (used for encryption and decryption).
- **Content**: Contains a hexadecimal string used as the encryption key for AES. This can be any random string (eg. 0123456789abcdef0123456789abcdef)

### `license.lic`
- **Purpose**: Encrypted license file.
               This will need to be shipped with the software (stores license info map).
- **Content**: Contains the encrypted form of the license data.

### `signature.txt`
- **Purpose**: Contains the digital signature of the encrypted license.
               This will need to be shipped with the software (used for verifying signature of the license we provided).
- **Content**: Stores the signature generated from the encrypted license data for verification.

### `public.pem`
- **Purpose**: Public key file.
               This will need to be shipped with the software (used for decryption).
- **Content**: Contains the RSA public key used for verifying the license signature.

### `private.pem`
- **Purpose**: Private key file.
- **Content**: Contains the RSA private key used for signing the license data.


---

## Setup and Usage
Ensure you have the necessary libraries and tools installed to compile and run the C++ code, such as a C++ compiler supporting C++11 or later and OpenSSL for cryptographic functions.

For generating license:
1. Make any changes to the license JSON in `generate_license.cpp` as per your requirements. 
   This JSON's info will get serialzied into a string , and then encrypted to generate the license.
2. Compile and run: `g++ -o generate_license generate_license.cpp -lcrypto -lssl && ./generate_license`
3. The license and signature strings will be printed.
4. Paste the license string into `license.lic`.
5. Paste the signature string into `signature.txt`.
6. Run this to check the license: `g++ -o license_check license_check.cpp -lcrypto -lssl && ./license_check`
   1. This will check if the license signing is correct, and print the decrypted license info map.
   2. This will run a program which can do various operations after checking license, for e.g. terminate after license `endDate` is breached.

Ensure all files such as `key.txt`, `iv.txt`, `public.pem`, and `private.pem` are correctly placed in the same directory as the executable or updated to match their path in your environment.

--- 

## Generating a `public.pem + private.pem` pair

If you do not already have `public.pem` and `private.pem` files, you can generate a new pair by following these steps:

- Generate `private.pem` by running this: `openssl genrsa -out private.pem 2048`. This is your private key.
- Generate `public.pem` by running this: `openssl rsa -in private.pem -pubout -out public.pem`. This is your public key.
  - Verify by running `openssl rsa -in public.pem -pubin -text -noout`. It should print the modulus and exponent details of the public key.

--- 

## Installing gcc11 on RHEL7

- Check gcc version by running `gcc --version`.
- `sudo yum-config-manager --enable rhel-server-rhscl-7-rpms` (this command might be different for RHEL8)
- `sudo yum install devtoolset-11`
- Run `scl enable devtoolset-11 bash` to spawn a new bash shell in which the gcc version will be 11. This command might be different for RHEL8.
  - Note that if you open a new terminal or shell, the gcc will be the original older version. You have to run this command to spawn a new bash shell
    with gcc set to the new version.
