#ifndef CRYPTO_UTILS_H
#define CRYPTO_UTILS_H

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <iomanip>
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/opensslv.h>

#include <openssl/evp.h>
#include <openssl/objects.h>

// Callback function for OBJ_NAME_do_all_sorted
static void print_names(const OBJ_NAME *name, void *arg) {
    // You can cast and use arg if you need to pass information to the callback
    std::cout << "Digest name: " << name->name << std::endl;
}

// Function to list all available digests
void list_all_available_digests() {
    // printf("========================================================\n");
    // printf("Listing all available digests: \n");
    OpenSSL_add_all_digests();  // Ensure all digest algorithms are loaded
    // Call OBJ_NAME_do_all_sorted with the callback
    OBJ_NAME_do_all_sorted(OBJ_NAME_TYPE_MD_METH, print_names, nullptr);
    // printf("========================================================\n");
}

// Error handling function
void handleErrors() {
    ERR_print_errors_fp(stderr);
    abort();
}

// Wrapper for creating a new EVP_CIPHER_CTX
EVP_CIPHER_CTX* evp_cipher_ctx_new() {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    return EVP_CIPHER_CTX_new();
#else
    EVP_CIPHER_CTX* ctx = (EVP_CIPHER_CTX*)malloc(sizeof(EVP_CIPHER_CTX));
    if (ctx != NULL) {
        EVP_CIPHER_CTX_init(ctx);
    }
    return ctx;
#endif
}

// Wrapper for freeing EVP_CIPHER_CTX
void evp_cipher_ctx_free(EVP_CIPHER_CTX* ctx) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_CIPHER_CTX_free(ctx);
#else
    EVP_CIPHER_CTX_cleanup(ctx);
    free(ctx);
#endif
}

// Wrapper functions for encryption and decryption
int evp_encrypt_init(EVP_CIPHER_CTX* ctx, const EVP_CIPHER* type, const unsigned char* key, const unsigned char* iv) {
    return EVP_EncryptInit_ex(ctx, type, NULL, key, iv);
}

int evp_encrypt_update(EVP_CIPHER_CTX* ctx, unsigned char* out, int* outlen, const unsigned char* in, int inlen) {
    return EVP_EncryptUpdate(ctx, out, outlen, in, inlen);
}

int evp_encrypt_final(EVP_CIPHER_CTX* ctx, unsigned char* out, int* outlen) {
    return EVP_EncryptFinal_ex(ctx, out, outlen);
}

int evp_decrypt_init(EVP_CIPHER_CTX* ctx, const EVP_CIPHER* type, const unsigned char* key, const unsigned char* iv) {
    return EVP_DecryptInit_ex(ctx, type, NULL, key, iv);
}

int evp_decrypt_update(EVP_CIPHER_CTX* ctx, unsigned char* out, int* outlen, const unsigned char* in, int inlen) {
    return EVP_DecryptUpdate(ctx, out, outlen, in, inlen);
}

int evp_decrypt_final(EVP_CIPHER_CTX* ctx, unsigned char* out, int* outlen) {
    return EVP_DecryptFinal_ex(ctx, out, outlen);
}

// Read file content into a string
std::string readFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        fprintf(stderr, "Error opening file: %s\n", filename.c_str());
        exit(EXIT_FAILURE);
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

std::string hex2bin(const std::string& hex) {
    std::string bin;
    bin.reserve(hex.length() / 2);
    for (size_t i = 0; i < hex.length(); i += 2) {
        unsigned byte = std::stoul(hex.substr(i, 2), nullptr, 16);
        bin.push_back(static_cast<char>(byte));
    }
    return bin;
}

std::string bin2hex(const std::string& bin) {
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (unsigned char c : bin) {
        ss << std::setw(2) << static_cast<int>(c);
    }
    return ss.str();
}


// Encrypt plaintext using AES-256-CBC
std::string encrypt(const std::string& plaintext, const std::string& key, const std::string& iv) {
    EVP_CIPHER_CTX* ctx = evp_cipher_ctx_new();
    if (!ctx) handleErrors();

    std::vector<unsigned char> ciphertext(plaintext.length() + EVP_MAX_BLOCK_LENGTH);
    int len = 0, ciphertext_len = 0;

    if (1 != evp_encrypt_init(ctx, EVP_aes_256_cbc(), (unsigned char*)key.data(), (unsigned char*)iv.data()))
        handleErrors();

    if (1 != evp_encrypt_update(ctx, ciphertext.data(), &len, (unsigned char*)plaintext.data(), plaintext.length()))
        handleErrors();
    ciphertext_len += len;

    if (1 != evp_encrypt_final(ctx, ciphertext.data() + len, &len))
        handleErrors();
    ciphertext_len += len;

    evp_cipher_ctx_free(ctx);

    return std::string((char*)ciphertext.data(), ciphertext_len);
}

// Decrypt ciphertext using AES-256-CBC
std::string decrypt(const std::string& ciphertext, const std::string& key, const std::string& iv) {
    EVP_CIPHER_CTX* ctx = evp_cipher_ctx_new();
    if (!ctx) handleErrors();

    std::vector<unsigned char> plaintext(ciphertext.length());
    int len = 0, plaintext_len = 0;

    if (1 != evp_decrypt_init(ctx, EVP_aes_256_cbc(), (unsigned char*)key.data(), (unsigned char*)iv.data()))
        handleErrors();

    if (1 != evp_decrypt_update(ctx, plaintext.data(), &len, (unsigned char*)ciphertext.data(), ciphertext.length()))
        handleErrors();
    plaintext_len += len;

    if (1 != evp_decrypt_final(ctx, plaintext.data() + len, &len))
        handleErrors();
    plaintext_len += len;

    evp_cipher_ctx_free(ctx);

    return std::string((char*)plaintext.data(), plaintext_len);
}


// New wrapper functions for RSA and Digest Management
EVP_MD_CTX* evp_md_ctx_new() {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    return EVP_MD_CTX_new();
#else
    EVP_MD_CTX* ctx = (EVP_MD_CTX*)malloc(sizeof(EVP_MD_CTX));
    EVP_MD_CTX_init(ctx);
    return ctx;
#endif
}

void evp_md_ctx_free(EVP_MD_CTX* ctx) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_MD_CTX_free(ctx);
#else
    EVP_MD_CTX_cleanup(ctx);
    free(ctx);
#endif
}


std::string signData(const std::string& data, RSA* rsa) {

    list_all_available_digests();

    EVP_PKEY* pkey = EVP_PKEY_new();
    if (!pkey || !EVP_PKEY_assign_RSA(pkey, rsa)) {
        if (pkey) EVP_PKEY_free(pkey);
        handleErrors();
    }
        
    EVP_MD_CTX* ctx = evp_md_ctx_new();
    if (!ctx) {
        // std::cout << "ctx = " << ctx << std::endl;
        EVP_PKEY_free(pkey);
        handleErrors();
    }

    const EVP_MD* md = EVP_get_digestbyname("SHA256");
    if (!md) {
        // std::cout << "md = " << md << std::endl;
        evp_md_ctx_free(ctx);
        EVP_PKEY_free(pkey);
        handleErrors();
    }

    EVP_SignInit(ctx, md);
    if (!EVP_SignUpdate(ctx, (const unsigned char*)data.c_str(), data.length())) {
        evp_md_ctx_free(ctx);
        EVP_PKEY_free(pkey);
        handleErrors();
    }

    unsigned char sig[4096];
    unsigned int sig_len;
    if (!EVP_SignFinal(ctx, sig, &sig_len, pkey)) {
        evp_md_ctx_free(ctx);
        EVP_PKEY_free(pkey);
        handleErrors();
    }

    evp_md_ctx_free(ctx);
    EVP_PKEY_free(pkey);

    return std::string((char*)sig, sig_len);
}


bool verifySignature(const std::string& data, const std::string& signature, RSA* rsa) {
    EVP_PKEY* pkey = EVP_PKEY_new();
    EVP_PKEY_assign_RSA(pkey, rsa);
    EVP_MD_CTX* mctx = evp_md_ctx_new();

    EVP_VerifyInit(mctx, EVP_sha256());
    EVP_VerifyUpdate(mctx, data.c_str(), data.size());

    int ret = EVP_VerifyFinal(mctx, (unsigned char*)signature.data(), signature.size(), pkey);

    evp_md_ctx_free(mctx);
    EVP_PKEY_free(pkey);

    return ret == 1; // return true if the verification is successful
}

// Load an RSA key from a file
RSA* loadRSAKey(const std::string& filename, bool isPrivateKey) {
    FILE* file = fopen(filename.c_str(), "rb");
    if (!file) {
        fprintf(stderr, "Error opening key file: %s\n", filename.c_str());
        return nullptr;
    }

    RSA* rsa = nullptr;
    if (isPrivateKey) {
        rsa = PEM_read_RSAPrivateKey(file, NULL, NULL, NULL);
    } else {
        rsa = PEM_read_RSA_PUBKEY(file, NULL, NULL, NULL);
    }
    fclose(file);

    if (rsa == NULL) {
        fprintf(stderr, "Error reading RSA key from file: %s\n", filename.c_str());
        ERR_print_errors_fp(stderr);
        abort();
    }

    return rsa;
}


#endif // CRYPTO_UTILS_H
