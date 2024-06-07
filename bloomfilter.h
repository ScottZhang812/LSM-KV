#include <iostream>

#include "MurmurHash3.h"
#include "type.h"

class BF {
   public:
    int m;           // 哈希bit数组的大小
    int k;           // 哈希函数的个数
    bool* bitArray;  // 哈希bit数组
    BF(int _m = DEFAULT_M_VAL, int _k = DEFAULT_HASHFUN_NUM) : m(_m), k(_k) {
        bitArray = new bool[m];
        for (int i = 0; i < m; i++) {
            bitArray[i] = false;
        }
    }
    BF(const BF& other) : m(other.m), k(other.k) {
        bitArray = new bool[m];
        for (int i = 0; i < m; i++) {
            bitArray[i] = other.bitArray[i];
        }
    }
    ~BF() {
        if (bitArray != nullptr) delete[] bitArray, bitArray = nullptr;
    }
    BF& operator=(const BF& other) {
        if (this != &other) {
            if (bitArray != nullptr) delete[] bitArray, bitArray = nullptr;
            m = other.m;
            k = other.k;
            bitArray = new bool[m];
            for (int i = 0; i < m; i++) {
                bitArray[i] = other.bitArray[i];
            }
        }
        return *this;
    }
    void getHashValue(const uint64_t& key, uint32_t* hashRes) {
        MurmurHash3_x64_128(&key, sizeof(key), 1, hashRes);
    }
    void insert(const uint64_t& key) {
        for (int i = 0; i < k; i++) {
            uint32_t hashRes[4];
            getHashValue(key, hashRes);
            bitArray[hashRes[0] % m] = true;
            bitArray[hashRes[1] % m] = true;
        }
    }
    bool query(const uint64_t& key) {
        for (int i = 0; i < k; i++) {
            uint32_t hashRes[4];
            getHashValue(key, hashRes);
            if (!bitArray[hashRes[0] % m] || !bitArray[hashRes[1] % m]) {
                return false;
            }
        }
        return true;
    }
};