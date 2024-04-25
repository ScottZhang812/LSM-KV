#pragma once

#include <fstream>
#include <limits>
#include <vector>

#include "kvstore_api.h"
#include "skiplist.h"
#include "utils.h"
using namespace skiplist;

class KVStore : public KVStoreAPI {
    // You can add your implementation here
   private:
    typedef uint64_t KEY_TYPE;
    typedef std::string VALUE_TYPE;
    typedef uint32_t VLEN_TYPE;

    std::string dir;
    std::string vlog;
    const int memTableLenThreshold = (16 * 1024 - 32 - 8192) / (8 + 8 + 4);
    skiplist_type *memTable;

   public:
    KVStore(const std::string &dir, const std::string &vlog);

    ~KVStore();

    void clearMemTable() {
        if (memTable) delete memTable;
        memTable = new skiplist_type();
    }
    void convertMemTable2File();
    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2,
              std::list<std::pair<uint64_t, std::string>> &list) override;

    void gc(uint64_t chunk_size) override;

    // utils
    void fillCrcObj(std::vector<unsigned char> &crcObj, KEY_TYPE key,
                    VLEN_TYPE vlen, const VALUE_TYPE &value)
};
