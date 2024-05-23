#pragma once

#include <fstream>
#include <limits>
#include <vector>

#include "bloom.h"
#include "kvstore_api.h"
#include "skiplist.h"
#include "utils.h"
using namespace skiplist;

typedef uint64_t KEY_TL;  // TL means 'type for LSM'
typedef std::string VALUE_TL;
typedef uint64_t SS_OFFSET_TL;
typedef uint32_t SS_VLEN_TL;
typedef uint8_t VLOG_MAGIC_TL;
typedef uint16_t VLOG_CHECKSUM_TL;
typedef uint32_t FILE_NUM_TL;
typedef uint64_t SST_HEADER_KVNUM_TL;

#define SS_HEADER_BYTENUM 32
#define SS_BLOOM_BYTENUM 8192  // There are 3 consts related in total
#define DEFAULT_M_VAL 65536
#define SS_MAX_FILE_BYTENUM 16384  // 16 * 1024
#define SS_FILE_SUFFIX ".sst"
#define VLOG_DEFAULT_MAGIC_VAL 0xff
#define SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM "/level-"

class KVStore : public KVStoreAPI {
    // You can add your implementation here
   private:
    typedef uint64_t KEY_TYPE;
    typedef std::string VALUE_TYPE;
    typedef uint32_t VLEN_TYPE;

    std::string dir;
    // for vlog
    std::string vlog;
    SS_OFFSET_TL head;
    SS_OFFSET_TL tail;
    // for ssTable
    std::vector<FILE_NUM_TL> levelLargestUidList;
    FILE_NUM_TL largestTimeStamp;
    const int memTableLenThreshold =
        (16 * 1024 - 32 - SS_BLOOM_BYTENUM) / (8 + 8 + 4);
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
                    VLEN_TYPE vlen, const VALUE_TYPE &value);
    void writeVlogEntry(std::ofstream &vlogFile, const VLOG_MAGIC_TL &magic,
                        const VLOG_CHECKSUM_TL &checksum, const KEY_TL &key,
                        const SS_VLEN_TL &vlen, const VALUE_TL &val);
    void writeSSTEntry(std::ofstream &sstFile, const KEY_TL &key,
                       const SS_OFFSET_TL &offset, const SS_VLEN_TL &vlen);
};
