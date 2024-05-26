#pragma once

#include <fstream>
#include <limits>
#include <unordered_map>
#include <vector>

#include "bloomfilter.h"
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
typedef uint64_t FILE_NUM_TL;
typedef FILE_NUM_TL SS_TIMESTAMP_TL;
typedef uint64_t SST_HEADER_KVNUM_TL;
typedef int SST_LEVEL_TL;

#define SS_HEADER_BYTENUM 32
#define SS_BLOOM_BYTENUM 8192  // There are 3 consts related in total
#define SS_KEY_BYTENUM sizeof(KEY_TL)
#define SS_OFFSET_BYTENUM sizeof(SS_OFFSET_TL)
#define SS_VLEN_BYTENUM sizeof(SS_VLEN_TL)
#define DEFAULT_M_VAL 65536
#define SS_MAX_FILE_BYTENUM 16384  // 16 * 1024
#define SS_TIMESTAMP_BYTENUM sizeof(SS_TIMESTAMP_TL)
#define SS_KVNUM_BYTENUM sizeof(SST_HEADER_KVNUM_TL)
#define VLOG_MAGIC_BYTENUM sizeof(VLOG_MAGIC_TL)
#define VLOG_CHECKSUM_BYTENUM sizeof(VLOG_CHECKSUM_TL)
#define SS_FILE_SUFFIX ".sst"
#define VLOG_DEFAULT_MAGIC_VAL 0xff
#define SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM "/level-"
#define MAX_SST_KV_GROUP_NUM 500
#define MAX_CACHE_ENTRIES 1000000
#define SS_ENTRY_BYTENUM (SS_KEY_BYTENUM + SS_OFFSET_BYTENUM + SS_VLEN_BYTENUM)
#define DELETE_MARK "~DELETED~"
#define CONVENTIONAL_MISS_FLAG_OFFSET 1

class KVStore : public KVStoreAPI {
    // You can add your implementation here
   private:
    // std::string dir; // e.g. "./data"
    // std::string vlog; // e.g. "./data/vlog"
    // for vlog
    SS_OFFSET_TL head;
    SS_OFFSET_TL tail;
    // for ssTable
    FILE_NUM_TL largestUid;
    FILE_NUM_TL largestTimeStamp;
    const int memTableLenThreshold =
        (SS_MAX_FILE_BYTENUM - SS_HEADER_BYTENUM - SS_BLOOM_BYTENUM) /
        (SS_KEY_BYTENUM + SS_OFFSET_BYTENUM + SS_VLEN_BYTENUM);
    skiplist_type *memTable;

   public:
    KVStore(const std::string &dir, const std::string &vlog);

    ~KVStore();

    typedef uint64_t KEY_TYPE;
    typedef std::string VALUE_TYPE;
    typedef uint32_t VLEN_TYPE;
    struct SSTHeaderProps {
        FILE_NUM_TL timestamp;
        SST_HEADER_KVNUM_TL kvNum;
        KEY_TL minKey;
        KEY_TL maxKey;
    };
    struct SSTEntryProps {
        KEY_TL key;
        SS_OFFSET_TL offset;
        SS_VLEN_TL vlen;
        SSTEntryProps(KEY_TL key, SS_OFFSET_TL offset, SS_VLEN_TL vlen)
            : key(key), offset(offset), vlen(vlen) {}
        SSTEntryProps() {
            key = 0;
            offset = 0;
            vlen = 0;
        }
    };
    struct CacheItemProps {
        BF bf;
        int kvNum;
        KEY_TL keyList[MAX_SST_KV_GROUP_NUM];
        SS_OFFSET_TL offsetList[MAX_SST_KV_GROUP_NUM];
        SS_VLEN_TL vlenList[MAX_SST_KV_GROUP_NUM];
        SST_LEVEL_TL level;
    };
    struct sstInfoItemProps {
        FILE_NUM_TL uid;
        FILE_NUM_TL timeStamp;
        KEY_TL minKey;
        KEY_TL maxKey;
        // bool hasCached;
        sstInfoItemProps(FILE_NUM_TL _uid, FILE_NUM_TL _timeStamp,
                         KEY_TL _minKey, KEY_TL _maxKey)
            : uid(_uid),
              timeStamp(_timeStamp),
              minKey(_minKey),
              maxKey(_maxKey) {}
    };

    // for cache
    std::unordered_map<FILE_NUM_TL, CacheItemProps *>
        sstCache;  // map `uid` to `cacheItem`
    std::vector<std::vector<sstInfoItemProps>> sstInfoLevelList;

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
    void addToCache(FILE_NUM_TL uid, CacheItemProps *cacheItemPtr);
    CacheItemProps *readFileNGetCacheItem(SST_LEVEL_TL level, FILE_NUM_TL uid);
    void readHeaderPropsFromSST(std::ifstream &file,
                                SSTHeaderProps &headerProps);
    void readBFFromSST(std::ifstream &file, BF &bf);
    void readEntryFromSST(std::ifstream &file, SSTEntryProps &entry,
                          int entryIndex);
    SSTEntryProps findOffsetInCacheItem(CacheItemProps *cacheItemPtr,
                                        KEY_TL key);
    std::string getValueByOffsetNVlen(SS_OFFSET_TL offset, SS_VLEN_TL vlen);

    // debug utils
    void printSST(SST_LEVEL_TL level, FILE_NUM_TL uid) {
        std::cout << "----printSST----\n";
        std::stringstream ss;
        ss << dir << "/" << SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM << level << "/"
           << uid << SS_FILE_SUFFIX;
        std::string filePath = ss.str();
        std::ifstream file(filePath, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to open file: " << filePath << std::endl;
            return;
        }
        SSTHeaderProps headerProps;
        readHeaderPropsFromSST(file, headerProps);
        std::cout << "timestamp: " << headerProps.timestamp
                  << ",kvNum: " << headerProps.kvNum
                  << ",minKey: " << headerProps.minKey
                  << ",maxKey: " << headerProps.maxKey << std::endl;
        // std::cout << "BF: \n";
        // BF bf;
        // readBFFromSST(file, bf);
        // for (int i = 0; i < DEFAULT_M_VAL; i++)
        //     std::cout << (bf.bitArray[i] ? 1 : 0);
        // std::cout << std::endl;
        std::cout << "entries: \n";
        for (int i = 0; i < headerProps.kvNum; i++) {
            SSTEntryProps entry;
            readEntryFromSST(file, entry, i);
            std::cout << "[" << i << ": " << entry.key << ", " << entry.offset
                      << ", " << entry.vlen << "]";
        }
        std::cout << std::endl;
        std::cout << "----END printSST----\n";
    }
    void printSSTCache(SST_LEVEL_TL level, FILE_NUM_TL uid) {
        CacheItemProps *cacheItemPtr = sstCache[uid];
        if (cacheItemPtr == nullptr) {
            std::cerr << "ERR: CacheItemPtr==nullptr\n";
            return;
        }
        std::cout << "----printCache----\n";

        std::cout << ",kvNum: " << cacheItemPtr->kvNum;
        // std::cout << "BF: \n";
        // BF bf;
        // readBFFromSST(file, bf);
        // for (int i = 0; i < DEFAULT_M_VAL; i++)
        //     std::cout << (bf.bitArray[i] ? 1 : 0);
        // std::cout << std::endl;
        std::cout << "entries: \n";
        for (int i = 0; i < cacheItemPtr->kvNum; i++) {
            std::cout << "[" << i << ": " << cacheItemPtr->keyList[i] << ", "
                      << cacheItemPtr->offsetList[i] << ", "
                      << cacheItemPtr->vlenList[i] << "]";
        }
        std::cout << std::endl;
        std::cout << "----END printCache----\n";
    }
};
