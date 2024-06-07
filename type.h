#include <cstdint>
#include <string>

// BloomFilter
#define DEFAULT_HASHFUN_NUM 1
#define DEFAULT_M_VAL 4096

// KVStore
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
#define SS_BLOOM_BYTENUM (DEFAULT_M_VAL / 8)
#define SS_KEY_BYTENUM sizeof(KEY_TL)
#define SS_OFFSET_BYTENUM sizeof(SS_OFFSET_TL)
#define SS_VLEN_BYTENUM sizeof(SS_VLEN_TL)
#define SS_MAX_FILE_BYTENUM 16384  // 16 * 1024
#define SS_TIMESTAMP_BYTENUM sizeof(SS_TIMESTAMP_TL)
#define SS_KVNUM_BYTENUM sizeof(SST_HEADER_KVNUM_TL)
#define VLOG_MAGIC_BYTENUM sizeof(VLOG_MAGIC_TL)
#define VLOG_CHECKSUM_BYTENUM sizeof(VLOG_CHECKSUM_TL)
#define SS_FILE_SUFFIX ".sst"
#define VLOG_DEFAULT_MAGIC_VAL 0xff
#define SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM "/level-"
#define MAX_SST_KV_GROUP_NUM 792  // actually same as memTableLenThreshold
#define MAX_CACHE_ENTRIES 1000000
#define SS_ENTRY_BYTENUM (SS_KEY_BYTENUM + SS_OFFSET_BYTENUM + SS_VLEN_BYTENUM)
#define DELETE_MARK "~DELETED~"
#define CONVENTIONAL_MISS_FLAG_OFFSET 1
#define MAX_FILE_NUM_GIVEN_LEVEL(level) ((FILE_NUM_TL)1 << (level + 1))
#define WATCHED_KEY 0
#define WATCHED_GC_KEY 0
#define WATCHED_FILEUID 0