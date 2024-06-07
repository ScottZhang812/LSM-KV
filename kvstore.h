#pragma once

#include <algorithm>
#include <cassert>
#include <fstream>
#include <limits>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "bloomfilter.h"
#include "kvstore_api.h"
#include "skiplist.h"
#include "type.h"
#include "utils.h"
using namespace skiplist;

class KVStore : public KVStoreAPI {
   private:
    std::string dir;
    std::string vlog;
    // for vlog
    SS_OFFSET_TL head;
    SS_OFFSET_TL tail;
    // for ssTable
    FILE_NUM_TL largestUid;
    FILE_NUM_TL largestTimeStamp;
    const int memTableLenThreshold =
        (SS_MAX_FILE_BYTENUM - SS_HEADER_BYTENUM - SS_BLOOM_BYTENUM) /
        (SS_KEY_BYTENUM + SS_OFFSET_BYTENUM + SS_VLEN_BYTENUM);
    const int OPTION_1 = 0;
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
        SST_HEADER_KVNUM_TL kvNum;
        KEY_TL minKey;
        KEY_TL maxKey;
        KEY_TL keyList[MAX_SST_KV_GROUP_NUM];
        SS_OFFSET_TL offsetList[MAX_SST_KV_GROUP_NUM];
        SS_VLEN_TL vlenList[MAX_SST_KV_GROUP_NUM];
        BF bf;
        // bool hasCached;
        sstInfoItemProps(FILE_NUM_TL _uid, FILE_NUM_TL _timeStamp,
                         SST_HEADER_KVNUM_TL _kvNum, KEY_TL _minKey,
                         KEY_TL _maxKey,
                         std::list<KEY_TL>::const_iterator _keyList,
                         std::vector<SS_OFFSET_TL>::const_iterator _offsetList,
                         std::vector<SS_VLEN_TL>::const_iterator _vlenList)
            : uid(_uid),
              timeStamp(_timeStamp),
              kvNum(_kvNum),
              minKey(_minKey),
              maxKey(_maxKey),
              bf() {
            for (SST_HEADER_KVNUM_TL i = 0; i < kvNum; i++) {
                keyList[i] = *_keyList;
                offsetList[i] = _offsetList[i];
                vlenList[i] = _vlenList[i];
                bf.insert(keyList[i]);
                _keyList++;
            }
        }
        sstInfoItemProps() : bf() {}
        sstInfoItemProps(const sstInfoItemProps &other)
            : uid(other.uid),
              timeStamp(other.timeStamp),
              kvNum(other.kvNum),
              minKey(other.minKey),
              maxKey(other.maxKey),
              bf(other.bf) {
            for (SST_HEADER_KVNUM_TL i = 0; i < kvNum; i++) {
                keyList[i] = other.keyList[i];
                offsetList[i] = other.offsetList[i];
                vlenList[i] = other.vlenList[i];
            }
        }
    };
    struct PtrTrackProps {
        std::vector<sstInfoItemProps> fileInfoList;
        PtrTrackProps(std::vector<sstInfoItemProps> _fileInfoList)
            : fileInfoList(std::move(_fileInfoList)) {}
    };
    // for cache
    std::unordered_map<FILE_NUM_TL, sstInfoItemProps *>
        hashCachePtrByUid;  // map `uid` to cachePtr
    std::vector<std::multimap<KEY_TL, sstInfoItemProps>>
        levelCache;  // slippery. Ordered by: minKey
    static std::vector<PtrTrackProps> ptrTracks;
    // for priority_queue
    struct TrackPointerProps {
        FILE_NUM_TL fileIndex;
        size_t CacheItemIndex;
        size_t trackIndex;

        TrackPointerProps(FILE_NUM_TL _fileIndex, size_t _CacheItemIndex,
                          size_t _trackIndex)
            : fileIndex(_fileIndex),
              CacheItemIndex(_CacheItemIndex),
              trackIndex(_trackIndex) {}
    };
    struct CompareForPointerQueue {
        // to get minimum, just define `>`
        bool operator()(const TrackPointerProps &a,
                        const TrackPointerProps &b) const {
            KEY_TL keya = ptrTracks[a.trackIndex]
                              .fileInfoList[a.fileIndex]
                              .keyList[a.CacheItemIndex],
                   keyb = ptrTracks[b.trackIndex]
                              .fileInfoList[b.fileIndex]
                              .keyList[b.CacheItemIndex];
            if (keya > keyb) return true;
            if (keya < keyb) return false;
            // keya == keyb
            // SLIPPERY
            return ptrTracks[a.trackIndex]
                       .fileInfoList[a.fileIndex]
                       .offsetList[a.CacheItemIndex] <
                   ptrTracks[b.trackIndex]
                       .fileInfoList[b.fileIndex]
                       .offsetList[b.CacheItemIndex];
            // SLIPPERY: 若不看offset，而根据uid/timestamp，可能存在反例
            SS_TIMESTAMP_TL
            timestampA =
                ptrTracks[a.trackIndex].fileInfoList[a.fileIndex].timeStamp,
            timestampB =
                ptrTracks[b.trackIndex].fileInfoList[b.fileIndex].timeStamp;
            if (timestampA != timestampB) return timestampA < timestampB;
            // timestampA == timestampB
            return ptrTracks[a.trackIndex].fileInfoList[a.fileIndex].uid <
                   ptrTracks[b.trackIndex].fileInfoList[b.fileIndex].uid;
        }
    };

    void clearMemTable() {
        if (memTable) delete memTable;
        memTable = new skiplist_type();
    }
    void convertAndWriteMemTable();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key);
    void getOffset(uint64_t key, SS_OFFSET_TL *userOffsetPtr);
    bool del(uint64_t key) override;
    void reset() override;
    void scan(uint64_t key1, uint64_t key2,
              std::list<std::pair<uint64_t, std::string>> &list) override;
    void gc(uint64_t chunk_size) override;

    // utils
    void getValueOrOffset(uint64_t key, std::string &userStr,
                          SS_OFFSET_TL *userOffsetPtr = nullptr);
    void fillCrcObj(std::vector<unsigned char> &crcObj, KEY_TYPE key,
                    VLEN_TYPE vlen, const VALUE_TYPE &value);
    void writeVlogEntry(std::ofstream &vlogFile, const VLOG_MAGIC_TL &magic,
                        const VLOG_CHECKSUM_TL &checksum, const KEY_TL &key,
                        const SS_VLEN_TL &vlen, const VALUE_TL &val);
    void writeSSTEntry(std::ofstream &sstFile, const KEY_TL &key,
                       const SS_OFFSET_TL &offset, const SS_VLEN_TL &vlen);
    void readFileNGetFileItem(std::string filePath, sstInfoItemProps &fileItem);
    void readHeaderPropsFromSST(std::ifstream &file,
                                SSTHeaderProps &headerProps);
    void readBFFromSST(std::ifstream &file, BF &bf);
    void readEntryFromSST(std::ifstream &file, SSTEntryProps &entry,
                          int entryIndex);
    template <typename T>
    void readDataFromVlog(std::ifstream &file, T &userdata,
                          SS_OFFSET_TL offset);
    void readValStringFromVlog(std::ifstream &file, std::string &userString,
                               SS_OFFSET_TL offset, SS_VLEN_TL vlen);
    SSTEntryProps findOffsetInSSTInfoItemPtr(sstInfoItemProps *sstInfoItemPtr,
                                             KEY_TL key);
    std::string getValueByOffsetnVlen(SS_OFFSET_TL offset, SS_VLEN_TL vlen);
    bool crcCheck(const VLOG_CHECKSUM_TL &curChecksum, const KEY_TL &curKey,
                  const SS_VLEN_TL &curVlen, const std::string &curValue);
    VLOG_CHECKSUM_TL calcChecksum(const KEY_TL &curKey,
                                  const SS_VLEN_TL &curVlen,
                                  const std::string &curValue);
    void deleteSSTInDisknCache(FILE_NUM_TL uid, FILE_NUM_TL level,
                               KEY_TL minKey);
    void mergeFilesAndReturnKOVList(std::vector<PtrTrackProps> &ptrTracks,
                                    std::list<KEY_TL> &keyList,
                                    std::vector<SS_OFFSET_TL> &offsetList,
                                    std::vector<SS_VLEN_TL> &vlenList,
                                    FILE_NUM_TL targetLevel);
    void generateSSTList(const std::list<KEY_TL> &keyList,
                         const std::vector<SS_OFFSET_TL> &offsetList,
                         const std::vector<SS_VLEN_TL> &vlenList,
                         std::vector<sstInfoItemProps> &userSSTList,
                         SS_TIMESTAMP_TL timestampToWrite);
    void writeSSTToDisk(FILE_NUM_TL level, std::vector<sstInfoItemProps> &list);
    void writeSSTToCache(FILE_NUM_TL level,
                         std::vector<sstInfoItemProps> &list);
    void writeKOVListToDiskAndCache(std::list<KEY_TL> &keyList,
                                    std::vector<SS_OFFSET_TL> &offsetList,
                                    std::vector<SS_VLEN_TL> &vlenList,
                                    FILE_NUM_TL sonLevel,
                                    SS_TIMESTAMP_TL maxTimeStampToWrite);
    void examineOld();
    /*
     *debug utils and less important utils
     */
    void printSST(SST_LEVEL_TL level, FILE_NUM_TL uid);
    void printSSTCache(SST_LEVEL_TL level, FILE_NUM_TL uid);
    void printUidContainsWatchedKey();
    bool checkTailCandidateValidity(int candidate);
    static bool compareReorderVec(const sstInfoItemProps &a,
                                  const sstInfoItemProps &b) {
        if (a.timeStamp < b.timeStamp) return true;
        if (a.timeStamp > b.timeStamp) return false;
        return a.minKey < b.minKey;
    }
    SS_OFFSET_TL getHead() { return head; }
    SS_OFFSET_TL getTail() { return tail; }
    void printQueue(
        std::priority_queue<TrackPointerProps, std::vector<TrackPointerProps>,
                            CompareForPointerQueue> &pointerQueue) {
        std::cout << "\\";
        while (!pointerQueue.empty()) {
            TrackPointerProps top = pointerQueue.top();
            std::cout << "Key: "
                      << ptrTracks[top.trackIndex]
                             .fileInfoList[top.fileIndex]
                             .keyList[top.CacheItemIndex]
                      << ", Timestamp: "
                      << ptrTracks[top.trackIndex]
                             .fileInfoList[top.fileIndex]
                             .timeStamp
                      << " - ";
            pointerQueue.pop();
        }
        std::cout << "//";
        fflush(stdout);
    }
    void lookInMemtable(KEY_TL key);
    void moveToFirstMagicPos(std::ifstream &vlogFile,
                             SS_OFFSET_TL &tailCandidate) {
        if (tailCandidate == head) return;
        VLOG_MAGIC_TL tmpMagic;
        readDataFromVlog(vlogFile, tmpMagic, tailCandidate++);
        while (tmpMagic != VLOG_DEFAULT_MAGIC_VAL) {
            readDataFromVlog(vlogFile, tmpMagic, tailCandidate++);
            if (tailCandidate == head) return;
        }
        tailCandidate--;
    }
    bool enableCache = true;
    bool enableBf = true;
    void disableCache() {
        enableCache = false;
        enableBf = true;
    }
    void enableIndexCache() {
        enableCache = true;
        enableBf = false;
    }
    void enableBloomFilterCache() {
        enableCache = true;
        enableBf = true;
    }
};
