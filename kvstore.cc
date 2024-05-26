#include "kvstore.h"

#include <string>
using namespace skiplist;
// #define DET

KVStore::KVStore(const std::string &dir, const std::string &vlog)
    : KVStoreAPI(dir, vlog) {
    memTable = new skiplist_type();

    // TODO: 设置head/tail的值. Belows are temps
    head = 0;
    tail = 0;
    largestUid = -1;
    largestTimeStamp = -1;
}
KVStore::~KVStore() {}
void KVStore::fillCrcObj(std::vector<unsigned char> &crcObj, KEY_TYPE key,
                         VLEN_TYPE vlen, const VALUE_TYPE &value) {
    // Convert key to binary and insert into crcObj
    unsigned char *keyPtr = reinterpret_cast<unsigned char *>(&key);
    crcObj.insert(crcObj.end(), keyPtr, keyPtr + sizeof(KEY_TYPE));

    // Convert vlen to binary and insert into crcObj
    unsigned char *vlenPtr = reinterpret_cast<unsigned char *>(&vlen);
    crcObj.insert(crcObj.end(), vlenPtr, vlenPtr + sizeof(VLEN_TYPE));

    // Convert value to binary and insert into crcObj
    const unsigned char *valuePtr =
        reinterpret_cast<const unsigned char *>(value.data());
    crcObj.insert(crcObj.end(), valuePtr, valuePtr + vlen);
}
void KVStore::convertMemTable2File() {
    // write to vLog
    std::ofstream vlogFile(vlog, std::ios::binary | std::ios::app);
    if (!vlogFile.is_open()) {
        std::cerr << "Failed to open vlogFile: " + vlog + " for writing.\n";
        return;
    }
    std::list<std::pair<KEY_TL, std::string>> list;
    std::vector<SS_OFFSET_TL> offsetList;
    KEY_TL minKey, maxKey;  // for writing to SST
    memTable->scan(0, std::numeric_limits<uint64_t>::max(), list);
    if (!list.empty()) {
        minKey = list.front().first;
        maxKey = list.back().first;
    } else {
        std::cerr << "MemTable is empty.\n";
        return;
    }
    for (const auto &item : list) {
        VLOG_MAGIC_TL magic = 0xff;  // i.e. VLOG_MAGIC_TL
        SS_VLEN_TL vlen;
        std::vector<unsigned char> crcObj;
        if (item.second == DELETE_MARK) {
            vlen = 0;
            fillCrcObj(crcObj, item.first, vlen, "");
        } else {
            vlen = item.second.length();
            fillCrcObj(crcObj, item.first, vlen, item.second);
        }
        VLOG_CHECKSUM_TL checksum = utils::crc16(crcObj);  // checksum
        // start writing
        head = vlogFile.tellp();
        offsetList.push_back(head);
        if (item.second == DELETE_MARK) {
            writeVlogEntry(vlogFile, magic, checksum, item.first, vlen, "");
        } else {
            writeVlogEntry(vlogFile, magic, checksum, item.first, vlen,
                           item.second);
        }
    }
    vlogFile.close();
    // write to SSTable. TODO: 现在只做了全部新建到0层
    FILE_NUM_TL targetLevel = 0;
    // check largestUid
    FILE_NUM_TL newUid = ++largestUid;
    std::string targetPath = dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
                             std::to_string(targetLevel),
                newFileName = std::to_string(newUid) + SS_FILE_SUFFIX,
                newFilePath = targetPath + "/" + newFileName;
    if (!utils::dirExists(targetPath)) utils::_mkdir(targetPath);
    // try to open sstFile
    std::fstream fileCheck(newFilePath);
    if (!fileCheck) {
        // The file does not exist, create a new one.
        std::ofstream newFile(newFilePath);
        if (!newFile) {
            std::cerr << "Failed to create the file: " << newFilePath << "\n";
            return;
        }
        newFile.close();
    }
    fileCheck.close();
    // Now, the file is guaranteed to exist. Open it and write.
    std::ofstream sstFile(newFilePath,
                          std::ios::app);  // Open the file in append mode.
    if (!sstFile) {
        std::cerr << "Failed to open the file: " << newFilePath << "\n";
        return;
    }
    FILE_NUM_TL curTimeStamp = ++largestTimeStamp;
    SST_HEADER_KVNUM_TL kvNum = list.size();
    sstFile.write(reinterpret_cast<const char *>(&curTimeStamp),
                  sizeof(curTimeStamp));
    sstFile.write(reinterpret_cast<const char *>(&kvNum), sizeof(kvNum));
    sstFile.write(reinterpret_cast<const char *>(&minKey), sizeof(minKey));
    sstFile.write(reinterpret_cast<const char *>(&maxKey), sizeof(maxKey));
    // write bloom filter to SST
    BF newBF;
    for (const auto &item : list) newBF.insert(item.first);
    char buffer = 0;
    for (int i = 0; i < DEFAULT_M_VAL; i++) {
        buffer = (buffer << 1) | newBF.bitArray[i];
        if ((i + 1) % 8 == 0) {
            sstFile.write(reinterpret_cast<const char *>(&buffer),
                          sizeof(buffer));
            buffer = 0;
        }
    }

    int index = -1;
    for (const auto &item : list) {
        index++;
        writeSSTEntry(sstFile, item.first, offsetList[index],
                      item.second == DELETE_MARK ? 0 : item.second.length());
    }
    sstFile.close();
    // 更新sstInfo
    if (sstInfoLevelList.size() < targetLevel + 1) {
        sstInfoLevelList.resize(targetLevel + 1);
    }
    sstInfoLevelList[targetLevel].push_back(
        sstInfoItemProps(newUid, curTimeStamp, minKey, maxKey));

    // TODO: 从第0层开始处理可能的compaction
    // 多余，debug用
    // sstFile.close();
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    if (memTable->getLength() < memTableLenThreshold)
        memTable->put(key, s);
    else {
        // length == threshold
        if (memTable->get(key).has_value())
            memTable->put(key, s);
        else {
            // memTable overflow, don't insert now
#ifdef DET
            std::cout << "memtable is full. Start to convert to sst.\n"
                      << "At 'key'=" << key << std::endl;
#endif
            convertMemTable2File();
            clearMemTable();
            memTable->put(key, s);
        }
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::optional<std::string> res = memTable->get(key);
    if (res.has_value()) return res.value() != DELETE_MARK ? res.value() : "";
    // cannot find in Memtable
    for (size_t i = 0; i < sstInfoLevelList.size(); ++i) {
        auto &level = sstInfoLevelList[i];

        // 暂时方案：sst全放第0层。这样不能在找到时直接返回，必须收集第0层所有可能的结果，比较时间戳。
        // 简化：在第0层从后往前搜，找到时直接返回。但是引入compaction之后需要大改
        for (auto itemIter = level.rbegin(); itemIter != level.rend();
             ++itemIter) {
            auto &item = *itemIter;
            FILE_NUM_TL curUid = item.uid;
            if (sstCache.find(item.uid) == sstCache.end()) {
                CacheItemProps *cacheItemPtr = readFileNGetCacheItem(i, curUid);
                addToCache(item.uid, cacheItemPtr);
            }
            // check sst in cache
            if (sstCache.find(curUid) == sstCache.end()) {
                std::cerr << "ERR: cannot find curUid in sstCache. sstCache & "
                             "isCached are not consistent.\n";
            }
            CacheItemProps *cacheItemPtr = sstCache[curUid];
            SSTEntryProps offsetRes = findOffsetInCacheItem(cacheItemPtr, key);
            if (offsetRes.offset == CONVENTIONAL_MISS_FLAG_OFFSET) {
                // not found in this SST
            } else {
                // found in this SST
                if (!offsetRes.vlen) return "";
                return getValueByOffsetNVlen(offsetRes.offset, offsetRes.vlen);
            }
        }
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    // std::optional<std::string> res = memTable->get(key);
    // if (!(res.has_value() && res.value() != "~DELETED")) return false;
    std::string res = get(key);
    if (res == "") return false;
    // memTable->put(key, DELETE_MARK);
    put(key, DELETE_MARK);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    delete memTable;
    memTable = new skiplist_type();
    // delete all SST & VLOG
    std::vector<std::string> nameList;
    int fileNum = utils::scanDir(dir, nameList);
    for (int i = 0; i < fileNum; i++) {
        std::string curName = nameList[i];
        std::string dirPath = dir + "/" + curName;
        if (utils::dirExists(dirPath)) {
            // it is a directory
            std::vector<std::string> fileNameList;
            int secondFileNum = utils::scanDir(dirPath, fileNameList);
            for (int j = 0; j < secondFileNum; j++)
                utils::rmfile(dirPath + "/" + fileNameList[j]);
            utils::rmdir(dirPath);
        } else {
            // it is a file
            utils::rmfile(dirPath);
        }
    }
    // delete cache
    sstInfoLevelList.clear();
    for (auto &item : sstCache) {
        if (item.second != nullptr) delete item.second;
    }
    sstCache.clear();

    head = 0, tail = 0;
    // others
    largestUid = -1, largestTimeStamp = -1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty list indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2,
                   std::list<std::pair<uint64_t, std::string>> &list) {
    list.clear();
    memTable->scan(key1, key2, list);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid
 * value. chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {}

void KVStore::writeVlogEntry(std::ofstream &vlogFile,
                             const VLOG_MAGIC_TL &magic,
                             const VLOG_CHECKSUM_TL &checksum,
                             const KEY_TL &key, const SS_VLEN_TL &vlen,
                             const VALUE_TL &val) {
    const char *datap = reinterpret_cast<const char *>(&magic);
    vlogFile.write(datap, sizeof(magic));
    datap = reinterpret_cast<const char *>(&checksum);
    vlogFile.write(datap, sizeof(checksum));
    datap = reinterpret_cast<const char *>(&key);
    vlogFile.write(datap, sizeof(key));
    datap = reinterpret_cast<const char *>(&vlen);
    vlogFile.write(datap, sizeof(vlen));
    datap = val.data();
    vlogFile.write(datap, vlen);
}
void KVStore::writeSSTEntry(std::ofstream &sstFile, const KEY_TL &key,
                            const SS_OFFSET_TL &offset,
                            const SS_VLEN_TL &vlen) {
    sstFile.write(reinterpret_cast<const char *>(&key), sizeof(key));
    sstFile.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
    sstFile.write(reinterpret_cast<const char *>(&vlen), sizeof(vlen));
}

void KVStore::addToCache(FILE_NUM_TL uid,
                         KVStore::CacheItemProps *cacheItemPtr) {
    // 检查缓存大小
    if (sstCache.size() >= MAX_CACHE_ENTRIES) {
        // 如果缓存已满，随机删除一个元素
        auto it = sstCache.begin();
        // for (auto &item : sstInfoLevelList[it->second->level]) {
        //     if (item.uid == it->first) {
        //         if (item.hasCached) {
        //             item.hasCached = 0;
        //             break;
        //         } else
        //             std::cerr << "ERR: when deleting cache items, its "
        //                          "hasCached not consistent\n";
        //     }
        // }
        if (it->second != nullptr) delete it->second;
        sstCache.erase(it);
    }
    // 添加新元素
    sstCache[uid] = cacheItemPtr;
}

KVStore::CacheItemProps *KVStore::readFileNGetCacheItem(SST_LEVEL_TL level,
                                                        FILE_NUM_TL uid) {
    std::stringstream ss;
    ss << dir << "/" << SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM << level << "/"
       << uid << SS_FILE_SUFFIX;
    std::string filePath = ss.str();
    std::ifstream file(filePath, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filePath << std::endl;
        return nullptr;
    }
    CacheItemProps *cacheItemPtr = new CacheItemProps();
    // read bf
    readBFFromSST(file, cacheItemPtr->bf);
    // read kvNum&level
    SSTHeaderProps headerProps;
    readHeaderPropsFromSST(file, headerProps);
    cacheItemPtr->kvNum = headerProps.kvNum;
    cacheItemPtr->level = level;
    // read 3 lists
    for (int i = 0; i < cacheItemPtr->kvNum; i++) {
        SSTEntryProps entry;
        readEntryFromSST(file, entry, i);
        cacheItemPtr->keyList[i] = entry.key;
        cacheItemPtr->offsetList[i] = entry.offset;
        cacheItemPtr->vlenList[i] = entry.vlen;
    }

    file.close();
    return cacheItemPtr;
}

void KVStore::readHeaderPropsFromSST(std::ifstream &file,
                                     SSTHeaderProps &headerProps) {
    file.seekg(0);
    char buffer[SS_HEADER_BYTENUM];  // bytes
    file.read(buffer, SS_TIMESTAMP_BYTENUM);
    headerProps.timestamp = *reinterpret_cast<uint64_t *>(buffer);
    file.read(buffer, SS_KVNUM_BYTENUM);
    headerProps.kvNum = *reinterpret_cast<uint64_t *>(buffer);
    file.read(buffer, SS_KEY_BYTENUM);
    headerProps.minKey = *reinterpret_cast<uint64_t *>(buffer);
    file.read(buffer, SS_KEY_BYTENUM);
    headerProps.maxKey = *reinterpret_cast<uint64_t *>(buffer);
}
void KVStore::readBFFromSST(std::ifstream &file, BF &bf) {
    file.seekg(SS_HEADER_BYTENUM);
    char buffer[SS_BLOOM_BYTENUM];  // bytes
    file.read(buffer, SS_BLOOM_BYTENUM);
    // fill into bf.bitArray
    for (int i = 0; i < SS_BLOOM_BYTENUM; ++i) {
        for (int j = 0; j < 8; ++j) {
            bf.bitArray[i * 8 + j] = (buffer[i] & (1 << (7 - j))) != 0;
        }
    }
}
void KVStore::readEntryFromSST(std::ifstream &file, SSTEntryProps &entry,
                               int entryIndex) {
    file.seekg(SS_HEADER_BYTENUM + SS_BLOOM_BYTENUM +
               SS_ENTRY_BYTENUM * entryIndex);
    char buffer[SS_ENTRY_BYTENUM];  // bytes
    file.read(buffer, SS_KEY_BYTENUM);
    entry.key = *reinterpret_cast<uint64_t *>(buffer);
    file.read(buffer, SS_OFFSET_BYTENUM);
    entry.offset = *reinterpret_cast<uint64_t *>(buffer);
    file.read(buffer, SS_VLEN_BYTENUM);
    entry.vlen = *reinterpret_cast<uint64_t *>(buffer);
}
// findOffsetInCacheItem - 根据key在cacheItem中寻找vlogOffset
// 若找不到，返回{xxx,CONVENTIONAL_MISS_FLAG_OFFSET,xxx}
KVStore::SSTEntryProps KVStore::findOffsetInCacheItem(
    KVStore::CacheItemProps *cacheItemPtr, KEY_TL key) {
    KVStore::SSTEntryProps entry(0, CONVENTIONAL_MISS_FLAG_OFFSET, 0);
    if (!(cacheItemPtr->bf.query(key))) return entry;
    // start binary search
    int head = 0, tail = (cacheItemPtr->kvNum) - 1;
    while (head <= tail) {
        int mid = (head + tail) / 2;
        KEY_TL midKey = cacheItemPtr->keyList[mid];
        if (key < midKey)
            tail = mid - 1;
        else if (key > midKey)
            head = mid + 1;
        else {
            entry.key = key, entry.offset = cacheItemPtr->offsetList[mid],
            entry.vlen = cacheItemPtr->vlenList[mid];
            return entry;
        }
    }
    return entry;
}
std::string KVStore::getValueByOffsetNVlen(SS_OFFSET_TL offset,
                                           SS_VLEN_TL vlen) {
    // TODO: 考虑更新tail，并从tail开始读，而非0开始
    std::ifstream file(vlog, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << vlog << std::endl;
        return "";
    }
    file.seekg(tail + offset + VLOG_MAGIC_BYTENUM + VLOG_CHECKSUM_BYTENUM +
               SS_KEY_BYTENUM + SS_VLEN_BYTENUM);  // slippery
    char buffer[vlen + 1];
    file.read(buffer, vlen);
    buffer[vlen] = '\0';
    std::string value(buffer, vlen);
    return value;
}
