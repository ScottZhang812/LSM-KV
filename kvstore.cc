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
    largestUid = 0;
    largestTimeStamp = 0;
}
KVStore::~KVStore() {}

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
            convertAndWriteMemTable();
            clearMemTable();
            memTable->put(key, s);
        }
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 * userOffsetPtr用于复用本函数，用于返回一个offset给gc()，便于校验是否为最新数据
 */
std::string KVStore::get(uint64_t key, SS_OFFSET_TL *userOffsetPtr) {
    std::optional<std::string> res = memTable->get(key);
    if (res.has_value()) return res.value() != DELETE_MARK ? res.value() : "";
    // cannot find in Memtable
    SS_TIMESTAMP_TL maxTimeStamp = 0;
    VALUE_TL ans = "";
    SS_OFFSET_TL offsetAns = CONVENTIONAL_MISS_FLAG_OFFSET;
    for (size_t i = 0; i < levelCache.size(); ++i) {
        auto &level = levelCache[i];

        // 暂时方案：sst全放第0层。这样不能在找到时直接返回，必须收集第0层所有可能的结果，比较时间戳。
        // 简化：在第0层从后往前搜，找到时直接返回。但是引入compaction之后需要大改
        for (auto itemIter = level.begin(); itemIter != level.end();
             ++itemIter) {
            auto &item = *itemIter;
            FILE_NUM_TL curUid = item.second.uid;
            sstInfoItemProps *sstInfoItemPtr = hashCachePtrByUid[curUid];
            SSTEntryProps offsetRes =
                findOffsetInSSTInfoItemPtr(sstInfoItemPtr, key);
            if (offsetRes.offset == CONVENTIONAL_MISS_FLAG_OFFSET) {
                // not found in this SST
            } else {
                // found in this SST
                if (!offsetRes.vlen) {
                    if (sstInfoItemPtr->timeStamp > maxTimeStamp) {
                        maxTimeStamp = sstInfoItemPtr->timeStamp;
                        ans = "";
                        offsetAns = offsetRes.offset;
                    }
                }
                // if (userOffsetPtr != nullptr) {
                //     *userOffsetPtr = offsetRes.offset;
                //     return "";
                // }
                if (sstInfoItemPtr->timeStamp > maxTimeStamp) {
                    maxTimeStamp = sstInfoItemPtr->timeStamp;
                    ans =
                        getValueByOffsetnVlen(offsetRes.offset, offsetRes.vlen);
                    offsetAns = offsetRes.offset;
                }
            }
        }
    }
    if (userOffsetPtr != nullptr) {
        if (offsetAns != CONVENTIONAL_MISS_FLAG_OFFSET)
            *userOffsetPtr = offsetAns;
        return "";
    }
    return ans;
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
    clearMemTable();
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
    levelCache.clear();
    hashCachePtrByUid.clear();

    head = 0, tail = 0;
    // others
    largestUid = 0, largestTimeStamp = 0;
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
void KVStore::gc(uint64_t chunk_size) {
    uint64_t scannedBytes = 0;
    while (scannedBytes < chunk_size) {
        if (!checkTailCandidateValidity(tail)) {
            std::cerr << "ERR: `tail` is invaild\n";
            return;
        }

        std::ifstream vlogFile(vlog, std::ios::binary);
        KEY_TL curKey;
        SS_OFFSET_TL offsetRecord =
            tail + VLOG_MAGIC_BYTENUM + VLOG_CHECKSUM_BYTENUM;
        readDataFromVlog(vlogFile, curKey, offsetRecord);
        SS_OFFSET_TL sstOffsetRes = CONVENTIONAL_MISS_FLAG_OFFSET;
        get(curKey, &sstOffsetRes);
        SS_VLEN_TL curVlen;
        offsetRecord += SS_KEY_BYTENUM;
        readDataFromVlog(vlogFile, curVlen, offsetRecord);
        if (sstOffsetRes == CONVENTIONAL_MISS_FLAG_OFFSET ||
            sstOffsetRes != tail) {
            // not latest data. do nothing
        } else {
            // is latest data
            std::string curValue;
            offsetRecord += SS_VLEN_BYTENUM;
            readValStringFromVlog(vlogFile, curValue, offsetRecord, curVlen);
            put(curKey, curValue);
        }

        // dig hole
        vlogFile.close();
        SS_OFFSET_TL curEntryLen = VLOG_MAGIC_BYTENUM + VLOG_CHECKSUM_BYTENUM +
                                   SS_KEY_BYTENUM + SS_VLEN_BYTENUM + curVlen;
        if (utils::de_alloc_file(vlog, tail, curEntryLen)) {
            std::cerr << "ERR: failed at de_ALLOC_FILE()\n";
            return;
        }
        // 更新tail
        tail += curEntryLen;
        scannedBytes += curEntryLen;
    }
    if (memTable->getLength()) {
        convertAndWriteMemTable();
        clearMemTable();
    }
}

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
void KVStore::convertAndWriteMemTable() {
    // write to vLog
    std::ofstream vlogFile(vlog, std::ios::binary | std::ios::app);
    if (!vlogFile.is_open()) {
        std::cerr << "Failed to open vlogFile: " + vlog + " for writing.\n";
        return;
    }
    std::list<std::pair<KEY_TL, std::string>> list;
    std::vector<SS_OFFSET_TL> offsetList;
    std::vector<SS_VLEN_TL> vlenList;
    memTable->scan(0, std::numeric_limits<uint64_t>::max(), list);
    for (const auto &item : list) {
        VLOG_MAGIC_TL magic = 0xff;  // i.e. VLOG_MAGIC_TL
        SS_VLEN_TL vlen;
        VLOG_CHECKSUM_TL checksum;
        if (item.second == DELETE_MARK) {
            vlen = 0;
            checksum = calcChecksum(item.first, vlen, "");
        } else {
            vlen = item.second.length();
            checksum = calcChecksum(item.first, vlen, item.second);
        }
        // start writing
        head = vlogFile.tellp();
        offsetList.push_back(head);
        vlenList.push_back(vlen);
        if (item.second != DELETE_MARK)
            writeVlogEntry(vlogFile, magic, checksum, item.first, vlen,
                           item.second);
    }
    head = vlogFile.tellp();
    vlogFile.close();

    // 根据key/offset/vlenList生成若干个sstFile
    std::vector<sstInfoItemProps> SSTList;
    std::list<KEY_TL> keyList;
    std::transform(
        list.begin(), list.end(), std::back_inserter(keyList),
        [](const std::pair<KEY_TL, std::string> &pair) { return pair.first; });
    generateSSTList(keyList, offsetList, vlenList, SSTList, ++largestTimeStamp);

    // write to SSTable.
    FILE_NUM_TL targetLevel = 0;
    // 首先写入硬盘
    writeSSTToDisk(targetLevel, SSTList);
    // 接着写入缓存
    // slippery
    writeSSTToCache(targetLevel, SSTList);

    // // 判断新增sst后，第0层是否溢出
    // // 先尝试更新sstInfo
    // if (sstInfoLevelList.size() < targetLevel + 1) {
    //     sstInfoLevelList.resize(targetLevel + 1);
    // }
    // sstInfoLevelList[targetLevel].insert(
    //     sstInfoItemProps(newUid, curTimeStamp, minKey, maxKey, kvNum));
    // if (sstInfoLevelList[targetLevel].size() >
    //     MAX_FILE_NUM_GIVEN_LEVEL(targetLevel)) {
    //     // 从level0合并
    //     std::vector<PtrTrackProps> ptrTracks;
    //     // 生成前3条track
    //     std::vector<KEY_TL> tmpMinKeyVector;
    //     std::vector<KEY_TL> tmpMaxKeyVector;
    //     for (int i = 0; i < 3; ++i) {
    //         auto it = sstInfoLevelList[targetLevel].begin();
    //         std::advance(it, i);
    //         std::vector<sstInfoItemProps> tmpVector = {*it};
    //         ptrTracks.push_back(PtrTrackProps(tmpVector));
    //         tmpMinKeyVector.push_back(tmpVector[0].minKey);
    //         tmpMaxKeyVector.push_back(tmpVector[0].maxKey);
    //     }
    //     KEY_TL intervalMinKey =
    //         *std::min_element(tmpMinKeyVector.begin(),
    //         tmpMinKeyVector.end());
    //     KEY_TL intervalMaxKey =
    //         *std::max_element(tmpMaxKeyVector.begin(),
    //         tmpMaxKeyVector.end());
    //     FILE_NUM_TL sonLevel = targetLevel + 1;
    //     if (sstInfoLevelList.size() - 1 >= sonLevel &&
    //         !sstInfoLevelList[sonLevel].empty()) {
    //         std::vector<sstInfoItemProps> tmpFileInfoList;
    //         for (auto &sonItem : sstInfoLevelList[sonLevel]) {
    //             if (sonItem.minKey <= intervalMaxKey &&
    //                 sonItem.maxKey >= intervalMinKey) {
    //                 tmpFileInfoList.push_back(sonItem);
    //             }
    //         }
    //         if (!tmpFileInfoList.empty())
    //             ptrTracks.push_back(PtrTrackProps(tmpFileInfoList));
    //     }
    //     // 开始归并成新文件
    //     mergeFilesAndAddToDiskAndCache(ptrTracks, sonLevel);
    //     // ptrTracks已组装完成，删除所有旧文件
    //     for (auto &trackItem : ptrTracks) {
    //         for (auto &fileInfoItem : trackItem.fileInfoList) {
    //             deleteSSTInDisknCache(fileInfoItem.uid, targetLevel);
    //         }
    //     }
    //     // 判断sonLevel的文件数是否达到上限
    // }
}

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
    headerProps.timestamp = *reinterpret_cast<SS_TIMESTAMP_TL *>(buffer);
    file.read(buffer, SS_KVNUM_BYTENUM);
    headerProps.kvNum = *reinterpret_cast<SST_HEADER_KVNUM_TL *>(buffer);
    file.read(buffer, SS_KEY_BYTENUM);
    headerProps.minKey = *reinterpret_cast<KEY_TL *>(buffer);
    file.read(buffer, SS_KEY_BYTENUM);
    headerProps.maxKey = *reinterpret_cast<KEY_TL *>(buffer);
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
    entry.key = *reinterpret_cast<KEY_TL *>(buffer);
    file.read(buffer, SS_OFFSET_BYTENUM);
    entry.offset = *reinterpret_cast<SS_OFFSET_TL *>(buffer);
    file.read(buffer, SS_VLEN_BYTENUM);
    entry.vlen = *reinterpret_cast<SS_VLEN_TL *>(buffer);
}
// findOffsetInCacheItem - 根据key在cacheItem中寻找vlogOffset
// 若找不到，返回{xxx,CONVENTIONAL_MISS_FLAG_OFFSET,xxx}
KVStore::SSTEntryProps KVStore::findOffsetInSSTInfoItemPtr(
    KVStore::sstInfoItemProps *sstInfoItemPtr, KEY_TL key) {
    KVStore::SSTEntryProps entry(0, CONVENTIONAL_MISS_FLAG_OFFSET, 0);
    if (!(sstInfoItemPtr->bf.query(key))) return entry;
    // start binary search
    int left = 0, right = (sstInfoItemPtr->kvNum) - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        KEY_TL midKey = sstInfoItemPtr->keyList[mid];
        if (key < midKey)
            right = mid - 1;
        else if (key > midKey)
            left = mid + 1;
        else {
            entry.key = key, entry.offset = sstInfoItemPtr->offsetList[mid],
            entry.vlen = sstInfoItemPtr->vlenList[mid];
            return entry;
        }
    }
    return entry;
}
std::string KVStore::getValueByOffsetnVlen(SS_OFFSET_TL offset,
                                           SS_VLEN_TL vlen) {
    // TODO: 考虑更新tail，并从tail开始读，而非0开始
    std::ifstream file(vlog, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << vlog << std::endl;
        return "";
    }
    file.seekg(offset + VLOG_MAGIC_BYTENUM + VLOG_CHECKSUM_BYTENUM +
               SS_KEY_BYTENUM + SS_VLEN_BYTENUM);  // slippery
    char buffer[vlen + 1];
    file.read(buffer, vlen);
    buffer[vlen] = '\0';
    std::string value(buffer, vlen);
    return value;
}
template <typename T>
void KVStore::readDataFromVlog(std::ifstream &file, T &userdata,
                               SS_OFFSET_TL offset) {
    size_t tsize = sizeof(T);
    file.seekg(offset);
    if (!file) {
        std::cerr << "ERR: seekg failed.\n";
        return;
    }
    char buffer[tsize];
    file.read(buffer, tsize);
    userdata = *reinterpret_cast<T *>(buffer);
}
void KVStore::readValStringFromVlog(std::ifstream &file,
                                    std::string &userString,
                                    SS_OFFSET_TL offset, SS_VLEN_TL vlen) {
    file.seekg(offset);
    char *buffer = new char[vlen];
    file.read(buffer, vlen);
    userString = std::string(buffer, vlen);
    delete[] buffer;
}
bool KVStore::crcCheck(const VLOG_CHECKSUM_TL &curChecksum,
                       const KEY_TL &curKey, const SS_VLEN_TL &curVlen,
                       const std::string &curValue) {
    VLOG_CHECKSUM_TL stdChecksum = calcChecksum(curKey, curVlen, curValue);
    return stdChecksum == curChecksum;
}
VLOG_CHECKSUM_TL KVStore::calcChecksum(const KEY_TL &curKey,
                                       const SS_VLEN_TL &curVlen,
                                       const std::string &curValue) {
    std::vector<unsigned char> crcObj;
    fillCrcObj(crcObj, curKey, curVlen, curValue);
    return utils::crc16(crcObj);
}
// void KVStore::deleteSSTInDisknCache(FILE_NUM_TL uid, FILE_NUM_TL level) {
//     // delete in sstCache & sstInfoLevelList
//     sstCache.erase(uid);
//     for (auto it = sstInfoLevelList[level].begin();
//          it != sstInfoLevelList[level].end();) {
//         if (it->uid == uid) {
//             it = sstInfoLevelList[level].erase(it);
//             break;
//         } else {
//             ++it;
//         }
//     }
//     // delete in disk
//     if (utils::rmfile(dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
//                       std::to_string(level) + "/" + std::to_string(uid))) {
//         std::cerr << "ERR: failed to delete a sst during compaction\n";
//     }
//     return;
// }

void KVStore::mergeFilesAndReturnKeyOffsetList(
    std::vector<PtrTrackProps> &ptrTracks,
    std::list<std::pair<KEY_TL, SS_OFFSET_TL>>) {}

void KVStore::writeSSTToDisk(FILE_NUM_TL level,
                             std::vector<sstInfoItemProps> &list) {
    //  写入disk
    for (size_t i = 0; i < list.size(); i++) {
        sstInfoItemProps *curP = &(list[i]);
        std::string targetPath = dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
                                 std::to_string(level),
                    newFileName = std::to_string(curP->uid) + SS_FILE_SUFFIX,
                    newFilePath = targetPath + "/" + newFileName;
        if (!utils::dirExists(targetPath)) utils::_mkdir(targetPath);
        // try to open sstFile
        std::fstream fileCheck(newFilePath);
        if (!fileCheck) {
            // The file does not exist, create a new one.
            std::ofstream newFile(newFilePath);
            if (!newFile) {
                std::cerr << "Failed to create the file: " << newFilePath
                          << "\n";
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
        sstFile.write(reinterpret_cast<const char *>(&(curP->timeStamp)),
                      sizeof(curP->timeStamp));
        sstFile.write(reinterpret_cast<const char *>(&(curP->kvNum)),
                      sizeof(curP->kvNum));
        sstFile.write(reinterpret_cast<const char *>(&(curP->minKey)),
                      sizeof(curP->minKey));
        sstFile.write(reinterpret_cast<const char *>(&(curP->maxKey)),
                      sizeof(curP->maxKey));
        // write bloom filter to SST
        char buffer = 0;
        for (int i = 0; i < DEFAULT_M_VAL; i++) {
            buffer = (buffer << 1) | (curP->bf).bitArray[i];
            if ((i + 1) % 8 == 0) {
                sstFile.write(reinterpret_cast<const char *>(&buffer),
                              sizeof(buffer));
                buffer = 0;
            }
        }

        for (SST_HEADER_KVNUM_TL i = 0; i < curP->kvNum; i++) {
            writeSSTEntry(sstFile, curP->keyList[i], curP->offsetList[i],
                          curP->vlenList[i]);
        }
        sstFile.close();
    }
}
void KVStore::writeSSTToCache(FILE_NUM_TL level,
                              std::vector<sstInfoItemProps> &list) {
    if ((long)levelCache.size() - 1 < (long)level) levelCache.resize(level + 1);
    for (auto &item : list) {
        auto emplaceResult = levelCache[level].emplace(
            std::piecewise_construct, std::forward_as_tuple(item.minKey),
            std::forward_as_tuple(item));
        hashCachePtrByUid[item.uid] = &((*emplaceResult).second);
    }
}
// deprecated
// void KVStore::mergeFilesAndAddToDiskAndCache(
//     std::vector<PtrTrackProps> &ptrTracks, size_t levelToWrite) {
//     size_t trackNum = ptrTracks.size();
//     size_t finishedTrackNum = 0;
//     SS_TIMESTAMP_TL globalTimeStampToWrite = 0;  // 后面无需维护
//     for (auto &trackItem : ptrTracks) {
//         for (auto &sstInfoItem : trackItem.fileInfoList)
//             globalTimeStampToWrite =
//                 std::max(globalTimeStampToWrite, sstInfoItem.timeStamp);
//     }

//     FILE_NUM_TL uidToWrite = ++largestUid;
//     SST_HEADER_KVNUM_TL kvNumToWrite =
//         0;  // 一般是memTableLenThreshold，除了最后一个生成的sst
//     std::vector<KEY_TL> generatedKeyList;
//     std::vector<SS_OFFSET_TL> generatedOffsetList;
//     std::vector<SS_VLEN_TL> generatedVlenList;
//     std::priority_queue<TrackPointerProps, std::vector<TrackPointerProps>,
//                         CompareForPointerQueue>
//         pointerQueue;
//     size_t tmpTrackIndex = -1;
//     for (auto &trackItem : ptrTracks) {
//         tmpTrackIndex++;
//         pointerQueue.push(TrackPointerProps(0, 0,
//         trackItem.fileInfoList[0].uid,
//                                             trackItem.fileInfoList[0].kvNum,
//                                             tmpTrackIndex));
//     }
//     while (finishedTrackNum < trackNum) {
//         TrackPointerProps topPointer = pointerQueue.top();
//         pointerQueue.pop();
//         // 根据topPointer的信息，在SST中尝试新增一项
//         if (kvNumToWrite + 1 <= memTableLenThreshold) {
//             kvNumToWrite++;
//             generatedKeyList.push_back(
//                 sstCache[topPointer.fileUid]
//                     ->keyList[topPointer.CacheItemIndex]);
//             generatedOffsetList.push_back(
//                 sstCache[topPointer.fileUid]
//                     ->offsetList[topPointer.CacheItemIndex]);
//             generatedVlenList.push_back(
//                 sstCache[topPointer.fileUid]
//                     ->vlenList[topPointer.CacheItemIndex]);
//         } else {
//             // sst已满，开始落盘
//             auto minElement = *std::min_element(generatedKeyList.begin(),
//                                                 generatedKeyList.end());
//             auto maxElement = *std::max_element(generatedKeyList.begin(),
//                                                 generatedKeyList.end());
//             if (sstInfoLevelList.size() - 1 < levelToWrite)
//                 sstInfoLevelList.resize(levelToWrite + 1);
//             sstInfoLevelList.at(levelToWrite)
//                 .insert(sstInfoItemProps(uidToWrite, globalTimeStampToWrite,
//                                          minElement, maxElement,
//                                          kvNumToWrite));

//             CacheItemProps *tmpCacheItemPtr = new CacheItemProps();
//             tmpCacheItemPtr->kvNum = kvNumToWrite;
//             tmpCacheItemPtr->level = levelToWrite;
//             for (int ti = 0; ti < kvNumToWrite; ti++)
//                 tmpCacheItemPtr->keyList[ti] = generatedKeyList[ti],
//                 tmpCacheItemPtr->offsetList[ti] = generatedOffsetList[ti],
//                 tmpCacheItemPtr->vlenList[ti] = generatedVlenList[ti];
//             // 生成一个BF

//             sstCache[uidToWrite] = tmpCacheItemPtr;
//         }

//         // update this pointer
//         topPointer.CacheItemIndex++;
//         if (topPointer.CacheItemIndex == topPointer.fileKVNum) {
//             // 尝试跳到该track的下一个文件
//             topPointer.fileIndex++;
//             if (ptrTracks[topPointer.trackIndex].fileInfoList.size() - 1 >=
//                 topPointer.fileIndex) {
//                 topPointer.CacheItemIndex = 0;
//                 topPointer.fileKVNum = sstCache[topPointer.fileUid]->kvNum;
//                 topPointer.fileUid = ptrTracks[topPointer.trackIndex]
//                                          .fileInfoList[topPointer.fileIndex]
//                                          .uid;
//                 pointerQueue.push(topPointer);
//             } else {
//                 // 该track已遍历完成
//                 finishedTrackNum++;
//             }
//         }
//     }
// }

/*
 *debug utils and less important utils
 */
void KVStore::printSST(SST_LEVEL_TL level, FILE_NUM_TL uid) {
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
    std::cout << "entries: \n";
    for (SST_HEADER_KVNUM_TL i = 0; i < headerProps.kvNum; i++) {
        SSTEntryProps entry;
        readEntryFromSST(file, entry, i);
        std::cout << "[" << i << ": " << entry.key << ", " << entry.offset
                  << ", " << entry.vlen << "]";
    }
    std::cout << std::endl;
    std::cout << "----END printSST----\n";
}

void KVStore::printSSTCache(SST_LEVEL_TL level, FILE_NUM_TL uid) {
    sstInfoItemProps *sstInfoItemPtr = hashCachePtrByUid[uid];
    if (sstInfoItemPtr == nullptr) {
        std::cerr << "ERR: CacheItemPtr==nullptr\n";
        return;
    }
    std::cout << "----printCache----\n";

    std::cout << ",kvNum: " << sstInfoItemPtr->kvNum;
    std::cout << "entries: \n";
    for (SST_HEADER_KVNUM_TL i = 0; i < sstInfoItemPtr->kvNum; i++) {
        std::cout << "[" << i << ": " << sstInfoItemPtr->keyList[i] << ", "
                  << sstInfoItemPtr->offsetList[i] << ", "
                  << sstInfoItemPtr->vlenList[i] << "]";
    }
    std::cout << std::endl;
    std::cout << "----END printCache----\n";
}

bool KVStore::checkTailCandidateValidity(int candidate) {
    SS_OFFSET_TL offsetRecord = candidate;
    std::ifstream vlogFile(vlog, std::ios::binary);
    VLOG_MAGIC_TL curMagic;
    readDataFromVlog(vlogFile, curMagic, offsetRecord);
    offsetRecord += VLOG_MAGIC_BYTENUM;
    if (curMagic != VLOG_DEFAULT_MAGIC_VAL) return false;

    VLOG_CHECKSUM_TL curChecksum;
    readDataFromVlog(vlogFile, curChecksum, offsetRecord);
    offsetRecord += VLOG_CHECKSUM_BYTENUM;
    KEY_TL curKey;
    readDataFromVlog(vlogFile, curKey, offsetRecord);
    offsetRecord += SS_KEY_BYTENUM;
    SS_VLEN_TL curVlen;
    readDataFromVlog(vlogFile, curVlen, offsetRecord);
    offsetRecord += SS_VLEN_BYTENUM;
    std::string curValue;
    readValStringFromVlog(vlogFile, curValue, offsetRecord, curVlen);
    offsetRecord += curVlen;

    if (!crcCheck(curChecksum, curKey, curVlen, curValue)) return false;

    vlogFile.close();
    return true;
}

void KVStore::generateSSTList(const std::list<KEY_TL> &keyList,
                              const std::vector<SS_OFFSET_TL> &offsetList,
                              const std::vector<SS_VLEN_TL> &vlenList,
                              std::vector<sstInfoItemProps> &userSSTList,
                              SS_TIMESTAMP_TL timestampToWrite) {
    SST_HEADER_KVNUM_TL totalKVNum = keyList.size();
    // 计算时向上取整
    FILE_NUM_TL totalSSTNum =
        (totalKVNum + MAX_SST_KV_GROUP_NUM - 1) / MAX_SST_KV_GROUP_NUM;
    for (FILE_NUM_TL i = 0; i < totalSSTNum; i++) {
        SST_HEADER_KVNUM_TL headKVIndex = MAX_SST_KV_GROUP_NUM * i,
                            tailKVIndex = MAX_SST_KV_GROUP_NUM * i +
                                          MAX_SST_KV_GROUP_NUM - 1;
        if (tailKVIndex > totalKVNum - 1) tailKVIndex = totalKVNum - 1;
        auto minIter = keyList.begin();
        std::advance(minIter, headKVIndex);
        auto maxIter = minIter;
        std::advance(maxIter, tailKVIndex - headKVIndex + 1);
        userSSTList.emplace_back(
            ++largestUid, timestampToWrite, tailKVIndex - headKVIndex + 1,
            *std::min_element(minIter, maxIter),
            *std::max_element(minIter, maxIter), minIter,
            offsetList.begin() + headKVIndex, vlenList.begin() + headKVIndex);
    }
}

//
// void KVStore::simpleConvertMemTable2File() {
//     // write to vLog
//     std::ofstream vlogFile(vlog, std::ios::binary | std::ios::app);
//     if (!vlogFile.is_open()) {
//         std::cerr << "Failed to open vlogFile: " + vlog + " for writing.\n";
//         return;
//     }
//     std::list<std::pair<KEY_TL, std::string>> list;
//     std::vector<SS_OFFSET_TL> offsetList;
//     KEY_TL minKey, maxKey;  // for writing to SST
//     memTable->scan(0, std::numeric_limits<uint64_t>::max(), list);
//     if (!list.empty()) {
//         minKey = list.front().first;
//         maxKey = list.back().first;
//     } else {
//         std::cerr << "MemTable is empty.\n";
//         return;
//     }
//     for (const auto &item : list) {
//         VLOG_MAGIC_TL magic = 0xff;  // i.e. VLOG_MAGIC_TL
//         SS_VLEN_TL vlen;
//         VLOG_CHECKSUM_TL checksum;
//         if (item.second == DELETE_MARK) {
//             vlen = 0;
//             checksum = calcChecksum(item.first, vlen, "");
//         } else {
//             vlen = item.second.length();
//             checksum = calcChecksum(item.first, vlen, item.second);
//         }
//         // start writing
//         head = vlogFile.tellp();
//         offsetList.push_back(head);
//         if (item.second == DELETE_MARK) {
//             // writeVlogEntry(vlogFile, magic, checksum, item.first, vlen,
//             "");
//         } else {
//             writeVlogEntry(vlogFile, magic, checksum, item.first, vlen,
//                            item.second);
//         }
//     }
//     head = vlogFile.tellp();
//     vlogFile.close();
//     // write to SSTable. TODO: 现在只做了全部新建到0层
//     FILE_NUM_TL targetLevel = 0;
//     // check largestUid
//     FILE_NUM_TL newUid = ++largestUid;
//     std::string targetPath = dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
//                              std::to_string(targetLevel),
//                 newFileName = std::to_string(newUid) + SS_FILE_SUFFIX,
//                 newFilePath = targetPath + "/" + newFileName;
//     if (!utils::dirExists(targetPath)) utils::_mkdir(targetPath);
//     // try to open sstFile
//     std::fstream fileCheck(newFilePath);
//     if (!fileCheck) {
//         // The file does not exist, create a new one.
//         std::ofstream newFile(newFilePath);
//         if (!newFile) {
//             std::cerr << "Failed to create the file: " << newFilePath <<
//             "\n"; return;
//         }
//         newFile.close();
//     }
//     fileCheck.close();
//     // Now, the file is guaranteed to exist. Open it and write.
//     std::ofstream sstFile(newFilePath,
//                           std::ios::app);  // Open the file in append mode.
//     if (!sstFile) {
//         std::cerr << "Failed to open the file: " << newFilePath << "\n";
//         return;
//     }
//     FILE_NUM_TL curTimeStamp = ++largestTimeStamp;
//     SST_HEADER_KVNUM_TL kvNum = list.size();
//     sstFile.write(reinterpret_cast<const char *>(&curTimeStamp),
//                   sizeof(curTimeStamp));
//     sstFile.write(reinterpret_cast<const char *>(&kvNum), sizeof(kvNum));
//     sstFile.write(reinterpret_cast<const char *>(&minKey), sizeof(minKey));
//     sstFile.write(reinterpret_cast<const char *>(&maxKey), sizeof(maxKey));
//     // write bloom filter to SST
//     BF newBF;
//     for (const auto &item : list) newBF.insert(item.first);
//     char buffer = 0;
//     for (int i = 0; i < DEFAULT_M_VAL; i++) {
//         buffer = (buffer << 1) | newBF.bitArray[i];
//         if ((i + 1) % 8 == 0) {
//             sstFile.write(reinterpret_cast<const char *>(&buffer),
//                           sizeof(buffer));
//             buffer = 0;
//         }
//     }

//     int index = -1;
//     for (const auto &item : list) {
//         index++;
//         writeSSTEntry(sstFile, item.first, offsetList[index],
//                       item.second == DELETE_MARK ? 0 : item.second.length());
//     }
//     sstFile.close();
//     // 更新sstInfo
//     if (sstInfoLevelList.size() < targetLevel + 1) {
//         sstInfoLevelList.resize(targetLevel + 1);
//     }
//     sstInfoLevelList[targetLevel].push_back(
//         sstInfoItemProps(newUid, curTimeStamp, minKey, maxKey));

//     // TODO: 从第0层开始处理可能的compaction
//     // 多余，debug用
//     // sstFile.close();
// }