#include "kvstore.h"

#include <string>
using namespace skiplist;
// #define DET
// #define WA
// #define LAST_WA
// #define WATCH
// #define GC_DEBUG

long visitInd = 0;

std::vector<KVStore::PtrTrackProps> KVStore::ptrTracks;
KVStore::KVStore(const std::string &dir, const std::string &vlog)
    :  KVStoreAPI(dir, vlog),dir(dir), vlog(vlog) {
    memTable = new skiplist_type();
    head = 0;
    tail = 0;
    largestUid = 0;
    largestTimeStamp = 0;
    // 根据之前遗留数据，调整head / tail / largestUid / largestTimeStamp
    examineOld();
}
KVStore::~KVStore() {
    if (memTable->getLength()) convertAndWriteMemTable();
    // slippery: 防止内存泄漏
    if (memTable) delete memTable;
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
            // #ifdef DET
            //             std::cout << "memtable is full. Start to convert to
            //                          sst.\n " << " At 'key' = " << key <<
            //                          std::endl;
            // #endif
            convertAndWriteMemTable();
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
    std::string retStr = "";
    getValueOrOffset(key, retStr);
    return retStr;
}

void KVStore::getOffset(uint64_t key, SS_OFFSET_TL *userOffsetPtr) {
    std::string tmpStr = "";
    getValueOrOffset(key, tmpStr, userOffsetPtr);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    std::string res = get(key);
    if (res == "") return false;
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
    ptrTracks.clear();

    head = 0, tail = 0;
    // others
    largestUid = 0, largestTimeStamp = 0;
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
        getOffset(curKey, &sstOffsetRes);
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

void KVStore::getValueOrOffset(uint64_t key, std::string &userStr,
                               SS_OFFSET_TL *userOffsetPtr) {
    std::optional<std::string> res = memTable->get(key);
    if (res.has_value()) {
        userStr = res.value() != DELETE_MARK ? res.value() : "";
        return;
    }
    // #ifdef DET
    //     std::cout << "memtable miss. Go to SST to find " << key << "\n";
    // #endif
    // cannot find in Memtable
    if (enableCache) {
        for (size_t i = 0; i < levelCache.size(); ++i) {
            auto &level = levelCache[i];
            SS_TIMESTAMP_TL maxTimeStamp = 0;
            VALUE_TL ans = "";
            SS_OFFSET_TL offsetAns = CONVENTIONAL_MISS_FLAG_OFFSET;
            for (auto itemIter = level.begin(); itemIter != level.end();
                 ++itemIter) {
                auto &item = *itemIter;
                if (key < item.second.minKey || key > item.second.maxKey)
                    continue;
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
                    } else {
                        if (sstInfoItemPtr->timeStamp > maxTimeStamp) {
                            maxTimeStamp = sstInfoItemPtr->timeStamp;
                            ans = getValueByOffsetnVlen(offsetRes.offset,
                                                        offsetRes.vlen);
                            offsetAns = offsetRes.offset;
                        }
                    }
                }
            }
            if (maxTimeStamp) {
                // found at least one entry
                if (userOffsetPtr != nullptr) {
                    if (offsetAns != CONVENTIONAL_MISS_FLAG_OFFSET)
                        *userOffsetPtr = offsetAns;
                    return;
                } else {
                    userStr = ans;
                    return;
                }
            }
        }
        // not found
        userStr = "";
        return;
    } else {
        // BANNED cache
        std::vector<std::string> nameList;
        int fileNum = utils::scanDir(dir, nameList);
        std::sort(nameList.begin(), nameList.end());
        for (int i = 0; i < fileNum; i++) {
            SS_TIMESTAMP_TL maxTimeStamp = 0;
            VALUE_TL ans = "";
            SS_OFFSET_TL offsetAns = CONVENTIONAL_MISS_FLAG_OFFSET;

            std::string curName = nameList[i];
            int curLevel = 0;
            std::string dirPath = dir + "/" + curName;
            std::size_t pos = curName.find_last_of('-');
            if (pos != std::string::npos) {
                std::string levelStr = curName.substr(pos + 1);
                curLevel = std::stoi(levelStr);
            }
            if (utils::dirExists(dirPath)) {
                // it is a directory
                // examine SST To Cache And LargestVars
                std::vector<std::string> fileNameList;
                int secondFileNum = utils::scanDir(dirPath, fileNameList);
                for (int j = 0; j < secondFileNum; j++) {
                    std::string curFilePath = dirPath + "/" + fileNameList[j];
                    sstInfoItemProps fileItem;
                    readFileNGetFileItem(curFilePath, fileItem);
                    std::string::size_type suffix_pos =
                        fileNameList[j].rfind(SS_FILE_SUFFIX);
                    if (suffix_pos != std::string::npos) {
                        std::string numStr =
                            fileNameList[j].substr(0, suffix_pos);
                        FILE_NUM_TL num =
                            static_cast<FILE_NUM_TL>(std::stoi(numStr));
                        fileItem.uid = num;
                    }
                    // got fileItem
                    SSTEntryProps offsetRes =
                        findOffsetInSSTInfoItemPtr(&fileItem, key);
                    if (offsetRes.offset != CONVENTIONAL_MISS_FLAG_OFFSET) {
                        // found in this SST
                        if (!offsetRes.vlen) {
                            if (fileItem.timeStamp > maxTimeStamp) {
                                maxTimeStamp = fileItem.timeStamp;
                                ans = "";
                                offsetAns = offsetRes.offset;
                            }
                        } else {
                            if (fileItem.timeStamp > maxTimeStamp) {
                                maxTimeStamp = fileItem.timeStamp;
                                ans = getValueByOffsetnVlen(offsetRes.offset,
                                                            offsetRes.vlen);
                                offsetAns = offsetRes.offset;
                            }
                        }
                        if (curLevel) break;
                    }
                }
                if (maxTimeStamp) {
                    // found at least one entry
                    if (userOffsetPtr != nullptr) {
                        if (offsetAns != CONVENTIONAL_MISS_FLAG_OFFSET)
                            *userOffsetPtr = offsetAns;
                        return;
                    } else {
                        userStr = ans;
                        return;
                    }
                }
            }
        }
        // not found
        userStr = "";
        return;
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
        VLOG_MAGIC_TL magic = 0xff;
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
        if (item.second == DELETE_MARK) {
            writeVlogEntry(vlogFile, magic, checksum, item.first, vlen, "");
        } else
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
#ifdef LAST_WA
    if (!std::is_sorted(keyList.begin(), keyList.end())) {
        auto it = std::adjacent_find(keyList.begin(), keyList.end(),
                                     std::greater<KEY_TL>());
        if (it != keyList.end()) {
            std::cerr << "Error: keyList is not sorted. The pair (" << *it
                      << ", " << *(std::next(it)) << ") is out of order.\n";
        }
    }
#endif
    generateSSTList(keyList, offsetList, vlenList, SSTList, ++largestTimeStamp);

    // write to SSTable.
    FILE_NUM_TL targetLevel = 0;
    // 首先写入硬盘
    writeSSTToDisk(targetLevel, SSTList);
    // 接着写入缓存
    // slippery
    writeSSTToCache(targetLevel, SSTList);

    // 判断新增sst后，第0层是否溢出
    while (levelCache[targetLevel].size() >
           MAX_FILE_NUM_GIVEN_LEVEL(targetLevel)) {
        // 从level0合并
        ptrTracks.clear();
        KEY_TL intervalMinKey, intervalMaxKey;
        long MinTrackIndexOfSonLevel;
        if (!targetLevel) {
            // 生成前3条归并track/指针
            MinTrackIndexOfSonLevel = 3;
            std::vector<KEY_TL> tmpMinKeyVector;
            std::vector<KEY_TL> tmpMaxKeyVector;
            for (int i = 0; i < 3; ++i) {
                auto it = levelCache[targetLevel].begin();
                std::advance(it, i);
                std::vector<sstInfoItemProps> tmpVector = {(*it).second};
                tmpMinKeyVector.push_back(tmpVector[0].minKey);
                tmpMaxKeyVector.push_back(tmpVector[0].maxKey);
                ptrTracks.push_back(PtrTrackProps(tmpVector));
            }
            intervalMinKey = *std::min_element(tmpMinKeyVector.begin(),
                                               tmpMinKeyVector.end());
            intervalMaxKey = *std::max_element(tmpMaxKeyVector.begin(),
                                               tmpMaxKeyVector.end());
        } else {
            // 不是第0层，则本层只需生成1条归并track/指针
            MinTrackIndexOfSonLevel = 1;
            std::vector<sstInfoItemProps> reorderVec;
            for (auto &item : levelCache[targetLevel]) {
                reorderVec.push_back(item.second);
            }
            std::sort(reorderVec.begin(), reorderVec.end(), compareReorderVec);
            FILE_NUM_TL fileNumToExpel = levelCache[targetLevel].size() -
                                         MAX_FILE_NUM_GIVEN_LEVEL(targetLevel);
            std::vector<sstInfoItemProps> tmpFileInfoList(
                reorderVec.begin(), reorderVec.begin() + fileNumToExpel);
            // SLIPPERY:
            // tmpFileInfoList是放到track中去的,需要重新按key排序,若不排则为timestamp顺序
            std::sort(tmpFileInfoList.begin(), tmpFileInfoList.end(),
                      [](const sstInfoItemProps &a, const sstInfoItemProps &b) {
                          return a.minKey < b.minKey;
                      });
            // slippery:
            // [前后逻辑贯通]要做下一层的区间覆盖,要保证下一层区间不相交性.
            auto minElement = *std::min_element(
                tmpFileInfoList.begin(), tmpFileInfoList.end(),
                [](const sstInfoItemProps &a, const sstInfoItemProps &b) {
                    return a.minKey < b.minKey;
                });
            intervalMinKey = minElement.minKey;
            auto maxElement = *std::max_element(
                tmpFileInfoList.begin(), tmpFileInfoList.end(),
                [](const sstInfoItemProps &a, const sstInfoItemProps &b) {
                    return a.maxKey < b.maxKey;
                });
            intervalMaxKey = maxElement.maxKey;
            if (!tmpFileInfoList.empty())
                ptrTracks.push_back(PtrTrackProps(tmpFileInfoList));
        }
        // 生成第4条归并track/指针
        FILE_NUM_TL sonLevel = targetLevel + 1;
        if (levelCache.size() - 1 >= sonLevel &&
            !levelCache[sonLevel].empty()) {
            std::vector<sstInfoItemProps> tmpFileInfoList;
            for (auto &sonItem : levelCache[sonLevel]) {
                if (sonItem.second.minKey <= intervalMaxKey &&
                    sonItem.second.maxKey >= intervalMinKey) {
                    tmpFileInfoList.push_back(sonItem.second);
                }
            }
            if (!tmpFileInfoList.empty())
                ptrTracks.push_back(PtrTrackProps(tmpFileInfoList));
        }
        // 开始归并成新文件
        SS_TIMESTAMP_TL maxTimeStampToWrite = 0;
        for (auto &trackItem : ptrTracks) {
            for (auto &sstInfoItem : trackItem.fileInfoList)
                maxTimeStampToWrite =
                    std::max(maxTimeStampToWrite, sstInfoItem.timeStamp);
        }
        std::list<KEY_TL> keyList;
        std::vector<SS_OFFSET_TL> offsetList;
        std::vector<SS_VLEN_TL> vlenList;
        mergeFilesAndReturnKOVList(ptrTracks, keyList, offsetList, vlenList,
                                   targetLevel);
        // ptrTracks已组装完成，删除所有旧文件
        long tmpTrackIndex = -1;
        for (auto &trackItem : ptrTracks) {
            tmpTrackIndex++;
            for (auto &fileInfoItem : trackItem.fileInfoList) {
                tmpTrackIndex < MinTrackIndexOfSonLevel
                    ? deleteSSTInDisknCache(fileInfoItem.uid, targetLevel,
                                            fileInfoItem.minKey)
                    : deleteSSTInDisknCache(fileInfoItem.uid, sonLevel,
                                            fileInfoItem.minKey);
            }
        }
#ifdef LAST_WA
        if (!std::is_sorted(keyList.begin(), keyList.end())) {
            auto it = std::adjacent_find(keyList.begin(), keyList.end(),
                                         std::greater<KEY_TL>());
            if (it != keyList.end()) {
                std::cerr
                    << "Error: [in Merge()] keyList is not sorted. The pair ("
                    << *it << ", " << *(std::next(it))
                    << ") is out of order. visitInd: " << visitInd << "\n";
            }
        }
#endif
        // 将本地的K/O/V list写入disk&cache
        writeKOVListToDiskAndCache(keyList, offsetList, vlenList, sonLevel,
                                   maxTimeStampToWrite);
        // 判断sonLevel的文件数是否达到上限，若达到上限则开启新一轮合并
        targetLevel = sonLevel;
    }
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

void KVStore::readFileNGetFileItem(std::string filePath,
                                   sstInfoItemProps &fileItem) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filePath << std::endl;
        return;
    }
    // read bf
    readBFFromSST(file, fileItem.bf);
    // read kvNum&level
    SSTHeaderProps headerProps;
    readHeaderPropsFromSST(file, headerProps);
    fileItem.kvNum = headerProps.kvNum;
    fileItem.timeStamp = headerProps.timestamp;
    fileItem.minKey = headerProps.minKey;
    fileItem.maxKey = headerProps.maxKey;
    // read 3 lists
    for (SST_HEADER_KVNUM_TL i = 0; i < fileItem.kvNum; i++) {
        SSTEntryProps entry;
        readEntryFromSST(file, entry, i);
        fileItem.keyList[i] = entry.key;
        fileItem.offsetList[i] = entry.offset;
        fileItem.vlenList[i] = entry.vlen;
    }
    file.close();
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
    if (enableBf && !(sstInfoItemPtr->bf.query(key))) return entry;
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
    if (!vlen) {
        userString = DELETE_MARK;
        return;
    }
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
void KVStore::deleteSSTInDisknCache(FILE_NUM_TL uid, FILE_NUM_TL level,
                                    KEY_TL minKey) {
    // delete in levelCache & hashCachePtrByUid
    hashCachePtrByUid.erase(uid);
    for (auto it = levelCache[level].begin(); it != levelCache[level].end();) {
        if (it->first == minKey && it->second.uid == uid) {
            it = levelCache[level].erase(it);
        } else {
            ++it;
        }
    }
    // delete in disk
    if (utils::rmfile(dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
                      std::to_string(level) + "/" + std::to_string(uid) +
                      SS_FILE_SUFFIX)) {
        std::cerr << "ERR: failed to delete "
                  << dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
                         std::to_string(level) + "/" + std::to_string(uid) +
                         SS_FILE_SUFFIX
                  << " during compaction\n";
    }
    return;
}

void KVStore::mergeFilesAndReturnKOVList(std::vector<PtrTrackProps> &ptrTracks,
                                         std::list<KEY_TL> &keyList,
                                         std::vector<SS_OFFSET_TL> &offsetList,
                                         std::vector<SS_VLEN_TL> &vlenList,
                                         FILE_NUM_TL targetLevel) {
    visitInd++;
    std::priority_queue<TrackPointerProps, std::vector<TrackPointerProps>,
                        CompareForPointerQueue>
        pointerQueue;
    for (size_t tmpTrackIndex = 0; tmpTrackIndex < ptrTracks.size();
         tmpTrackIndex++) {
        pointerQueue.push(TrackPointerProps(0, 0, tmpTrackIndex));
    }
    while (!pointerQueue.empty()) {
        TrackPointerProps topPointer = pointerQueue.top();
        pointerQueue.pop();
        sstInfoItemProps *topFilePtr =
            &(ptrTracks[topPointer.trackIndex]
                  .fileInfoList[topPointer.fileIndex]);
        size_t topEntryIndex = topPointer.CacheItemIndex;
        // 使所有可能的key相同的pointer右移
        // slippery: 注意不要漏写右移逻辑
        KEY_TL topKey = topFilePtr->keyList[topEntryIndex];
        while (!pointerQueue.empty() &&
               ptrTracks[pointerQueue.top().trackIndex]
                       .fileInfoList[pointerQueue.top().fileIndex]
                       .keyList[pointerQueue.top().CacheItemIndex] == topKey) {
#ifdef WA
            std::cout << "topTimeStamp: " << topFilePtr->timeStamp
                      << ",expellingTimeStamp: "
                      << ptrTracks[pointerQueue.top().trackIndex]
                             .fileInfoList[pointerQueue.top().fileIndex]
                             .timeStamp
                      << "\n";
#endif
            TrackPointerProps expelledPointer = pointerQueue.top();
            sstInfoItemProps *expelledFilePtr =
                &(ptrTracks[expelledPointer.trackIndex]
                      .fileInfoList[expelledPointer.fileIndex]);
            bool controlFlag = 0;
            pointerQueue.pop();
            // update `expelledPointer`
            expelledPointer.CacheItemIndex++;
            if (expelledPointer.CacheItemIndex == expelledFilePtr->kvNum) {
                // 尝试跳到该track的下一个文件
                expelledPointer.CacheItemIndex = 0;
                expelledPointer.fileIndex++;
                if (ptrTracks[expelledPointer.trackIndex].fileInfoList.size() -
                        1 >=
                    expelledPointer.fileIndex) {
                } else {
                    // 该track已遍历完成，不push
                    controlFlag = 1;
                }
            }
            if (!controlFlag) pointerQueue.push(expelledPointer);
        }
        SS_VLEN_TL vlenToInsert = topFilePtr->vlenList[topEntryIndex];
        bool nextLevelEmpty = levelCache.size() - 1 < targetLevel + 1 ||
                              levelCache[targetLevel + 1].empty();
        // if (!(!vlenToInsert && nextLevelEmpty)) {
        if (vlenToInsert || !nextLevelEmpty) {
            keyList.push_back(topKey);
            offsetList.push_back(topFilePtr->offsetList[topEntryIndex]);
            vlenList.push_back(vlenToInsert);
        }

        // update this pointer
        topPointer.CacheItemIndex++;
        if (topPointer.CacheItemIndex == topFilePtr->kvNum) {
            // 尝试跳到该track的下一个文件
            topPointer.CacheItemIndex = 0;
            topPointer.fileIndex++;
            if (ptrTracks[topPointer.trackIndex].fileInfoList.size() - 1 >=
                topPointer.fileIndex) {
            } else {
                // 该track已遍历完成，不push
                continue;
            }
        }
        pointerQueue.push(topPointer);
    }
    // 优先队列已空，归并结束
}

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
void KVStore::writeKOVListToDiskAndCache(std::list<KEY_TL> &keyList,
                                         std::vector<SS_OFFSET_TL> &offsetList,
                                         std::vector<SS_VLEN_TL> &vlenList,
                                         FILE_NUM_TL sonLevel,
                                         SS_TIMESTAMP_TL maxTimeStampToWrite) {
    // 扩充过短的vector
    if ((long)levelCache.size() - 1 < (long)sonLevel)
        levelCache.resize(sonLevel + 1);

    std::vector<sstInfoItemProps> SSTList;
    generateSSTList(keyList, offsetList, vlenList, SSTList,
                    maxTimeStampToWrite);
    writeSSTToDisk(sonLevel, SSTList);
    writeSSTToCache(sonLevel, SSTList);
}
void KVStore::examineOld() {
    std::vector<std::string> nameList;
    int fileNum = utils::scanDir(dir, nameList);
    for (int i = 0; i < fileNum; i++) {
        std::string curName = nameList[i];
        int curLevel = 0;
        std::string dirPath = dir + "/" + curName;
        std::size_t pos = curName.find_last_of('-');
        if (pos != std::string::npos) {
            std::string levelStr = curName.substr(pos + 1);
            curLevel = std::stoi(levelStr);
        }
        if (utils::dirExists(dirPath)) {
            // it is a directory
            // examine SST To Cache And LargestVars
            std::vector<std::string> fileNameList;
            int secondFileNum = utils::scanDir(dirPath, fileNameList);
            for (int j = 0; j < secondFileNum; j++) {
                std::string curFilePath = dirPath + "/" + fileNameList[j];
                sstInfoItemProps fileItem;
                readFileNGetFileItem(curFilePath, fileItem);
                std::string::size_type suffix_pos =
                    fileNameList[j].rfind(SS_FILE_SUFFIX);
                if (suffix_pos != std::string::npos) {
                    std::string numStr = fileNameList[j].substr(0, suffix_pos);
                    FILE_NUM_TL num =
                        static_cast<FILE_NUM_TL>(std::stoi(numStr));
                    largestUid = std::max(largestUid, num);
                    fileItem.uid = num;
                }

                largestTimeStamp =
                    std::max(largestTimeStamp, fileItem.timeStamp);
                if ((long)levelCache.size() - 1 < (long)curLevel)
                    levelCache.resize(curLevel + 1);
                auto insertResult = levelCache[curLevel].insert(
                    std::make_pair(fileItem.minKey, fileItem));
                hashCachePtrByUid[fileItem.uid] = &insertResult->second;
            }
        } else {
            // it is a file.
            // examine Vlog To Head And Tail
            std::ifstream vlogFile(dirPath, std::ifstream::binary);
            if (!vlogFile.is_open()) {
                // handle error
            }
            head = vlogFile.tellg();
            SS_OFFSET_TL afterHoleOffset =
                utils::seek_data_block(dirPath.c_str());
            SS_OFFSET_TL tailCandidate = afterHoleOffset;
            moveToFirstMagicPos(vlogFile, tailCandidate);
            if (tailCandidate == head) return;
            // tailCandidate
            while (!checkTailCandidateValidity(tailCandidate)) {
                moveToFirstMagicPos(vlogFile, ++tailCandidate);
                if (tailCandidate == head) return;
            }
            tail = tailCandidate;
        }
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty list indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2,
                   std::list<std::pair<uint64_t, std::string>> &list) {
    list.clear();
    if (OPTION_1) {
        const std::vector<SS_OFFSET_TL> tmpOffsetVtr;
        const std::vector<SS_VLEN_TL> tmpVlenVtr;
        const std::list<KEY_TL> tmpMemtableList;
        // memtable 指针
        std::list<std::pair<uint64_t, std::string>> memtableList;
        memTable->scanRange(key1, key2, memtableList);
        if (!memtableList.empty()) {
            std::vector<sstInfoItemProps> memtableVector;
            memtableVector.emplace_back(sstInfoItemProps(
                0, 0, memtableList.size(), key1, key2, tmpMemtableList.begin(),
                tmpOffsetVtr.begin(), tmpVlenVtr.begin()));
            ptrTracks.emplace_back(PtrTrackProps(memtableVector));
        }

        // Level 0 指针
        for (const auto &entry : levelCache[0]) {
            if (entry.second.maxKey >= key1 && entry.second.minKey <= key2) {
                ptrTracks.emplace_back(PtrTrackProps({entry.second}));
            }
        }

        // 其他层指针
        for (FILE_NUM_TL level = 1; level < levelCache.size(); ++level) {
            std::vector<sstInfoItemProps> trackFiles;
            for (const auto &entry : levelCache[level]) {
                if (entry.second.maxKey >= key1 &&
                    entry.second.minKey <= key2) {
                    trackFiles.push_back(entry.second);
                }
            }
            if (!trackFiles.empty()) {
                ptrTracks.emplace_back(PtrTrackProps(trackFiles));
            }
        }

        // 多路归并
        std::priority_queue<TrackPointerProps, std::vector<TrackPointerProps>,
                            CompareForPointerQueue>
            pointerQueue;
        for (size_t i = 0; i < ptrTracks.size(); ++i) {
            if (!ptrTracks[i].fileInfoList.empty()) {
                pointerQueue.emplace(TrackPointerProps(0, 0, i));
            }
        }

        while (!pointerQueue.empty()) {
            TrackPointerProps topPointer = pointerQueue.top();
            pointerQueue.pop();

            const sstInfoItemProps *topFilePtr =
                &ptrTracks[topPointer.trackIndex]
                     .fileInfoList[topPointer.fileIndex];
            size_t topEntryIndex = topPointer.CacheItemIndex;

            KEY_TL topKey = topFilePtr->keyList[topEntryIndex];
            if (topKey >= key1 && topKey <= key2) {
                SS_VLEN_TL vlen = topFilePtr->vlenList[topEntryIndex];
                if (vlen > 0) {
                    std::string value;
                    std::ifstream vlogFile(vlog, std::ios::binary);
                    readValStringFromVlog(vlogFile, value,
                                          topFilePtr->offsetList[topEntryIndex],
                                          vlen);
                    list.emplace_back(topKey, value);
                } else {
                    list.emplace_back(topKey, DELETE_MARK);
                }
            }

            // 更新指针
            topPointer.CacheItemIndex++;
            if (topPointer.CacheItemIndex == topFilePtr->kvNum) {
                topPointer.CacheItemIndex = 0;
                topPointer.fileIndex++;
            }
            if (topPointer.fileIndex <
                ptrTracks[topPointer.trackIndex].fileInfoList.size()) {
                pointerQueue.push(topPointer);
            }
        }
    } else {
        for (KEY_TL i = key1; i <= key2; i++)
            list.push_back(std::make_pair(i, get(i)));
    }
}

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
        auto tmpIter = keyList.begin();
        std::advance(tmpIter, headKVIndex);
        for (SST_HEADER_KVNUM_TL ind = headKVIndex; ind <= tailKVIndex; ind++) {
            // #ifdef WATCH
            if (WATCHED_KEY && *tmpIter == WATCHED_KEY) {
                std::cout << "WATCHED_KEY in sstuid: " << largestUid + 1
                          << " offset: " << offsetList[ind]
                          << " timestamp: " << timestampToWrite << " \n";
            }
            // #endif
            std::advance(tmpIter, 1);
        }
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

void KVStore::printUidContainsWatchedKey() {
    for (size_t i = 0; i < levelCache.size(); ++i) {
        auto &level = levelCache[i];
        for (auto itemIter = level.begin(); itemIter != level.end();
             ++itemIter) {
            auto &item = *itemIter;
            for (SST_HEADER_KVNUM_TL ii = 0; ii < item.second.kvNum; ii++)
                if (item.second.keyList[ii] == WATCHED_GC_KEY) {
                    std::cout << "SNAPSHOT: key in " << item.second.uid
                              << " at level: " << i << "\n";
                    for (SST_HEADER_KVNUM_TL zz = 0; zz < item.second.kvNum;
                         zz++) {
                        if (item.second.keyList[zz] == WATCHED_GC_KEY) {
                            std::cout << "[" << item.second.keyList[zz] << ", "
                                      << item.second.offsetList[zz] << ", "
                                      << item.second.vlenList[zz] << "]";
                        }
                    }
                }
        }
    }
}
void KVStore::lookInMemtable(KEY_TL key) {
    auto res = memTable->get(key);
    if (res.has_value())
        std::cout << "spot in memtable: " << res.value() << "\n";
    else
        std::cout << "NOT spot in memtable\n";
}
