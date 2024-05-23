#include "kvstore.h"

#include <string>
using namespace skiplist;

KVStore::KVStore(const std::string &dir, const std::string &vlog)
    : KVStoreAPI(dir, vlog) {
    memTable = new skiplist_type();

    // TODO: 设置head/tail的值. Belows are temps
    head = 0;
    tail = 0;
    levelLargestUidList.clear();
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
    std::ofstream vlogFile(vlog, std::ios::binary | std::ios::out);
    if (!vlogFile.is_open()) {
        std::cerr << "Failed to open vlogFile for writing.\n";
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
        std::vector<unsigned char> crcObj;
        SS_VLEN_TL vlen = item.second.length();
        fillCrcObj(crcObj, item.first, vlen, item.second);
        VLOG_CHECKSUM_TL checksum = utils::crc16(crcObj);  // checksum
        // start writing
        offsetList.push_back(head);
        writeVlogEntry(vlogFile, magic, checksum, item.first, vlen,
                       item.second);
    }
    vlogFile.close();
    // write to SSTable. TODO: 现在只做了全部新建到0层
    FILE_NUM_TL targetLevel = 0;
    // check largestUid
    if (levelLargestUidList.size() <= targetLevel) {
        while (levelLargestUidList.size() <= targetLevel)
            levelLargestUidList.push_back(-1);
    }
    FILE_NUM_TL uidInLevel = ++(levelLargestUidList[targetLevel]);
    std::string targetPath = dir + SS_DIR_PATH_SUFFIX_WITHOUT_LEVELNUM +
                             std::to_string(targetLevel),
                newFileName = std::to_string(uidInLevel),
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
    sstFile << curTimeStamp;
    sstFile << kvNum;
    sstFile << minKey;
    sstFile << maxKey;

    // write bloom filter to SST
    BF newBF;
    for (const auto &item : list) newBF.insert(item.first);
    char buffer = 0;
    for (int i = 0; i < DEFAULT_M_VAL; i++) {
        buffer = (buffer << 1) | newBF.bitArray[i];
        if ((i + 1) % 8 == 0) {
            sstFile << buffer;
            buffer = 0;
        }
    }

    int index = -1;
    for (const auto &item : list) {
        index++;
        writeSSTEntry(sstFile, item.first, offsetList[index],
                      item.second.length());
    }
    sstFile.close();

    //TODO: 从第0层开始处理可能的compaction
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
    return (res.has_value() && res.value() != "~DELETED~") ? res.value() : "";
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
    memTable->put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    delete memTable;
    memTable = new skiplist_type();
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
    vlogFile.seekp(head);
    const char *datap = reinterpret_cast<const char *>(&magic);
    vlogFile.write(datap, sizeof(magic));
    const char *datap = reinterpret_cast<const char *>(&checksum);
    vlogFile.write(datap, sizeof(checksum));
    const char *datap = reinterpret_cast<const char *>(&key);
    vlogFile.write(datap, sizeof(key));
    datap = reinterpret_cast<const char *>(&vlen);
    vlogFile.write(datap, sizeof(vlen));
    datap = val.data();
    vlogFile.write(datap, vlen);
}
void KVStore::writeSSTEntry(std::ofstream &sstFile, const KEY_TL &key,
                            const SS_OFFSET_TL &offset,
                            const SS_VLEN_TL &vlen) {
    sstFile << key;
    sstFile << offset;
    sstFile << vlen;
}
