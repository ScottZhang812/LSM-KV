#include "kvstore.h"

#include <string>
using namespace skiplist;

KVStore::KVStore(const std::string &dir, const std::string &vlog)
    : KVStoreAPI(dir, vlog), dir(dir), vlog(vlog) {
    memTable = new skiplist_type();
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
    std::ofstream file(vlog, std::ios::binary | std::ios::out);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for writing.\n";
        return;
    }
    std::list<std::pair<uint64_t, std::string>> list;
    memTable->scan(0, std::numeric_limits<uint64_t>::max(), list);
    for (const auto &item : list) {
        char byte = 0xff;
        file.write(&byte, sizeof(byte));  // magic

        std::vector<unsigned char> crcObj;
        uint32_t vlen = item.second.length();
        fillCrcObj(crcObj, item.first, vlen, item.second);

        uint16_t checksum = utils::crc16(crcObj);
        // checksum

        const char *datap = reinterpret_cast<const char *>(&item.first);
        file.write(datap, sizeof(uint64_t));  // key
        datap = reinterpret_cast<const char *>(&vlen);
        file.write(datap, sizeof(vlen));  // vlen
        datap = item.second.data();
        file.write(datap, vlen);  // value
    }

    file.close();
    // write to SSTable
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