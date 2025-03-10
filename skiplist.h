#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
#include <optional>
// #include <vector>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <list>
#include <string>

namespace skiplist {
using key_type = uint64_t;
// using value_type = std::vector<char>;
using value_type = std::string;
const int MAXLV = 32;

struct SkipListNode {
    key_type key;
    value_type value;
    SkipListNode *forward[MAXLV + 1];
    SkipListNode(key_type k, value_type v) : key(k), value(v), forward{} {}
};
class skiplist_type {
    // add something here
   private:
    SkipListNode *head = nullptr;
    int level, length = 0;
    double p;

   public:
    int getLength() { return length; }
    int randomLevel();
    explicit skiplist_type(double p = 0.25);
    ~skiplist_type();
    void put(key_type key, const value_type &val);
    std::optional<value_type> get(key_type key) const;
    void scan(uint64_t key1, uint64_t key2,
              std::list<std::pair<uint64_t, std::string>> &list);
    // std::string get(key_type key) const;
    void scanRange(uint64_t key1, uint64_t key2,
                   std::list<std::pair<uint64_t, std::string>> &list) {
        SkipListNode *x = head;
        for (int i = level; i >= 0; i--) {
            while (x->forward[i]->key < key1) {
                x = x->forward[i];
            }
            SkipListNode *y = x->forward[i];
            while (y->key >= key1 && y->key <= key2) {
                list.emplace_back(y->key, y->value);
                y = y->forward[i];
            }
        }
    };
};

}  // namespace skiplist

#endif  // SKIPLIST_H
