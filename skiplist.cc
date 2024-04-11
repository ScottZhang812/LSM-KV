#include "skiplist.h"

#include <optional>

namespace skiplist {
int skiplist_type::randomLevel() {
    int lv = 0;
    // use bitwise operation to accelerate float comparison
    // while ((random() & 0xFFFF) < (0xFFFF * p))
    //     ++lv;
    while ((double(rand()) / RAND_MAX) < p) ++lv;

    // std::cout << "ramdomLevel gens: " << std::min(MAXLV, lv) << std::endl;
    return std::min(MAXLV, lv);
}
skiplist_type::skiplist_type(double p) {
    srand(time(0));
    this->p = p;
    length = 0;
    level = 0;
    head = new SkipListNode(0, "");
    for (int i = 0; i <= MAXLV; i++) head->forward[i] = nullptr;
}
skiplist_type::~skiplist_type() {
    SkipListNode *current = head;
    while (current) {
        SkipListNode *node_to_delete = current;
        current = current->forward[0];
        delete node_to_delete;
    }
}
void skiplist_type::put(key_type key, const value_type &val) {
    SkipListNode *update[MAXLV + 1];
    SkipListNode *p = head;
    for (int i = level; i >= 0; i--) {
        while (p->forward[i] && p->forward[i]->key < key) p = p->forward[i];
        update[i] = p;
    }
    p = p->forward[0];
    if (p && p->key == key) {
        p->value = val;
        return;
    }

    int lv = randomLevel();
    if (lv > level) {
        lv = ++level;
        update[lv] = head;
    }
    try {
        SkipListNode *newNode = new SkipListNode(key, val);
        for (int i = 0; i <= lv; i++) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }
    } catch (const std::bad_alloc &e) {
        std::cerr << "Memory allocation failed: " << e.what() << '\n';
        std::exit(EXIT_FAILURE);  // Exit the program
    }
    ++length;
}
std::optional<value_type> skiplist_type::get(key_type key) const {
    SkipListNode *p = head;
    for (int i = level; i >= 0; i--) {
        while (p->forward[i] && p->forward[i]->key < key) {
            p = p->forward[i];
        }
        if (p->forward[i] && p->forward[i]->key == key) {
            return p->forward[i]->value;
        }
    }
    return std::nullopt;
}
void skiplist_type::scan(uint64_t key1, uint64_t key2,
                         std::list<std::pair<uint64_t, std::string>> &list) {
    SkipListNode *p = head;
    SkipListNode *res = nullptr;
    for (int i = level; i >= 0; i--) {
        while (p->forward[i] && p->forward[i]->key < key1) {
            p = p->forward[i];
        }
        if ((p->forward[i] && p->forward[i]->key == key1) || i == 0) {
            res = p->forward[i];
            break;
        }
    }
    while (res && res->key <= key2) {
        list.emplace_back(res->key, res->value);
        res = res->forward[0];
    }
}

}  // namespace skiplist
