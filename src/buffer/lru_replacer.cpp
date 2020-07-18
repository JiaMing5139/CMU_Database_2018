/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer()
{}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
       auto it = hashTable.find(value);
        if(it!= hashTable.end()){
            replacable_.splice(replacable_.begin(),replacable_,hashTable[value]);
            hashTable[value] = replacable_.begin();
            return;
        }
        replacable_.push_front(value);
        hashTable[value] = replacable_.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    if(replacable_.empty()) return false;
    value = replacable_.back();
    hashTable.erase(value);
    replacable_.pop_back();
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        auto it = hashTable.find(value);
        if(it == hashTable.end()) return false;
         hashTable.erase(value);
         replacable_.remove(value);
         return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return replacable_.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
