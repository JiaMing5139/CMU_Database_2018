#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include "queue"

namespace cmudb {
/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size):
            buckets_(2), bucket_size(size) {

    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return std::hash<K>()(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        return buckets_[bucket_id]->depth;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return 0;
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        std::lock_guard<std::mutex> lockGuard(lock);
        size_t hash = HashKey(key);
        size_t buck_id = getTopNBinary(hash, globalDepth);
        if (buck_id >= buckets_.size()) return false;
        if (!buckets_[buck_id]) return false;
        auto it = buckets_[buck_id]->items.find(key);
        if (it == buckets_[buck_id]->items.end()) return false;
        else
            value = it->second;
        return true;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        std::lock_guard<std::mutex> lockGuard(lock);
        size_t hash = HashKey(key);
        size_t buck_id = getTopNBinary(hash, globalDepth);
        if (buck_id >= buckets_.size()) return false;
        if (!buckets_[buck_id]) return false;
        auto it = buckets_[buck_id]->items.find(key);
        if (it == buckets_[buck_id]->items.end()) return false;
        else
            buckets_[buck_id]->items.erase(it);
        return true;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        std::lock_guard<std::mutex> lockGuard(lock);
        hash_t hash = std::hash<K>()(key);
        hash_t bucketId = getTopNBinary(hash, globalDepth);


        std::shared_ptr<Bucket<K, V>> tBucket;
        {
            // 保护bucket
            if (!buckets_[bucketId]) {
                buckets_[bucketId] = std::make_shared<Bucket<K, V>>(globalDepth, bucketId);
            }
            tBucket = buckets_[bucketId];
        }
        tBucket->insert(key, value);
//        pair_count++;

        std::queue<std::shared_ptr<Bucket<K, V>>> needSplit;
        needSplit.push(tBucket);

        while (!needSplit.empty()) {
            auto targetBucket = needSplit.front();
            needSplit.pop();
            if (targetBucket->items.size() > bucket_size) {
                //split
                assert(globalDepth >= targetBucket->depth);
                if (globalDepth > targetBucket->depth) {
                    // don't need double bucketlist
                    splitBucket(buckets_,targetBucket);
                    needSplit.push(targetBucket);
                } else {
                    //globalDepth == loacldepth double
                    size_t size = buckets_.size() * 2;
                    std::vector<std::shared_ptr<Bucket<K, V>>> tmp(size);
                    for (size_t i = 0; i < tmp.size(); ++i) {
                        size_t buckId = getTopNBinary(i, globalDepth);
                        tmp[i] = buckets_[buckId];
                    }
                    splitBucket(tmp,targetBucket);
                    needSplit.push(targetBucket);
                    globalDepth++;
                    buckets_.swap(tmp);
                }
            }
        }
    }

    template<typename K, typename V>
    typename ExtendibleHash<K, V>::hash_t
    ExtendibleHash<K, V>::getTopNBinary(ExtendibleHash::hash_t hash, ExtendibleHash::hash_t n) {
        return hash & ((1 << n) - 1);
    }

    template<typename K, typename V>
    void ExtendibleHash<K, V>::splitBucket( std::vector<std::shared_ptr<Bucket<K, V>>>& buckets,std::shared_ptr<Bucket<K,V>> targetbucket) {
        Bucket<K,V> copy = *targetbucket;
        for (auto item : copy.items) {
            size_t tmpKey = HashKey(item.first);
            size_t bId = getTopNBinary(tmpKey, targetbucket->depth + 1);
            if (buckets[bId]->id != bId) {
                //remove from previous bucket
                targetbucket->items.erase(item.first);
                //add a new bucket
                std::shared_ptr<Bucket<K, V>> buck;
                buck.reset(new Bucket<K, V>(targetbucket->depth + 1, bId));
                buckets[bId] = buck;

                buckets[bId]->items.insert(item);
            } else {
                targetbucket->items.erase(item.first);
                buckets[bId]->items.insert(item);
            }
        }
        targetbucket->depth++;
    }

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb
