/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include "hash/hash_table.h"
#include <map>
#include <mutex>

namespace cmudb {
template <typename K,typename V>
struct Bucket{
    Bucket( int depth,int id) :depth(depth),id(id){ }
    Bucket(const Bucket &) = default;
    Bucket() = default;
    void insert(const K & key  , const V & value){
        items[key] = value;
    }
    std::map<K, V> items;
    int depth = 0;
    size_t id = 0;
};

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
    typedef size_t hash_t ;
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  void splitBucket( std::vector<std::shared_ptr<Bucket<K, V>>>& buckets,std::shared_ptr<Bucket<K,V>> bucket);

private:

  // add your own member variables here
  inline  hash_t getTopNBinary(hash_t hash,hash_t n);
    int globalDepth = 1;
    std::vector<std::shared_ptr<Bucket<K,V>>> buckets_;
    std::mutex lock;
    size_t  bucket_size;
};
} // namespace cmudb
