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
#include "mutex"

namespace cmudb {
template <typename K,typename V>
struct Bucket{
    Bucket( int depth,int id) :depth(depth),id(id){ }
    Bucket(const Bucket &) = default;
    void insert(const K & key  , const V & value){
        items[key] = value;
    }
    Bucket() = default;;
    std::map<K, V> items;          // key-value pairs

    int depth = 0;                 // local depth counter
    size_t id = 0;                 // id of Bucket
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
  inline  hash_t getTopNBinary(hash_t hash,hash_t n);
  // add your own member variables here

    int globalDepth = 1;
    std::vector<std::shared_ptr<Bucket<K,V>>> buckets_;
    std::mutex lock;
    int pair_count = 0;
    size_t  bucket_size;
};
} // namespace cmudb
