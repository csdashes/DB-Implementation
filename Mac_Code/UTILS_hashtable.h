// HashTable.h
#ifndef HashTable_h_
#define HashTable_h_

// This class is identical to the isis++ class of the same name,
// except namesapces have been removed.

// ----------------------------------------------------------------------
// Copyright (c) 2002 Simon Perkins <s.perkins@lanl.gov>
//
// Defines common functionality of the HashMap, HashSet and maybe one
// day the HashMultiMap class. Parts of this file are taken directly
// from the SGI/HP STL implementation. Permission is granted to use,
// copy, modify, distribute and sell this software, subject to the
// requirements laid out in the following copyright notices.
// ----------------------------------------------------------------------

/*
* Copyright (c) 1996-1998
* Silicon Graphics Computer Systems, Inc.
*
* Permission to use, copy, modify, distribute and sell this software
* and its documentation for any purpose is hereby granted without fee,
* provided that the above copyright notice appear in all copies and
* that both that copyright notice and this permission notice appear
* in supporting documentation.  Silicon Graphics makes no
* representations about the suitability of this software for any
* purpose.  It is provided "as is" without express or implied warranty.
*
*
* Copyright (c) 1994
* Hewlett-Packard Company
*
* Permission to use, copy, modify, distribute and sell this software
* and its documentation for any purpose is hereby granted without fee,
* provided that the above copyright notice appear in all copies and
* that both that copyright notice and this permission notice appear
* in supporting documentation.  Hewlett-Packard Company makes no
* representations about the suitability of this software for any
* purpose.  It is provided "as is" without express or implied warranty.
*
*/

#include <functional>
#include <utility>
#include <algorithm>
#include <vector>
#include <list>
#include <string>


using namespace std;

// Define the standard hash functions.
// These are based on the standard STL hash functions

inline size_t __hashStringFunc(const char* __s)
{
  unsigned long __h = 0;
  for ( ; *__s; ++__s)
    __h = 5*__h + *__s;

  return size_t(__h);
}


/**
* An STL-compatible hash function that can be used with HashMap's or HashSet's.
*
* This template is defined for all integral and floating point types,
* plus char* and const char* and string.
*/
template <class _Key> struct Hash {};

template<> struct Hash<char*>
{
  size_t operator()(const char* __s) const { return __hashStringFunc(__s); }
};

template<> struct Hash<const char*>
{
  size_t operator()(const char* __s) const { return __hashStringFunc(__s); }
};
template <> struct Hash<string> {
  size_t operator()(const string& __s) const { return __hashStringFunc(__s.c_str()); }
};

template<> struct Hash<char> {
  size_t operator()(char __x) const { return __x; }
};
template<> struct Hash<unsigned char> {
  size_t operator()(unsigned char __x) const { return __x; }
};
template<> struct Hash<signed char> {
  size_t operator()(unsigned char __x) const { return __x; }
};
template<> struct Hash<short> {
  size_t operator()(short __x) const { return __x; }
};
template<> struct Hash<unsigned short> {
  size_t operator()(unsigned short __x) const { return __x; }
};
template<> struct Hash<int> {
  size_t operator()(int __x) const { return __x; }
};
template<> struct Hash<unsigned int> {
  size_t operator()(unsigned int __x) const { return __x; }
};
template<> struct Hash<long> {
  size_t operator()(long __x) const { return __x; }
};
template<> struct Hash<unsigned long> {
  size_t operator()(unsigned long __x) const { return __x; }
};

/**
* @}
*/

// We want the number of buckets to be a prime number
// to spread elements out nicely, but we also want to
// roughly double the size of the hash map each time we resize.
// Hence we need this list of primes.
static const int __hashTablePrimeCount = 28;
static const size_t __hashTablePrimes[__hashTablePrimeCount] =
{
  53ul,         97ul,         193ul,       389ul,       769ul,
    1543ul,       3079ul,       6151ul,      12289ul,     24593ul,
    49157ul,      98317ul,      196613ul,    393241ul,    786433ul,
    1572869ul,    3145739ul,    6291469ul,   12582917ul,  25165843ul,
    50331653ul,   100663319ul,  201326611ul, 402653189ul, 805306457ul,
    1610612741ul, 3221225473ul, 4294967291ul
};


// Get the next prime number from our primes array, equal to or
// greater than n.
static size_t __getNextHashTablePrime(size_t n) {
  const size_t* first = __hashTablePrimes;
  const size_t* last = __hashTablePrimes + __hashTablePrimeCount;
  const size_t* pos = lower_bound(first, last, n);
  return pos == last ? *(last - 1) : *pos;
}


// A generic hash table implementation that is shared by
// HashMap and HashSet.

/**
* This is an implementation of a hash table for use with HashMap and
* HashSet, which in turn are replacements for hash_map and hash_set,
* which for some bizarre reason are not part of the C++ standard and
* hence not supported by some compilers.
*/
template <class Key, class Value, class KeyExtractor, class Hasher, class Equalizer>
class HashTable {

  // Implementation details: we store a vector of buckets, each of
  // which contains a singly linked list of Node objects, which hold
  // the hash table data. This implementation is very similar to that
  // provided in the original STL from SGI and HP. The hashtable is
  // always sized to make the number of buckets a prime number. This
  // reduces the possibility of poor hash functions skipping cells in
  // the table (e.g. imagine a hash function that produces a number
  // that's always a multiple of 5, combined with a table size that
  // has 5 as a factor). Once the average number of elements in each
  // cell begins to rise above a certain fraction, we need to resize
  // the table in order to maintain efficiency. In this
  // implementation, that fraction is 1.0, i.e. the number of elements
  // is always less than or equal to the number of buckets. When we
  // resize we need to increase the table size by a constant factor in
  // order to maintain amortized constant time insertion. So we go to
  // the next prime that is approximately twice as large as the
  // current size.

private:

  // Node is the basic entity used for storing values
  struct Node {

    Value value;
    Node* next;

    Node(const Value& v, Node* n = 0) : value(v), next(n) {}

  };

  // The hash table consists of a vector of pointers to lists of Nodes
  vector<Node*> buckets;

  Hasher hasher;
  Equalizer equalizer;
  KeyExtractor keyExtractor;
  size_t numElements;

  size_t bucketIndex(const Value& value) const {
    return hasher(keyExtractor(value)) % buckets.size();
  }

  size_t bucketIndex(const Value& value, size_t n) const {
    return hasher(keyExtractor(value)) % n;
  }

  size_t bucketIndexKey(const Key& key) const {
    return hasher(key) % buckets.size();
  }

public:

  typedef HashTable<Key, Value, KeyExtractor, Hasher, Equalizer> HashTable_;

  // Iterator definitions

  struct iterator {

    Node* node;
    HashTable_* hashTable;

    iterator() {}

    iterator(Node* n, HashTable_* h) : node(n), hashTable(h) {}

    Value& operator*() const {
      return node->value;
    }

    iterator& operator++() {
      const Node* old = node;
      node = node->next;
      if (!node) {
        size_t bucket = hashTable->bucketIndex(old->value);
        while (!node && ++bucket < hashTable->buckets.size())
          node = hashTable->buckets[bucket];
      }
      return *this;
    }

    iterator& operator++(int) {
      iterator tmp = *this;
      ++*this;
      return tmp;
    }

    bool operator==(const iterator& it) const {
      return node == it.node;
    }

    bool operator!=(const iterator& it) const {
      return node != it.node;
    }

  };

friend struct
  HashTable<Key, Value, KeyExtractor, Hasher, Equalizer>::iterator;
  //  friend iterator;

  struct const_iterator {

    const Node* node;
    const HashTable_* hashTable;

    const_iterator() {}

    const_iterator(const Node* n, const HashTable_* h) : node(n), hashTable(h) {}

    const Value& operator*() const {
      return node->value;
    }

    const_iterator& operator++() {
      const Node* old = node;
      node = node->next;
      if (!node) {
        size_t bucket = hashTable->bucketIndex(old->value);
        while (!node && ++bucket < hashTable->buckets.size())
          node = hashTable->buckets[bucket];
      }
      return *this;
    }

    const_iterator& operator++(int){
      iterator tmp = *this;
      ++*this;
      return tmp;
    }

    bool operator==(const const_iterator& it) const {
      return node == it.node;
    }

    bool operator!=(const const_iterator& it) const {
      return node != it.node;
    }

  };

  //  friend const_iterator;
friend struct
  HashTable<Key, Value, KeyExtractor, Hasher, Equalizer>::const_iterator;
  // Constructors and Destructors

  HashTable(size_t n = 0) {
    initialize(n);
  }

  HashTable(size_t n, const Hasher& hasher_) : hasher(hasher_) {
    initialize(n);
  }

  HashTable(size_t n, const Hasher& hasher_, const Equalizer& equalizer_) : hasher(hasher_), equalizer(equalizer_) {
    initialize(n);
  }

  ~HashTable() {
    clear();
  }

  // Member functions

  iterator end() {
    return iterator(0, this);
  }

  iterator begin() {
    for (size_t i = 0; i < buckets.size(); ++i) {
      if (buckets[i])
        return iterator(buckets[i], this);
    }
    return end();
  }

  const_iterator end() const {
    return const_iterator(0, this);
  }

  const_iterator begin() const {
    for (size_t i = 0; i < buckets.size(); ++i) {
      if (buckets[i])
        return const_iterator(buckets[i], this);
    }
    return end();
  }

  size_t size() const {
    return numElements;
  }

  size_t max_size() const {
    return buckets.size();
  }

  size_t bucket_count() const {
    return buckets.size();
  }

  bool empty() const {
    return numElements == 0;
  }

  Hasher hash_funct() const {
    return hasher;
  }

  Equalizer key_eq() const {
    return equalizer;
  }

  void resize(size_t n) {    // Resize only if bucket count is less than required element count
    size_t oldSize = buckets.size();
    if (n > oldSize) {
      size_t newSize = __getNextHashTablePrime(n);
      // Create a new temporary hash table
      vector<Node*> tmp(newSize, 0);
      for (size_t b = 0; b < oldSize; ++b) {
        Node* node = buckets[b];
        while (node) {
          size_t index = bucketIndex(node->value, newSize);
          buckets[b] = node->next;
          node->next = tmp[index];
          tmp[index] = node;
          node = buckets[b];
        }
      }
      // Now we need to copy the temporary table back into the old one
      buckets.resize(newSize);
      for ( int b = 0; b < newSize; ++b)
        buckets[b] = tmp[b];
    }
  }

  pair<iterator,bool> insert(const Value& value) {
    resize(numElements + 1);
    Key key = keyExtractor(value);
    size_t index = bucketIndexKey(key);
    Node* node = buckets[index];
    while (node) {
      if (equalizer(key, keyExtractor(node->value)))
        return make_pair(iterator(node, this), false);
      node = node->next;
    }
    // If we get here the key was not found
    Node* newNode = new Node(value, buckets[index]);
    ++numElements;
    buckets[index] = newNode;
    return make_pair(iterator(newNode, this), true);
  }

  void clear() {
    for (size_t b = 0; b < buckets.size(); ++b) {
      Node* node = buckets[b];
      while (node) {
        buckets[b] = node->next;
        delete node;
        node = buckets[b];
      }
    }
    numElements = 0;
  }

  iterator find(const Key& key) {
    size_t index = bucketIndexKey(key);
    Node* node = buckets[index];
    while (node) {
      if (equalizer(key, keyExtractor(node->value)))
        return iterator(node, this);
      node = node->next;
    }
    return end();
  }

  const_iterator find(const Key& key) const {
    size_t index = bucketIndexKey(key);
    const Node* node = buckets[index];
    while (node) {
      if (equalizer(key, keyExtractor(node->value)))
        return const_iterator(node, this);
      node = node->next;
    }
    return end();
  }

  size_t count(const Key& key) const {
    return find(key) == end() ? 0 : 1;
  }

  bool erase(iterator pos) {
    Node* targetNode = pos.node;
    if (targetNode) {
      size_t index = bucketIndex(targetNode->value);
      Node* node = buckets[index];

      if (node == targetNode) {
        buckets[index] = node->next;
        delete node;
        --numElements;
      }
      else {
        Node* nextNode = node->next;
        while (nextNode) {
          if (nextNode == targetNode) {
            node->next = nextNode->next;
            delete nextNode;
            --numElements;
            break;
          }
          else {
            node = nextNode;
            nextNode = node->next;
          }
        }
      }
	  return true;
    }
	else
	  return false;
  }

  // Note that this version assumes unique keys and so will only delete one item
  size_t erase(const Key& key) {
    size_t index = bucketIndexKey(key);
    Node* node = buckets[index];
    size_t erased = 0;

    // If bucket is empty then we're not going to erase anything
    if (!node)
      return 0;

    if (equalizer(keyExtractor(node->value), key)) {
      buckets[index] = node->next;
      delete node;
      --numElements;
      ++erased;
    }
    else {
      Node* nextNode = node->next;
      while (nextNode) {
        if (equalizer(keyExtractor(nextNode->value), key)) {
          node->next = nextNode->next;
          delete nextNode;
          --numElements;
          ++erased;
          break;
        }
        else {
          node = nextNode;
          nextNode = node->next;
        }
      }
    }
    return erased;
  }

private:

  void initialize(size_t n) {
    numElements = 0;
    size_t numBuckets = __getNextHashTablePrime(n);
    buckets.resize(numBuckets, 0);
  }

};


#endif