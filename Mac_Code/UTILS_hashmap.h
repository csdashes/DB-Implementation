// HashMap.h
#ifndef HashMap_h_
#define HashMap_h_

// This class is identical to the isis++ class of the same name,
// except namesapces have been removed.

// ----------------------------------------------------------------------
// HashMap is a C++ implementation of a hashtable-based associative
// array, designed to be a single-file drop-in replacement for the STL
// hash_map class. This very useful hash_map class did not make it
// into the C++ standard, leaving us in a situation where some
// compilers support it (e.g gcc), and some don't (e.g. Visual C++).
// The HashMap API is identical to STL hash_map, although at the time
// of writing not all the STL methods have been implemented. However
// most of the more common methods are available, and others will be
// added as requested. HashMap also provides a
// templated hash function similar to the STL's hash<>, called Hash<>.
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// NOTE: HashSet shares some functionality with HashMap through the
// class HashTable.
//----------------------------------------------------------------------

// ----------------------------------------------------------------------
// Copyright (c) 2002 Simon Perkins <s.perkins@lanl.gov>
//
// Permission is granted to use, copy, modify, distribute and sell
// this software, without fee, as long as this copyright notice is
// retained in the file, along with any other copyright notices in
// other associated files.
// ----------------------------------------------------------------------

#include "UTILS_hashtable.h"


/**
* This is an implementation of an STL-like hash_map, which for some
* bizarre reason is not part of the C++ standard and hence not
* supported by some compilers. It is implemented in terms of a
* generic has table, which is implemented in the HashTable class.
* That class in turn is heavily based on SGI's STL implementation.
*/
template <class Key, class Value, class Hasher = Hash<Key>, class Equalizer = equal_to<Key> >
class HashMap
{
public:
	  
	// Types
	typedef Hasher hasher;
	typedef Equalizer equal_key;
	typedef Key key_type;
	typedef Value data_type;
	typedef pair<const Key,Value> value_type;
	typedef size_t size_type;

	// A key extraction object
	struct SelectFirst {
	  const key_type& operator()(const value_type& value) const {
		  return value.first;
	  }
	};

private:

	// Most implementation is in terms of a HashTable
    typedef HashTable<key_type, value_type, SelectFirst, hasher, equal_key> HT;

    // The HashTable
    HT ht;

public:

      // Iterator class is just an interator into the elements list
    typedef typename HT::iterator iterator;
    typedef typename HT::const_iterator const_iterator;

      // Constructors

    HashMap() {}

    HashMap(size_type n) : ht(n) {}

    HashMap(size_type n, const Hasher& hasher_) : ht(n, hasher_) {}

    HashMap(size_type n, const Hasher& hasher_, const Equalizer& equalizer_) : ht(n, hasher_, equalizer_) {}

    // Member functions

    iterator begin() {
      return ht.begin();
    }

    iterator end() {
      return ht.end();
    }

    const_iterator begin() const {
      return ht.begin();
    }

    const_iterator end() const {
      return ht.end();
    }

    size_type size() const {
      return ht.size();
    }

    size_type max_size() const {
      return ht.max_size();
    }

    size_type bucket_count() const {
      return ht.bucket_count();
    }

    bool empty() const {
      return ht.empty();
    }

    hasher hash_funct() const {
      return ht.hash_funct();
    }

    equal_key key_eq() const {
      return ht.key_eq();
    }

    void resize(size_type n) {
      ht.resize(n);
    }

    pair<iterator,bool> insert(const value_type& x) {
      return ht.insert(x);
    }

    data_type& operator[](const key_type& k) {
      return (*(insert(value_type(k, data_type())).first)).second;
    }

    void clear() {
      ht.clear();
    }

    iterator find(const key_type& k) {
      return ht.find(k);
    }

    const_iterator find(const key_type& k) const {
      return ht.find(k);
    }

    size_type count(const key_type& k) const {
      return ht.count(k);
    }

    bool erase(iterator pos) {
      return ht.erase(pos);
    }

    bool erase(const key_type& k) {
      return( ht.erase(k) > 0 );
    }
};


// #ifndef HashMap_h_
#endif
