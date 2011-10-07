/*
 * LibCassandra
 * Copyright (C) 2010-2011 Padraig O'Sullivan, Ewen Cheslack-Postava
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#ifndef __LIBCASSANDRA_UTIL_PLATFORM_H
#define __LIBCASSANDRA_UTIL_PLATFORM_H

#ifdef __GNUC__

// Disables boost::fusion version of tuple. This is for Sirikata compatibility,
// you'll need a similar setup if you do your own inclusion of boost headers.
#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 3) || __GNUC__ > 4
#define BOOST_HAS_GCC_TR1

// #include_next is broken: it does not search default include paths!
#define BOOST_TR1_DISABLE_INCLUDE_NEXT
// config_all.hpp reads this variable, then sets BOOST_HAS_INCLUDE_NEXT anyway
#include <boost/tr1/detail/config_all.hpp>
#ifdef BOOST_HAS_INCLUDE_NEXT
// This behavior has existed since boost 1.34, unlikely to change.
#undef BOOST_HAS_INCLUDE_NEXT
#endif

#endif
#endif

#include <boost/tr1/memory.hpp>
#include <boost/tr1/tuple.hpp>
#include <boost/tr1/unordered_map.hpp>

#include <string>
#include <vector>
#include <map>
#include <set>

#endif //__LIBCASSANDRA_UTIL_PLATFORM_H
