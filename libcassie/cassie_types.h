/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_TYPES_H
#define __LIBCASSIE_TYPES_H

#ifdef __cplusplus
namespace libcassie {

	extern "C" {
#endif

		typedef struct _cassie * cassie_t;

		typedef enum {
			CASSIE_CONSISTENCY_LEVEL_ZERO          =  0,
			CASSIE_CONSISTENCY_LEVEL_ONE           =  1,
			CASSIE_CONSISTENCY_LEVEL_QUORUM        =  2,
			CASSIE_CONSISTENCY_LEVEL_DCQUORUM      =  3,
			CASSIE_CONSISTENCY_LEVEL_DCQUORUMSYNC  =  4,
			CASSIE_CONSISTENCY_LEVEL_ALL           =  5,
			CASSIE_CONSISTENCY_LEVEL_ANY           =  6
		} cassie_consistency_level_t;

		/* Represents a column.  Used as input/output to the blob insert/get functions */
		typedef struct _cassie_column {
			char * name;
			size_t name_len;
			char * value;
			size_t value_len;
			int64_t timestamp;
		} * cassie_column_t;

#ifdef __cplusplus
	}
}
#endif

#endif