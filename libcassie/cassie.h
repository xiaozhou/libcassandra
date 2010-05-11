/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_H
#define __LIBCASSIE_H

#ifdef __cplusplus
namespace libcassie {
	extern "C" {
#endif

		/* -----------------------------------------------------
		 * COMMON TYPES
		 * -----------------------------------------------------
		 */

		typedef enum {
			CASSIE_CONSISTENCY_LEVEL_ZERO          =  0,
			CASSIE_CONSISTENCY_LEVEL_ONE           =  1,
			CASSIE_CONSISTENCY_LEVEL_QUORUM        =  2,
			CASSIE_CONSISTENCY_LEVEL_DCQUORUM      =  3,
			CASSIE_CONSISTENCY_LEVEL_DCQUORUMSYNC  =  4,
			CASSIE_CONSISTENCY_LEVEL_ALL           =  5,
			CASSIE_CONSISTENCY_LEVEL_ANY           =  6
		} cassie_consistency_level_t;

		typedef struct _cassie * cassie_t;

		typedef struct _cassie_blob {
			char * data;
			size_t length;
		} * cassie_blob_t;

		typedef struct _cassie_column {
			cassie_blob_t name;
			cassie_blob_t value;
			int64_t timestamp;
		} * cassie_column_t;

		typedef struct _cassie_super_column {
			cassie_blob_t 		name;
			cassie_column_t	* columns;
			unsigned int 		num_columns;
		} * cassie_super_column_t;

		/* -----------------------------------------------------
		 * In cassie.cc
		 * -----------------------------------------------------
		 */

		/* Initializes a new cassie object
		 * Use cassie_free when done with it
		 */
		cassie_t cassie_init(const char * host, int port);

		/* Frees a cassie object initialied with cassie_init */
		void cassie_free(cassie_t cassie);

		/* Self-explanatory */
		char * cassie_last_error(cassie_t cassie);
		void cassie_print_debug(cassie_t cassie);

		/* -----------------------------------------------------
		 * In cassie_blob.cc
		 * -----------------------------------------------------
		 */

		/* Allows in-line conversion of C string to cassie_blob_t */
#define CASSIE_CTOB(cstr) (cstr == NULL ? (&(struct _cassie_blob){NULL, 0}) : (&(struct _cassie_blob){(cstr), strlen(cstr)}))

		/* Returns the underlying data of the blob
		 * Notice that if you treat this as a C string, it will be safe but may be LOSSY:
		 * If the blob already contains a NULL character, your use of it as a C-string will be terminated at that point.
		 * If the blob does not contain a NULL, cassie_blob_init will ensure there's a NULL sitting at its end, so reading it like a C string
		 * will not read past allocation.
		 */
#define CASSIE_BDATA(blob) (blob->data)

		/* Returns the underlying length of the blob
		 */
#define CASSIE_BLENGTH(blob) (blob->length)

		/* Initializes a blob
		 * Use cassie_blob_free when you're done with it
		 */
		cassie_blob_t cassie_blob_init(
				const char * data,
				size_t length
				);

		/* Free a blob that was initialized with cassie_blob_init */
		void cassie_blob_free(cassie_blob_t blob);


		/* -----------------
		 * In cassie_column.cc
		 * -----------------
		 */

		/* Free a column that was initialized by cassie_column_init */
		void cassie_column_free(cassie_column_t column);

		/* -----------------
		 * In cassie_io_column.cc
		 * -----------------
		 */

		/* Insert a column into cassandra
		 * The column will be inserted inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be inserted inside that column_family+super_column_name
		 */
		int cassie_insert_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level
				);

		/* Retrieves a column from cassandra
		 * The column will be retrieved from inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be retrieved from that column_family+super_column_name
		 * Call cassie_column_free() when you're done with it
		 */
		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				);

		/* Similar to cassie_get_column, but will return the columns value as a C String
		 * free() the returned value when you no longer need it
		 * See the notes on the CASSIE_B2C macro regarding lossiness of converting from blob->C string
		 */
		char * cassie_get_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				);

		/* -----------------
		 * In cassie_io_super_column.cc
		 * -----------------
		 */

		/* Retrieves an entire super column from cassandra
		 * Call cassie_super_column_free() when you're done with it
		 */
		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				);

		/* -----------------
		 * In cassie_super_column.cc
		 * -----------------
		 */

		/* Frees a super column, typically returned with cassie_get_super_column */
		void cassie_super_column_free(cassie_super_column_t supercol);

#ifdef __cplusplus
	}
}
#endif

#endif
