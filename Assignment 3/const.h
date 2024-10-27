#ifndef CONST_H
#define CONST_H

// module wide constants

#define PAGE_SIZE 4096

// Schema Stringify delimiter
#define DELIMITER ((char *) ",")

// Per table buffer size
#define PER_TBL_BUF_SIZE 10

// Page header length
#define PAGE_HEADER_LEN 11

// bytes to represent number of slots in page
#define BYTES_SLOTS_COUNT 2

// Table header length
#define TABLE_HEADER_PAGES_LEN 2

// Number of bits in byte
#define NUM_BITS 8

//size of long 
#define HEXADECIMAL_BASE 16

// DB path configuration
#define PATH_DIR "/tmp/database_ado/"
#define DEFAULT_MODE 0777

#endif
