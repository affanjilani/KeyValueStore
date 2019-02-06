#ifndef __A2_LIB_HEADER__
#define __A2_LIB_HEADER__

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>

/* -------------------------------------
	Define your own globals here
---------------------------------------- */

#define __KEY_VALUE_STORE_SIZE__	50 * 1024*1024        			
#define __NUMBER_OF_PODS__			128
#define __NUMBER_OF_VALUES__		256
#define __VALUE_SIZE__ 				256
#define __KEY_SIZE__ 				32
#define __KV_ACCESS_SEMAPHORE__		"ACCESS_260742032"
#define __KV_READERS_SEMAPHORE__	"READER_260742032" 
#define __KV_STORE_NAME__			"/KV_STORE_260742032"
#define KV_EXIT_SUCCESS				0
#define KV_EXIT_FAILURE				-1

/* ---------------------------------------- */

/*
  Struct to store key value pair
*/
struct KV_pair{
    char key[__KEY_SIZE__];       //Key can be max 32 chars
    char value[__VALUE_SIZE__];    //Value can be max 256 chars
};

/*
  Struct to store info about a key
*/
struct Key_info{
    char key[__KEY_SIZE__];       //Key about which we are storing info
    int  count;         //Amount of these we have (1 if only one)
    int  lastViewed;    //Last viewed value for this key
};

/*
  Struct for the pods in memory
*/
struct Pod{
	
    /* Stores the key values associated to this pod */
    struct KV_pair data[__NUMBER_OF_VALUES__];

    /* ------------------------------------------------------------------------------
    Bookeeping of the data array
    ------------------------------------------------------------------------------*/
    int kv_size;       //number of key value pairs in data
    int kv_head;       //the earliest element added
    int kv_tail;       //last element added

    /*Stores global info about all of the keys that hash to this pod*/
    struct Key_info keys[__NUMBER_OF_VALUES__];

};

struct Database{
	int readerCount;
	struct Pod pods[__NUMBER_OF_PODS__];
};

typedef struct KV_pair 	KV_pair;
typedef struct Key_info Key_info;
typedef struct Pod 		Pod;
typedef struct Database Database;

unsigned long generate_hash(unsigned char *str);

int kv_store_create(char *kv_store_name);
int kv_store_write(char *key, char *value);
char *kv_store_read(char *key);
char **kv_store_read_all(char *key);

#endif
