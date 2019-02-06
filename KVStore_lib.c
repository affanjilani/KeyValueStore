#include "KVStore_lib.h"

/**************************************************
  Method to open semaphores and print error if any
***************************************************/
sem_t *openSem(char *sem_name){
	sem_t *sem = sem_open(sem_name, O_RDWR);
	if (sem == SEM_FAILED)
		perror("error opening semaphore\n");
	return sem;
}

/***************************************************
  Method to close semaphores and print error if any
***************************************************/
void closeSem(sem_t *sem_name){
	if(sem_close(sem_name) < 0)
		perror("Unable to close\n");
}

/***********************************************************************
  Find the index at which the info of a key is, return -1 if none there.
***********************************************************************/
int getKeyInfoIndex(char *key, Key_info keyInfo[]){
	for(int i = 0; i<__NUMBER_OF_VALUES__; i++){	//go through each member of the info array

		if(strcmp(keyInfo[i].key, key) == 0)
			return i;								//if we find key info related to key return the index
	}
	return -1;										//if we don't find it then return -1
}

/****************************************************
  Function to write to database. Assumes that it has access to DB.
****************************************************/
int writeToDB(int podIndex, char *key, char *value){
	int shm_fd;
	Database *db;
	struct stat s;

	//Open the shm
	shm_fd = shm_open(__KV_STORE_NAME__, O_RDWR, 0);
	if (shm_fd < 0)
		perror("Error opening the shm");

	if(fstat(shm_fd, &s) == -1)
		perror("Error fstat\n");

	//map shm to database struct
	db = (Database *) mmap(NULL, s.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

	//close file descriptor
	close(shm_fd);

	//Now we write to database
	Pod pod = db -> pods[podIndex];
	
	/* Add the key value to the data array */

	pod.kv_size = pod.kv_size + 1;								//increment size

	/* If we are at max capacity, we must remove the earliest entry and replace it */
	if(pod.kv_size > __NUMBER_OF_VALUES__){						
		KV_pair toRemove = pod.data[pod.kv_head];				//remove the head

		int keyInfoIndex = getKeyInfoIndex(toRemove.key, pod.keys);

		if(keyInfoIndex == -1){
			return -1;
		}

		Key_info keyInfo = pod.keys[keyInfoIndex];

		keyInfo.count = keyInfo.count -1;						//we reduce the number of instances of this key

		if(keyInfo.count == 0){
			memset(keyInfo.key, '\0', sizeof(keyInfo.key));		//set the key to null if no more in store
			keyInfo.lastViewed = -1;							//reinitialize lastViewed
		}
		if(keyInfo.lastViewed == pod.kv_head)
			keyInfo.lastViewed = -1;							//set last viewed to -1 if the last viewed is the one we're removing
																//MAYBE DONT SET IT TO -1

		pod.keys[keyInfoIndex] = keyInfo;						//put key info back in appropriate slot

		pod.kv_head = (pod.kv_head + 1) % __NUMBER_OF_VALUES__;	//advance head to point to next earliest entry
		//Note, if size is max, the tail will be right behind the head, so tail++ will
		//give us the head position before this increment

		pod.kv_size = pod.kv_size -1 ;							//we were at __NUMBER_OF_VALUES__+1 so bring it down 1
	}

	if(pod.kv_size != 1){										//if it's not the first element we add
		pod.kv_tail = (pod.kv_tail + 1) % __NUMBER_OF_VALUES__;	//advance tail to point to where we add the element
	}

	//Add the key value to the store.
	KV_pair keyValue = pod.data[pod.kv_tail];					//get the slot where we must add
	memset(keyValue.key, '\0', sizeof(keyValue.key));			//reinitialize the keys and values at that slot
	memset(keyValue.value, '\0', sizeof(keyValue.value));

	memcpy(keyValue.key, key, strlen(key)+1);					//copy the bytes in key to the pairs key slot
	memcpy(keyValue.value, value, strlen(value)+1);				//copy the bytes in the value to the pair

	/* Key info bookkeeping */
	int newKeyInfoIndex  = getKeyInfoIndex(key, pod.keys);
	if(newKeyInfoIndex == -1){									//IT means that there's no info for this key (new key)
		for(int i = 0; i < __NUMBER_OF_VALUES__; i++){
			if(pod.keys[i].count == 0){
				pod.keys[i].count = pod.keys[i].count + 1;
				memcpy(pod.keys[i].key, key, strlen(key)+1);	//add key's info
				break;
			}
		}
	}

	else{														//if the key was already added before
		pod.keys[newKeyInfoIndex].count = pod.keys[newKeyInfoIndex].count + 1;
	}

	pod.data[pod.kv_tail] = keyValue;							//being pedantic about making sure the latest changes are in the pod
	db -> pods[podIndex] = pod;									//this is also me being pedantic because i don't want no bugs

	
	//unmap virtual memory after using it.
	munmap(db, s.st_size);


	return 0;
}


/************************************************************************************************* 
  Function to read the value associated to a key from the store. Assumes valid key and mapped db. 
   Value passed in is the null string to in which we want to put the value in.
*************************************************************************************************/
int readDB(char *key, Database *db, char *value){
	

	unsigned long hashValue = generate_hash((unsigned char *) key);
	int podIndex = (int)(hashValue % __NUMBER_OF_PODS__);

	Pod pod = db -> pods[podIndex];

	int infoIndex = getKeyInfoIndex(key, pod.keys);
	if(infoIndex == -1)
		return -1;													//If no key found return -1


	int lastViewedValue = pod.keys[infoIndex].lastViewed;			//last viewed value

	if(lastViewedValue == -1 || pod.keys[infoIndex].count > 1){		//if not init or more than one value, find next value
		int nextValue = (lastViewedValue+1)%pod.kv_size;
		while(strcmp(pod.data[nextValue].key,key)!=0)				//while we don't have the next value keep going
			nextValue = (nextValue + 1)%pod.kv_size;

		memcpy(value,pod.data[nextValue].value, __VALUE_SIZE__ );	//copy the value into the value to return
		
		db -> pods[podIndex].keys[infoIndex].lastViewed = nextValue;
		return 0;
	}

	if(pod.keys[infoIndex].count == 1){								//if we only have one value for that key then lastViewed is the only one
		memcpy(value,pod.data[lastViewedValue].value, __VALUE_SIZE__);
		return 0;	
	}

	return 0;
}

/****************************
  Method to initialize store
  Assumes fd is open
*****************************/
int initStore(int shm_fd){
	struct stat s;
	Database *db;

	sem_t *accessSem;	//semaphore that gives access to the data
	sem_t *readerSem;	//semaphore that gives access to the read data

	//Set shared memory to appropriate size, the size being the number of pods * their content size
	
	if( ftruncate(shm_fd, sizeof(*db)) == -1){
		perror("ftruncate");
		return -1;
	}

	if(fstat(shm_fd,&s) == -1)
		perror("error with fstat\n");
	

	db = (Database *) mmap(NULL, sizeof(*db), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);


	//Initialize the values in the different pods	
	for(int i = 0; i< __NUMBER_OF_PODS__; i++){
		//Pod we are configuring
		Pod pod = db->pods[i];

		//set head and tail and size
		pod.kv_size = 0;
		pod.kv_head = 0;
		pod.kv_tail = 0;

		//Initialize the data and key info to null
		for(int val = 0; val<__NUMBER_OF_VALUES__; val++){

			memset(pod.data[val].key, '\0', sizeof(pod.data[val].key));
			memset(pod.data[val].value, '\0', sizeof(pod.data[val].value));

			memset(pod.keys[val].key, '\0', sizeof(pod.keys[val].key));
			pod.keys[val].count = 0;
			pod.keys[val].lastViewed = -1;
		}

		//store pod back into the correct space
		db -> pods[i] = pod;
	}

	//Init the reader count
	db -> readerCount = 0;

	/* Create Semaphores */
	accessSem = sem_open(__KV_ACCESS_SEMAPHORE__, O_CREAT, 0666, 1);
	readerSem = sem_open(__KV_READERS_SEMAPHORE__, O_CREAT, 0666, 1);
	if(accessSem == SEM_FAILED || readerSem == SEM_FAILED)
		perror("Unable to open semaphores");

	/* Close Semaphores */
	if(sem_close(accessSem) < 0)
		perror("Unable to close accessSem");
	if(sem_close(readerSem) < 0)
		perror("Unable to close readerSem");

	//unmap virtual memory after using it.
	munmap(db, sizeof(*db));

	return 0;
}


int kv_store_create(char *kv_store_name){
	struct stat s;
	//Attempt to create the store
	int shm_fd = shm_open(kv_store_name, O_CREAT|O_EXCL|O_RDWR, 0666);

	//if we get an error
	if(shm_fd < 0){
		if(errno != EEXIST)		//if the error was not due to the shm already existing
			return -1;
		else
			return 0;			//if it did exist, it has already been configured so we gucci
	}

	//If there were no errors then we must configure the new shm object
	if( initStore(shm_fd) == -1){
		close(shm_fd);
		return -1;
	}

	//close the shm file descriptor
	close(shm_fd);

	return 0;
}

int kv_store_write(char *potentialKey, char *potentialValue){
	sem_t *accessSem;													//Semaphore for db access

	int keyLength = __KEY_SIZE__;
	int valueLength = __VALUE_SIZE__;
	char key[keyLength];
	char value[valueLength];

	memset(key,'\0',sizeof(key));
	memset(value, '\0', sizeof(value));

	/*******CHECK INPUTS********/

	if(strlen(potentialKey)<keyLength)
		memcpy(key,potentialKey,strlen(potentialKey));
	else
		memcpy(key,potentialKey,keyLength-1);

	if(strlen(potentialValue)<valueLength)
		memcpy(value,potentialValue,strlen(potentialValue));
	else
		memcpy(value,potentialValue,valueLength-1);

	if(key[0] == '\0')													//we do not allow empty keys
		return -1;
	/***************************/
	

	//Get pod index in which key is
	unsigned long hashValue = generate_hash((unsigned char *) key);
	int podIndex = (int)(hashValue % __NUMBER_OF_PODS__);

	/* Entering Critical Section */

	//Open the access semaphore
	accessSem = openSem(__KV_ACCESS_SEMAPHORE__);

	sem_wait(accessSem);				//Wait for access to db
	writeToDB(podIndex, key, value);	//write the key values into the database
	sem_post(accessSem);				//Signal that we done homie

	//Close semaphore after use
	closeSem(accessSem);

	/* Exiting Critical Section */

	return 0;
}

char *kv_store_read(char *potentialKey){
	sem_t *accessSem;											//Access semaphore
	sem_t *readerSem;											//Reader semaphore

	Database *db;
	struct stat s;

	int shm_fd;													//file descriptor for shm

	int valueLength = __VALUE_SIZE__;
	char *value = (char *) calloc(valueLength, sizeof(char));	//Initialize the string
	if(value == NULL)											//Check if calloc was successful
		return NULL;



	int keyLength = __KEY_SIZE__;
	char key[keyLength];

	memset(key,'\0',sizeof(key));								//initialize the key array

	/********CHECK INPUTS********/
	if(strlen(potentialKey)<keyLength)
		memcpy(key,potentialKey,strlen(potentialKey));
	else
		memcpy(key,potentialKey,keyLength-1);

	if(*key == '\0')											//no empty keys!!!!
		return NULL;

	/****************************/

	//Open the store and map it to the database struct
	shm_fd = shm_open(__KV_STORE_NAME__, O_RDWR, 0);

	if(fstat(shm_fd, &s) == -1)
		return NULL;

	db = (Database *) mmap(NULL, s.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);

	readerSem = openSem(__KV_READERS_SEMAPHORE__);		//open the reader's semaphore
	accessSem = openSem(__KV_ACCESS_SEMAPHORE__);		//open the semaphore for acces to store

	/* Entering reader critical section */

	sem_wait(readerSem);								//wait for access to reader's count
	db -> readerCount = db -> readerCount + 1;			//increment reader count
	if(db -> readerCount == 1)
		sem_wait(accessSem);							//if this is the first reader then we ensure access first
	sem_post(readerSem);								//release the reader's semaphore


	int success = readDB(key, db, value);				//check if we successfully found value
	if(success == -1)
		value = NULL;

	sem_wait(readerSem);
	db -> readerCount = (db -> readerCount) - 1;			//decrement reader as it is done
	if(db -> readerCount == 0){
		sem_post(accessSem);							//if this was last reader release access to database
	}
	sem_post(readerSem);								//release reader's semaphore

	
	/* Exiting reader critical section */

	closeSem(readerSem);
	closeSem(accessSem);

	munmap(db,sizeof(*db));
	close(shm_fd);
	
	return value;
}

char **kv_store_read_all(char *potentialKey){
	sem_t * readerSem;											//Reader semaphore
	sem_t * accessSem;											//Access semaphore

	Database *db;
	struct stat s;
	int shm_fd;

	int keyLength =  __KEY_SIZE__;
	char *key = (char *) calloc(keyLength,sizeof(char));
	if(key == NULL)
		return NULL;

	int valueLength = __VALUE_SIZE__;
	char *value = (char *) calloc(valueLength,sizeof(char));	//initialize the string
	if(value == NULL)
		return NULL;

	char **allValues;

	int numberOfValues;

	/********CHECK INPUTS********/
	if(strlen(potentialKey)<keyLength)
		memcpy(key,potentialKey,strlen(potentialKey));
	else
		memcpy(key,potentialKey,keyLength-1);

	if(*key == '\0')										//no empty keys!!!!
		return NULL;
	/****************************/

	//Open the store and map it to the database struct
	shm_fd = shm_open(__KV_STORE_NAME__, O_RDWR, 0);

	if(fstat(shm_fd, &s) == -1)
		return NULL;

	db = (Database *) mmap(NULL, s.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);

	//get the number of values 
	unsigned long hashValue = generate_hash((unsigned char *) key);
	int podIndex = (int)(hashValue % __NUMBER_OF_PODS__);

	Pod pod = db -> pods[podIndex];

	int infoIndex = getKeyInfoIndex(key, pod.keys);					
	if(infoIndex == -1){
		free(key);
		free(value);

		close(shm_fd);
		munmap(db,sizeof(*db));

		return NULL;												//If no key found return null
	}

	numberOfValues = pod.keys[infoIndex].count;						//get count

	//Now calloc the allValues array
	allValues = (char **) calloc(numberOfValues + 1, sizeof(char*));	//store numberOfValues amount of values
	for(int i = 0; i<numberOfValues; i++){
		allValues[i] = (char *) calloc(valueLength + 1, sizeof(char));	//initialize the strings
	}
	


	readerSem = openSem(__KV_READERS_SEMAPHORE__);		//open the reader's semaphore
	accessSem = openSem(__KV_ACCESS_SEMAPHORE__);		//open the semaphore for acces to store

	/* Entering critical section */
	sem_wait(readerSem);								//gain access to the reader sem
	db -> readerCount = db -> readerCount + 1;
	if(db -> readerCount == 1)
		sem_wait(accessSem);							//if first reader then need to gain access
	sem_post(readerSem);

	for(int offset = 0; offset<numberOfValues; offset++){
		int success = readDB(key,db,value);							//get the next value in the *value string
		if(success == -1){
			allValues = NULL;
			break;
		}

		memcpy(allValues[offset],value,__VALUE_SIZE__);				//copy value to appropriate slot

		memset(value,'\0',__VALUE_SIZE__);							//reinitialize value for next read
	}

	sem_wait(readerSem);
	db -> readerCount = (db -> readerCount) - 1;			//decrement reader as it is done
	if(db -> readerCount == 0){
		sem_post(accessSem);							//if this was last reader release access to database
	}
	sem_post(readerSem);								//release reader's semaphore



	/* Exiting critical section */

	//cleanup
	free(value);
	free(key);

	closeSem(accessSem);
	closeSem(readerSem);

	munmap(db, sizeof(*db));
	close(shm_fd);

	return allValues;
}