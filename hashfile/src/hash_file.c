#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

typedef struct info{
  char * fileName;
  // int indexBlocks;
  int file_desc;
}info;

typedef struct key{
  int value;
  bool isEmpty;
  int blockNumb;
  // BF_Block * pointer;
}key;

static info * array[MAX_OPEN_FILES];

HT_ErrorCode HT_Init() {
  for(int i = 0; i < MAX_OPEN_FILES; i++){
    array[i] = NULL;
  }
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  int fileDesc;
  BF_Block * block;
  char * data;
  BF_Block_Init(&block);
  CALL_BF(BF_CreateFile(filename));
  CALL_BF(BF_OpenFile(filename,&fileDesc));
  CALL_BF(BF_AllocateBlock(fileDesc, block));

  data = BF_Block_GetData(block);
  int * desc;
  desc = (int *)data;
  *desc = 777;
  *(desc+1) = buckets;
  *(desc + 2) = buckets / (BF_BLOCK_SIZE / sizeof(key));
  if( (buckets % (BF_BLOCK_SIZE / sizeof(key))) != 0 )
    (*(desc + 2)) += 1;

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_AllocateBlock(fileDesc, block));
  data = BF_Block_GetData(block);

  int space;
  key * ptr;
  ptr = (key *)data;
  space = BF_BLOCK_SIZE / sizeof(key);
  int y = 0;
  for(int i = 0; i < buckets; i++){
    ptr->value = i;
    ptr->isEmpty = true;
    // ptr->pointer = NULL;
    ptr++;
    y++;
    if( (y == space) && (i < buckets - 1) ){
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      CALL_BF(BF_AllocateBlock(fileDesc, block));
      ptr = (key *)BF_Block_GetData(block);
      y = 0;
    }
  }


  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(fileDesc));
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
	int fileDesc ,i;
	CALL_BF(BF_OpenFile(fileName, &fileDesc));

  for(i = 0; i < MAX_OPEN_FILES; i++){
    if(array[i] == NULL){
      if((array[i] = malloc( sizeof(info) )) == NULL) return HT_ERROR;
      if((array[i]->fileName = malloc(strlen(fileName) + 1)) == NULL) return HT_ERROR;
      strcpy(array[i]->fileName, fileName);
      // CALL_BF(BF_GetBlockCounter(fileDesc, &array[i]->indexBlocks));
      array[i]->file_desc = fileDesc;
      *indexDesc = i;
      break;
    }
  }
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  CALL_BF(BF_CloseFile(array[indexDesc]->file_desc));
  free(array[indexDesc]->fileName);
  free(array[indexDesc]);
  array[indexDesc] = NULL;
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  BF_Block * block, * bucket;
  char * data;
  int mod, keyOfRecord, keysPerBlock, blockOfKey, positionInBlock, i;
  int * temp, * positionOfNext, * recordsInBlock;
  key * ptr;
  Record * rec;
  bool * isFull;

  BF_Block_Init(&block);

  //get the mod that is going to be used for hashing
  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, 0, block));
  data = BF_Block_GetData(block);
  temp = (int *)data;
  mod = *(temp + 1);
  //********//

  CALL_BF(BF_UnpinBlock(block));
  keyOfRecord = record.id % mod; //hashing

  //get the index block of the key and the position of the key in that block
  keysPerBlock = BF_BLOCK_SIZE / sizeof(key);
  blockOfKey = keyOfRecord / keysPerBlock;
  positionInBlock = keyOfRecord % keysPerBlock;
  //********//

  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, blockOfKey + 1, block));
  ptr = (key *)BF_Block_GetData(block);
  ptr += positionInBlock;

  BF_Block_Init(&bucket);
  if(ptr->isEmpty) //this is the first record to be ever inserted in the bucket
  {
    CALL_BF(BF_AllocateBlock(array[indexDesc]->file_desc, bucket));
    CALL_BF(BF_GetBlockCounter(array[indexDesc]->file_desc, &ptr->blockNumb));
    ptr->blockNumb -= 1;
    ptr->isEmpty = false;

    data = BF_Block_GetData(bucket);
    isFull = (bool *)data;
    *isFull = false;
    data += 1;
    positionOfNext = (int *)data;
    *positionOfNext = 0;
    data += 4;
    recordsInBlock = (int *)data;
    *recordsInBlock = 1;
    data += 4;

    rec = (Record *)data;
    memcpy(rec->name, record.name , strlen(record.name) +1);
    memcpy(rec->surname, record.surname , strlen(record.surname) +1);
    memcpy(rec->city, record.city , strlen(record.city) +1);
    rec->id = record.id;
  }
  else
  {
    CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, ptr->blockNumb, bucket));
    data = BF_Block_GetData(bucket);
    isFull = (bool *)data;
    data += 1;
    positionOfNext = (int *)data;
    data += 4;
    recordsInBlock = (int *)data;
    data += 4;

    while( (*isFull) && (*positionOfNext != 0) )
    {
      int x;
      x = *positionOfNext;
      CALL_BF(BF_UnpinBlock(bucket));
      CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, x, bucket));
      data = BF_Block_GetData(bucket);
      isFull = (bool *)data;
      data += 1;
      positionOfNext = (int *)data;
      data += 4;
      recordsInBlock = (int *)data;
      data += 4;
    }

    if( !(*isFull) )
    {
      int maxRecsInBlock;
      i = 0;
      rec = (Record *)data;
      maxRecsInBlock = BF_BLOCK_SIZE / sizeof(Record);
      while (i < maxRecsInBlock)
        {
          if(strcmp(rec->name,"") == 0)
          {
            memcpy(rec->name, record.name , strlen(record.name) +1);
            memcpy(rec->surname, record.surname , strlen(record.surname) +1);
            memcpy(rec->city, record.city , strlen(record.city) +1);
            rec->id = record.id;
            (*recordsInBlock) += 1;
            break;
          }
          rec += 1;
          i++;
        }
      if(*recordsInBlock == maxRecsInBlock)
        *isFull = true;
    }
    else
    {
      BF_Block * nextInChain;

      BF_Block_Init(&nextInChain);
      CALL_BF(BF_AllocateBlock(array[indexDesc]->file_desc, nextInChain));
      CALL_BF(BF_GetBlockCounter(array[indexDesc]->file_desc, positionOfNext));
      (*positionOfNext) -= 1;

      data = BF_Block_GetData(nextInChain);
      isFull = (bool *)data;
      *isFull = false;
      data += 1;
      positionOfNext = (int *)data;
      *positionOfNext = 0;
      data += 4;
      recordsInBlock = (int *)data;
      *recordsInBlock = 1;
      data += 4;

      rec = (Record *)data;
      memcpy(rec->name, record.name , strlen(record.name) +1);
      memcpy(rec->surname, record.surname , strlen(record.surname) +1);
      memcpy(rec->city, record.city , strlen(record.city) +1);
      rec->id = record.id;

      BF_Block_SetDirty(nextInChain);
      CALL_BF(BF_UnpinBlock(nextInChain));
      BF_Block_Destroy(&nextInChain);
    }

  }

  BF_Block_SetDirty(bucket);
  CALL_BF(BF_UnpinBlock(bucket));
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&bucket);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  BF_Block * block, * bucket;
  int * temp;
  char * data;
  key * ptr;
  Record * rec;
  int * positionOfNext;
  int maxRecsInBlock, x;

  BF_Block_Init(&block);
  BF_Block_Init(&bucket);


  maxRecsInBlock = BF_BLOCK_SIZE / sizeof(Record);
  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, 0, block));
  data = BF_Block_GetData(block);
  temp = (int *)data;

  if(id == NULL) //print everything
  {
    int indexBlocks, keysLeftToVisit, recordsPrinted, maxKeysInBlock;

    keysLeftToVisit = *(temp + 1);
    indexBlocks = *(temp + 2);

    maxKeysInBlock = BF_BLOCK_SIZE / sizeof(key);
    recordsPrinted = 0;

    CALL_BF(BF_UnpinBlock(block));

    //get every indexBlock
    for(int i = 1;i <= indexBlocks; ++i)
    {
      CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, i, block));
      data = BF_Block_GetData(block);
      ptr = (key *)data;
      //get every key in indexBlock
      for(int y = 0; y < maxKeysInBlock; ++y)
      {
        if(keysLeftToVisit == 0)
          break;

        printf("--------------------------------------------------------\n");
        printf("$$$$$$$$$$$$$$$  Printing for key = %d  $$$$$$$$$$$$$$$$\n\n",ptr->value);
        //print out the whole bucket of every key
        CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, ptr->blockNumb, bucket));
        data = BF_Block_GetData(bucket);
        data += 1;
        positionOfNext = (int *)data;
        data += 8;
        rec = (Record *)data;

        while (*positionOfNext != 0) {
          for (int z = 0; z < maxRecsInBlock; z++) {
            if(strcmp(rec->name,"") != 0) //a record exists in this spot
            {
              printf("Id: %d|Name: %s|Surname: %s|City: %s\n",rec->id,rec->name,rec->surname,rec->city);
              recordsPrinted++;
            }

            rec++;
          }

          x = *positionOfNext;
          CALL_BF(BF_UnpinBlock(bucket));
          CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, x, bucket));
          data = BF_Block_GetData(bucket);
          data += 1;
          positionOfNext = (int *)data;
          data += 8;
          rec = (Record *)data;

        }

        for (int z = 0; z < maxRecsInBlock; z++) {
          if(strcmp(rec->name,"") != 0) //a record exists in this spot
          {
            printf("Id: %d|Name: %s|Surname: %s|City: %s\n",rec->id,rec->name,rec->surname,rec->city);
            recordsPrinted++;
          }
          rec++;
        }
        //********************//

        printf("\n$$$$$$$$$$$$$$$ Printing Finished for key = %d  $$$$$$$$$$$$$$$$\n\n",ptr->value);
        CALL_BF(BF_UnpinBlock(bucket));
        keysLeftToVisit--;
        ptr++;
      }

      CALL_BF(BF_UnpinBlock(block));
    }

    printf("*HT_PrintAllEntries* Records Printed: %d\n\n",recordsPrinted);
  }
  else
  {
    int mod, keysPerBlock, keyOfRecord, blockOfKey, positionInBlock;
    bool foundRecord;

    foundRecord = false;
    mod = *(temp + 1);

    CALL_BF(BF_UnpinBlock(block));
    keyOfRecord = (*id) % mod; //hashing

    //get the index block of the key and the position of the key in that block
    keysPerBlock = BF_BLOCK_SIZE / sizeof(key);
    blockOfKey = keyOfRecord / keysPerBlock;
    positionInBlock = keyOfRecord % keysPerBlock;
    //********//

    CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, blockOfKey + 1, block));
    ptr = (key *)BF_Block_GetData(block);
    ptr += positionInBlock;

    CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, ptr->blockNumb, bucket));
    data = BF_Block_GetData(bucket);
    data += 1;
    positionOfNext = (int *)data;
    data += 8;
    rec = (Record *)data;

    while (*positionOfNext != 0) {
      for (int z = 0; z < maxRecsInBlock; z++) {
        if(rec->id == *id) //a record exists in this spot
        {
          printf("\nRecord with id: %d was found!\n",*id);
          printf("Id: %d|Name: %s|Surname: %s|City: %s\n\n",rec->id,rec->name,rec->surname,rec->city);
          foundRecord = true;
          break;
        }

        rec++;
      }

      if(foundRecord)
        break;

      x = *positionOfNext;
      CALL_BF(BF_UnpinBlock(bucket));
      CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, x, bucket));
      data = BF_Block_GetData(bucket);
      data += 1;
      positionOfNext = (int *)data;
      data += 8;
      rec = (Record *)data;

    }

    for (int z = 0; z < maxRecsInBlock; z++) {
      if(rec->id == *id) //a record exists in this spot
      {
        printf("\nRecord with id: %d was found!\n",*id);
        printf("Id: %d|Name: %s|Surname: %s|City: %s\n\n",rec->id,rec->name,rec->surname,rec->city);
        foundRecord = true;
        break;
      }
      rec++;
    }

    if(!foundRecord)
      printf("\nRecord with id: %d is not in HASH FILE\n\n",*id);

    CALL_BF(BF_UnpinBlock(bucket));
    CALL_BF(BF_UnpinBlock(block));
  }


  BF_Block_Destroy(&bucket);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  BF_Block * block, * bucket;
  int * temp, * recordsInBlock;
  char * data;
  key * ptr;
  Record * rec;
  int * positionOfNext;
  int maxRecsInBlock, x;
  bool * isFull;

  BF_Block_Init(&block);
  BF_Block_Init(&bucket);

  maxRecsInBlock = BF_BLOCK_SIZE / sizeof(Record);
  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, 0, block));
  data = BF_Block_GetData(block);
  temp = (int *)data;

  int mod, keysPerBlock, keyOfRecord, blockOfKey, positionInBlock;
  bool foundRecord;

  foundRecord = false;
  mod = *(temp + 1);

  CALL_BF(BF_UnpinBlock(block));
  keyOfRecord = (id) % mod; //hashing

  //get the index block of the key and the position of the key in that block
  keysPerBlock = BF_BLOCK_SIZE / sizeof(key);
  blockOfKey = keyOfRecord / keysPerBlock;
  positionInBlock = keyOfRecord % keysPerBlock;
  //********//

  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, blockOfKey + 1, block));
  ptr = (key *)BF_Block_GetData(block);
  ptr += positionInBlock;

  CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, ptr->blockNumb, bucket));
  data = BF_Block_GetData(bucket);
  isFull = (bool *)data;
  data += 1;
  positionOfNext = (int *)data;
  data += 4;
  recordsInBlock = (int *)data;
  data += 4;
  rec = (Record *)data;

  while (*positionOfNext != 0) {
    for (int z = 0; z < maxRecsInBlock; z++) {
      if(rec->id == id) //a record exists in this spot
      {
        printf("\nRecord with id: %d was found!,Deleting It.\n\n",id);

        memcpy(rec->name, "" , strlen("") + 1);
        memcpy(rec->surname, "" , strlen("") + 1);
        memcpy(rec->city, "" , strlen("") + 1);
        rec->id = 0;

        (*recordsInBlock) -= 1;
        *isFull = false;
        foundRecord = true;
        break;
      }

      rec++;
    }

    if(foundRecord)
      break;

    x = *positionOfNext;
    CALL_BF(BF_UnpinBlock(bucket));
    CALL_BF(BF_GetBlock(array[indexDesc]->file_desc, x, bucket));
    data = BF_Block_GetData(bucket);
    data += 1;
    positionOfNext = (int *)data;
    data += 8;
    rec = (Record *)data;

  }

  for (int z = 0; z < maxRecsInBlock; z++) {
    if(rec->id == id) //a record exists in this spot
    {
      printf("\nRecord with id: %d was found!,Deleting It.\n\n",id);

      memcpy(rec->name, "" , strlen("") + 1);
      memcpy(rec->surname, "" , strlen("") + 1);
      memcpy(rec->city, "" , strlen("") + 1);
      rec->id = 0;

      (*recordsInBlock) -= 1;
      *isFull = false;
      foundRecord = true;
      break;
    }
    rec++;
  }

  if(!foundRecord)
    printf("\nRecord with id: %d is not in HASH FILE\n\n",id);

  CALL_BF(BF_UnpinBlock(bucket));
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&bucket);
  BF_Block_Destroy(&block);

  return HT_OK;
}