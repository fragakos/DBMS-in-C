//
// Created by theodore on 15/12/19.
//

#include "functions.h"

extern int AM_errno;

bool wrongInput(const char attrType,const int attrLength) {
  if((attrType != 'i') && (attrType != 'c') && (attrType != 'f'))
    return true;

  if(((attrType == 'i') || (attrType == 'f')) && attrLength != 4)
    return true;

  if(attrLength < 1 || attrLength > 255)
    return true;

  return false;
}

bool nameExists(const char * const fileName) { // check if the file is open at the moment
  for(int i = 0; i < MAXOPENFILES; ++i) {
    if(openFilesTable[i].fileName != NULL \
     && strcmp(fileName,openFilesTable[i].fileName) == 0)
      return true;
  }
  return false;
}

bool tableIsFull() {
  if(openFiles == MAXOPENFILES)
    return true;

  return false;
}

bool scanTableIsFull() {
  if(scanningFiles == MAXSCANS)
    return true;

  return false;
}

int setAndReturn(const int value) {
  AM_errno = value;
  return value;
}

bool beingScanned(const int fileDesc) { // check if the file is being scanned at the moment
  for(int i = 0; i < MAXSCANS; ++i) {
    if(scanTable[i].isUsed && fileDesc == scanTable[i].fileDesc)
      return true;
  }

  return false;
}

int maxIndexPointers(const int fileIndex) { // returns the amount of (pointer,key) pairs that a block can fit
  int size = openFilesTable[fileIndex].sizeOfKey;

  return (BF_BLOCK_SIZE - sizeof(int) - sizeof(char) + size)\
   / (sizeof(int) + size);
}

int maxDataRecords(const int fileIndex) { // returns the amount of records that a block can fit
  int sizeOfKey = openFilesTable[fileIndex].sizeOfKey;
  int sizeOfSecField = openFilesTable[fileIndex].sizeOfSecField;

  return (BF_BLOCK_SIZE - 2 * sizeof(int) - sizeof(char))\
   / (sizeOfKey + sizeOfSecField);
}

int makeDataBlock(int nextBlock, BF_Block *block, int fileIndex) { // creates a new data block and returns its number
  int blockKey;
  char identifier = 'D';
  char * data;
  int offset = 0;
  int * temp;

  CALL_BF(BF_AllocateBlock(openFilesTable[fileIndex].fileDesc,block));
  CALL_BF(BF_GetBlockCounter(openFilesTable[fileIndex].fileDesc,&blockKey));
  blockKey -= 1;

  data = BF_Block_GetData(block);
  memcpy(data,&identifier,sizeof(char));
  offset += sizeof(char);

  temp = (int *)(data + offset);
  *temp = 0; //initialize the counter of records
  offset += sizeof(int);

  temp = (int *)(data + offset);
  *temp = nextBlock; //set pointer to next data block

  return blockKey;
}

void insertRecord(BF_Block *block, int fileIndex,\
 void *value1, void *value2) { // insert a record at the end of the data block
  char *data;
  int *counter;

  data = BF_Block_GetData(block);
  data += sizeof(char);
  counter = (int *)data; // get current count of records in the block
  data += 2 * sizeof(int);
  data = getToEnd(data, *counter, openFilesTable[fileIndex].sizeOfKey,\
   openFilesTable[fileIndex].sizeOfSecField); // move the pointer to the free space of the block
  memcpy(data, value1, openFilesTable[fileIndex].sizeOfKey);
  data += openFilesTable[fileIndex].sizeOfKey;
  memcpy(data, value2, openFilesTable[fileIndex].sizeOfSecField);
  *counter += 1;
}

char *getToEnd(char *data,int counter, int size1, int size2) {
  //returns a pointer that points to the free space of the block
  data += counter * (size1 + size2);
  return data;
}

int makeIndexBlock(BF_Block *block, int fileIndex) { // creates a new index block and returns its number
  int blockKey;
  char identifier = 'I';
  char * data;
  int offset = 0;
  int * temp;

  CALL_BF(BF_AllocateBlock(openFilesTable[fileIndex].fileDesc,block));
  CALL_BF(BF_GetBlockCounter(openFilesTable[fileIndex].fileDesc,&blockKey));
  blockKey -= 1;

  data = BF_Block_GetData(block);
  memcpy(data,&identifier,sizeof(char));
  offset += sizeof(char);

  temp = (int *)(data + offset);
  *temp = 0; //initialize the counter of pointers

  return blockKey;
}

void insertPointer(BF_Block *block, int fileIndex,int value) { // insert a pointer at the end of the index block
  char *data;
  int *counter;

  data = BF_Block_GetData(block);
  data += sizeof(char);
  counter = (int *)data; // get current count of pointers
  data += sizeof(int);
  data = getToEnd(data, *counter, sizeof(int),\
   openFilesTable[fileIndex].sizeOfKey);
  int * temp;
  temp = (int *)data;
  *temp = value;
  *counter += 1;
}

void insertKey(BF_Block *block, int fileIndex,void *value) { // insert a key at the end of the index block
  char *data;
  int *counter;

  data = BF_Block_GetData(block);
  data += sizeof(char);
  counter = (int *)data; // get current count of pointers
  data += 2 * sizeof(int); // skip the first pointer
  data = getToEnd(data, *counter -1,\
   openFilesTable[fileIndex].sizeOfKey, sizeof(int)); // reach the end of the block
  memcpy(data, value, openFilesTable[fileIndex].sizeOfKey);
}

void updateFileTable(char * fileName, int newRoot) {
  // updates all the entries of the open files table that contain information about the same file
  for (int i = 0; i < MAXOPENFILES; i++) {
    if(openFilesTable[i].fileName != NULL &&\
    strcmp(openFilesTable[i].fileName,fileName) == 0) {
      openFilesTable[i].rootIndex = newRoot;
      openFilesTable[i].depth += 1;
    }
  }

}

int updateMetadata(int fileDesc, int newRoot) {
  // updates the root of the file and increases the depth by one
  BF_Block * block;
  char *data;
  int offset;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(fileDesc, 0, block));
  data = BF_Block_GetData(block);
  offset = strlen("B+") + 1 + 2 * (sizeof(char) + sizeof(int));
  int * temp;
  temp = (int *)(data + offset); // now points at the root block number
  *temp = newRoot;
  offset += sizeof(int);
  temp = (int *)(data + offset); // now points at the depth of the index levels
  *temp += 1;

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return AME_OK;
}

bool blockIsFull(BF_Block *block,int fileIndex, char blockType) {
  // returns true if a block(index or data) is full, false otherwise
  char *data;
  data = BF_Block_GetData(block);
  data += sizeof(char);
  int * count = (int *)data; // get the counter of the blocks records
  if(blockType == 'D') {
    if(*count == maxDataRecords(fileIndex))
      return true;
  } else if(blockType == 'I'){
    if(*count == maxIndexPointers(fileIndex))
      return true;
  }
  return false;
}

int sortBlock(BF_Block *block,int fileIndex, char blockType) { // returns appropriate error code
  // place the last record of the block in the correct position and move all remaining records to the right
  char *data;
  int offset;
  void *v1;
  int retval;
  int size1, size2;
  data = BF_Block_GetData(block);
  offset = sizeof(char);
  int count;
  count = *(int *)(data + offset); // get the count of records in the block(data or index)
  size1 = openFilesTable[fileIndex].sizeOfKey;
  if(blockType == 'I') {
    count -= 1;
    size2 = sizeof(int);
  }
  else
    size2 = openFilesTable[fileIndex].sizeOfSecField;

  offset += 2 * sizeof(int); // skip counter and next block pointer(if data block) or first pointer(if index block)
  offset += (count - 1) * (size1 + size2); // set offset to the last record

  if((v1 = malloc(size1 + size2)) == NULL) // allocate space for the records that will be swapped out
    return setAndReturn(AME_MEM_ERR);

  memcpy(v1, &data[offset],size1 + size2);
  data += sizeof(char) + 2 * sizeof(int);

  for (int i = 0; i < (count - 1); ++i) {
    offset = i * (size1 + size2);

    if(cmpValues((char *)v1,data + offset,fileIndex) < 0) {
      retval = swapValues((char *)v1,data + offset, blockType, fileIndex);
      if(retval != AME_OK)
        return retval;
    }
  }
  offset = (count - 1) * (size1 + size2);
  retval = swapValues((char *)v1,data + offset, blockType, fileIndex); // place the biggest key record to the end
  if(retval != AME_OK)
    return retval;

  free(v1);

  return AME_OK;
}

int swapValues(char *v1,char *v2, char blockType, int fileIndex) {
  // swaps records v1 and v2
  int size1, size2;
  size1 = openFilesTable[fileIndex].sizeOfKey;
  if(blockType == 'I')
    size2 = sizeof(int);
  else
    size2 = openFilesTable[fileIndex].sizeOfSecField;

  void *temp1,*temp2;

  if((temp1 = malloc(size1)) == NULL)
    return setAndReturn(AME_MEM_ERR);
  if((temp2 = malloc(size2)) == NULL)
    return setAndReturn(AME_MEM_ERR);

  memcpy(temp1,v1,size1);
  memcpy(temp2,v1 + size1, size2);
  memcpy(v1,v2,size1);
  memcpy(v1 + size1,v2 + size1, size2);
  memcpy(v2,temp1,size1);
  memcpy(v2 + size1,temp2,size2);

  free(temp1);
  free(temp2);
  return AME_OK;
}

void printBlock(BF_Block *block, int fileIndex) {
  // was used to check the contents of a block and for debugging
  char * data;
  int offset;
  int size1, size2;
  size1 = openFilesTable[fileIndex].sizeOfKey;
  char blockType;
  data = BF_Block_GetData(block);
  blockType = *data;
  int count = *(int *)(data + sizeof(char));
  printf("BLOCK TYPE: %c , Counter: %d\n",*data,count);
  data += sizeof(char) + sizeof(int);
  if(blockType == 'I') {
    size2 = sizeof(int);
    printf("POINTER: %d ",*(int *)data);
  }else{
    size2 = openFilesTable[fileIndex].sizeOfSecField;
    printf("Next Block: %d\n",*(int *)data);
  }
  data += sizeof(int);
  if(blockType == 'I')
    count -= 1;

  if(blockType == 'D' && count == 0)
    printf("(NO DATA)");

  for (int i = 0; i < count; ++i) {
    offset = i * (size1 + size2);
    if(blockType == 'I') {
      if(openFilesTable[fileIndex].keyField == 'c'){
        printf("KEY: %s POINTER: %d ",data + offset,*(int *)(data + offset + size1));
      }else if(openFilesTable[fileIndex].keyField == 'f'){
        printf("KEY: %f POINTER: %d ",*(float *)(data + offset),*(int *)(data + offset + size1));
      }else {
        printf("KEY: %d POINTER: %d ",*(int *)(data + offset),*(int *)(data + offset + size1));
      }
    } else {
      if(openFilesTable[fileIndex].keyField == 'c'){
        printf("KEY: %s ",data + offset);
      }else if(openFilesTable[fileIndex].keyField == 'f'){
        printf("KEY: %f ",*(float *)(data + offset));
      }else{
        printf("KEY: %d ",*(int *)(data + offset));
      }

      if(openFilesTable[fileIndex].secondField == 'c'){
        printf("SEC FIELD: %s | ",data + offset + size1);
      }else if(openFilesTable[fileIndex].secondField == 'f'){
        printf("SEC FIELD: %f | ",*(float *)(data + offset + size1));
      }else {
        printf("SEC FIELD: %d | ",*(int *)(data + offset + size1));
      }
    }
  }
  printf("\n\n");
}

bool indexBlock(BF_Block *block) {
  // returns true if the block is an index block, false otherwise
  char * data;
  data = BF_Block_GetData(block);
  if(*data == 'I')
    return true;

  return false;
}

void setCounter(BF_Block *block,int val) {
  // change the value of the record counter of a block
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char);
  *(int *)data = val;
}

int getCounter(BF_Block *block) {
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char);
  return *(int *)data;
}

void setNextBlck(BF_Block *block,int val) {
  // change the pointer to next block of a data block
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char) + sizeof(int);
  *(int *)data = val;
}

int getNextBlck(BF_Block *block) {
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char) + sizeof(int);
  return *(int *)data;
}

int splitDataBl(BF_Block *lblock, BF_Block *rblock, void *v1, void *v2, int fileIndex, void *retKey) {
  /* splits a data block into two, the resulting blocks are sorted and the key value of the first record of the right block
   is returned via input argument retKey */
  int count = getCounter(lblock);
  if(count % 2 != 0)
    count = count / 2 + 1;
  else
    count = count / 2;
  int ldups, rdups;
  int size1, size2, retval;
  char * data;
  ldups = 0;
  rdups = 0;
  data = BF_Block_GetData(lblock);
  data += sizeof(char) + 2 * sizeof(int);
  size1 = openFilesTable[fileIndex].sizeOfKey;
  size2 = openFilesTable[fileIndex].sizeOfSecField;
  if(cmpValues(data + count * (size1 + size2),data + (count - 1) * (size1 + size2),fileIndex) == 0) {
    // if there are duplicate key values in the middle of the block
    for (int i = count - 1; i >= 0; --i) { // count amount at the left half
      if(cmpValues(data + i * (size1 + size2),data + (count - 1) * (size1 + size2),fileIndex) == 0)
        ldups += 1;
      else
        break;
    }

    for (int i = count; i < getCounter(lblock); ++i) { // count amount at the right half
      if(cmpValues(data + i * (size1 + size2),data + (count - 1) * (size1 + size2),fileIndex) == 0)
        rdups += 1;
      else
        break;
    }

    if(ldups > rdups)
      count += rdups;
    else
      count -= ldups;
  }

  for (int i = count; i < getCounter(lblock); ++i) { // insert the right half to the right data block
    insertRecord(rblock,fileIndex,\
    (void *)(data + i * (size1 + size2)),(void *)(data + size1 + i * (size1 + size2)));
  }

  setCounter(lblock,count); // update record counter of the left data block

  // insert the new record that caused the split to the correct block and sort it
  if(cmpValues((char *)v1,data + (count - 1) * (size1 + size2),fileIndex) <= 0) {
    insertRecord(lblock, fileIndex, v1, v2);
    retval = sortBlock(lblock,fileIndex,'D');
    if(retval != AME_OK)
      return retval;
  }
  else {
    insertRecord(rblock, fileIndex, v1, v2);
    retval = sortBlock(rblock,fileIndex,'D');
    if(retval != AME_OK)
      return retval;
  }

  data = BF_Block_GetData(rblock);
  data += sizeof(char) + 2 * sizeof(int);
  memcpy(retKey,data, size1);
  if(getCounter(lblock) == 0 || getCounter(rblock) == 0)
    printf("$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");

//  printf("------------------------\n\n");
//  printBlock(lblock,fileIndex);
//  printBlock(rblock,fileIndex);
//  printf("---------------------------\n\n");

  return AME_OK;
}

int cmpValues(char * val1,char *val2,int fileIndex) {
  // returns -1 if val2 greater than val1, 0 if equal and 1 if val1 greater than val2
  if(openFilesTable[fileIndex].keyField == 'c'){
    if(strcmp(val1,val2) < 0){
      return -1;
    }else if(strcmp(val1,val2) == 0){
      return 0;
    }else if(strcmp(val1,val2) > 0){
      return 1;
    }
  }else if(openFilesTable[fileIndex].keyField == 'f'){
    if(*(float *)val1 < *(float *)val2){
      return -1;
    } else if(*(float *)val1 == *(float *)val2){
      return 0;
    } else if(*(float *)val1 > *(float *)val2){
      return 1;
    }
  }else {
    if(*(int *)val1 < *(int *)val2){
      return -1;
    } else if(*(int *)val1 == *(int *)val2){
      return 0;
    } else if(*(int *)val1 > *(int *)val2){
      return 1;
    }
  }
}

int splitIndexBl(BF_Block *lblock, BF_Block *rblock, void *v1, void *v2, int fileIndex, void *retKey) {
  /* v1 is the new key and v2 is the new pointer.
   Splits an index block into two, the resulting blocks are sorted and the key value
   that will be added to the upper levels of the tree is returned via input argument retKey */
  int count = getCounter(lblock);
  if(count % 2 != 0)
    count = count / 2 + 1;
  else
    count = count / 2;
  char * temp;
  int size1, size2;
  size1 = openFilesTable[fileIndex].sizeOfKey;
  size2 = sizeof(int);
  char * data;
  if((temp = malloc(size1 + size2)) == NULL)
    return setAndReturn(AME_MEM_ERR);
  memcpy(temp,v1,size1);
  memcpy(temp + size1, v2, size2);
  data = BF_Block_GetData(lblock);
  data += sizeof(char) + sizeof(int);
  int offset;
  offset = size2 + (getCounter(lblock) - 2) * (size1 + size2);
  swapValues(temp,data + offset,'I',fileIndex); // swap the last pair with the new pair that caused the split
  sortBlock(lblock,fileIndex,'I'); // sort the index block
  offset = count * (size1 + size2);

  // insert the right half of the left block to the right block
  for (int i = 0; i < (getCounter(lblock) - count); ++i) {
    if(i == (getCounter(lblock) - count -1)){
      insertPointer(rblock,fileIndex,*(int *)(data + offset + i *(size1 + size2)));
    }else {
      insertPointer(rblock,fileIndex,*(int *)(data + offset + i *(size1 + size2)));
      insertKey(rblock,fileIndex,(void *)(data + offset + i *(size1 + size2) + size2));
    }
  }
  // insert the biggest key value pair to the end of the right block
  insertKey(rblock,fileIndex,(void *)temp);
  insertPointer(rblock,fileIndex,*(int *)(temp + size1));

  memcpy(retKey,data + offset - size1,size1);
  setCounter(lblock,count);
  free(temp);
//  printf("------------------------\n\n");
//  printBlock(lblock,fileIndex);
//  printBlock(rblock,fileIndex);
//  printf("---------------------------\n\n");
  return AME_OK;
}

int nextDepthBlock(BF_Block * block, void * key, int fileIndex) {
  /* returns the number of the block located at the level below the current block
     in order to reach the data block that the key should be located */
  int count = getCounter(block);
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char) + sizeof(int);
  int offset;
  int retval;
  for (int i = 0; i < count; ++i) {
    offset = i * (openFilesTable[fileIndex].sizeOfKey + sizeof(int));
    if(i == count - 1) {
      retval = *(int *)(data + offset);
    } else if(cmpValues((char *)key,data + offset + sizeof(int),fileIndex) < 0) {
        retval = *(int *)(data + offset);
        break;
    }
  }

  return retval;
}

int getDataBlock(int fileIndex, void * value) {
  // returns the block number of the first data block or a data block in which the key value should be located
  char * data;

  int nextBlock = openFilesTable[fileIndex].rootIndex;
  BF_Block * block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(openFilesTable[fileIndex].fileDesc,nextBlock,block))

  while(indexBlock(block)) {
    if(value == NULL) {
      data = BF_Block_GetData(block);
      data += sizeof(char) + sizeof(int);
      nextBlock = *(int *)data;
    }
    else
      nextBlock = nextDepthBlock(block, value, fileIndex);

    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_GetBlock(openFilesTable[fileIndex].fileDesc, nextBlock, block))
  }
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return nextBlock;
}

bool wrongOp(int op) {
  if(op >= 1 && op <= 6)
    return false;

  return true;
}

void *getNextSecField(BF_Block *block, int scanIndex) {
  // get the second field value of next record to be scanned
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char) + 2 * sizeof(int);
  int offset;
  offset = scanTable[scanIndex].lastRec * (openFilesTable[scanTable[scanIndex].fileIndex].sizeOfKey + openFilesTable[scanTable[scanIndex].fileIndex].sizeOfSecField);
  return (void *)(data + offset + openFilesTable[scanTable[scanIndex].fileIndex].sizeOfKey);
}

void *getNextKeyField(BF_Block *block, int scanIndex) {
  // get the key field value of the next record to be scanned
  char * data;
  data = BF_Block_GetData(block);
  data += sizeof(char) + 2 * sizeof(int);
  int offset;
  offset = scanTable[scanIndex].lastRec * (openFilesTable[scanTable[scanIndex].fileIndex].sizeOfKey + openFilesTable[scanTable[scanIndex].fileIndex].sizeOfSecField);
  return (void *)(data + offset);
}

void *setAndReturnNULL(const int value) {
  AM_errno = value;
  return NULL;
}