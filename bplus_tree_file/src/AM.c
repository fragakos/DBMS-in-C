#include "AM.h"

int AM_errno = AME_OK;

scanInfo *scanTable;
info *openFilesTable;
int openFiles; //Counter of open files
int scanningFiles; //Counter of scanning files

void AM_Init() {
  openFiles = 0;
  scanningFiles = 0;
  
  BF_Init(MRU);
  
  if((openFilesTable = malloc(MAXOPENFILES * sizeof(info))) == NULL) {
    AM_errno = AME_MEM_ERR;
    return;
  }

  for(int i = 0; i < MAXOPENFILES; ++i)
    openFilesTable[i].fileName = NULL;

  if((scanTable = malloc(MAXSCANS * sizeof(scanInfo))) == NULL) {
    AM_errno = AME_MEM_ERR;
    return;
  }

  for(int i = 0; i < MAXSCANS; ++i)
    scanTable[i].isUsed = false;
}


int AM_CreateIndex(char *fileName,
	               char attrType1,
	               int attrLength1,
	               char attrType2,
	               int attrLength2) {

  if(nameExists(fileName))
    return setAndReturn(AME_FILE_EXISTS);
  
  if(wrongInput(attrType1,attrLength1))
    return setAndReturn(AME_INPUT_ERR);
  
  if(wrongInput(attrType2,attrLength2))
    return setAndReturn(AME_INPUT_ERR);
  
  BF_Block *block;
  BF_Block_Init(&block);
  int fileDesc;
  int offset = 0;
  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fileDesc));
  CALL_BF(BF_AllocateBlock(fileDesc, block));
  char *bplusIdentifier = "B+";
  char *data;
  data = BF_Block_GetData(block);

  /* METADATA INSERTION */
  memcpy(&data[offset], bplusIdentifier, strlen(bplusIdentifier) + 1);
  offset += strlen(bplusIdentifier) + 1;
  
  memcpy(&data[offset], &attrType1, sizeof(char)); // insert key data type
  offset += sizeof(char);

  memcpy(&data[offset], &attrLength1, sizeof(int)); // insert key size
  offset += sizeof(int);

  memcpy(&data[offset], &attrType2, sizeof(char)); // insert second field data type
  offset += sizeof(char);

  memcpy(&data[offset], &attrLength2, sizeof(int)); // insert second field size
  offset += sizeof(int);

  int rootIndex = 0;
  int depth = 0;
  memcpy(&data[offset], &rootIndex, sizeof(int)); // init root (= 0)
  offset += sizeof(int);

  memcpy(&data[offset], &depth, sizeof(int)); // init depth (= 0)
  /***********/

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(fileDesc));
  BF_Block_Destroy(&block);

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
  if(nameExists(fileName))
    return setAndReturn(AME_FILE_OPEN);

  if(remove(fileName))
    return setAndReturn(AME_DISK_ERR);

  return AME_OK;
}


int AM_OpenIndex(char *fileName) {
  if(tableIsFull())
    return setAndReturn(AME_MAX_FILES);

  BF_Block *block;
  BF_Block_Init(&block);
  int fileDesc;

  CALL_BF(BF_OpenFile(fileName, &fileDesc));
  CALL_BF(BF_GetBlock(fileDesc, 0, block));	

  char *data;
  data = BF_Block_GetData(block);
  int tablePos;
  int offset = strlen("B+") + 1;

  for(int i = 0; i < MAXOPENFILES; ++i)
  {
    if(openFilesTable[i].fileName == NULL)
    {
      tablePos = i;
      break;
    }
  }
  int * temp;
  
  openFilesTable[tablePos].keyField = data[offset];
  offset += sizeof(char);

  temp = (int *)(data + offset);
  openFilesTable[tablePos].sizeOfKey = *temp;
  offset += sizeof(int);

  openFilesTable[tablePos].secondField = data[offset];
  offset += sizeof(char);
  
  temp = (int *)(data + offset);
  openFilesTable[tablePos].sizeOfSecField = *temp;
  offset += sizeof(int);

  openFilesTable[tablePos].fileDesc = fileDesc;
  
  temp = (int *)(data + offset);
  openFilesTable[tablePos].rootIndex = *temp;
  offset += sizeof(int);

  temp = (int *)(data + offset);
  openFilesTable[tablePos].depth = *temp;

  if((openFilesTable[tablePos].fileName =\
   malloc(strlen(fileName) + 1)) == NULL)
    return setAndReturn(AME_MEM_ERR);
  
  strcpy(openFilesTable[tablePos].fileName, fileName);

  openFiles++;
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  
  return tablePos;
}


int AM_CloseIndex(int fileDesc) {
  int tablePos = fileDesc;
  if(beingScanned(openFilesTable[tablePos].fileDesc))
    return setAndReturn(AME_FILE_SCANNING);
  
  CALL_BF(BF_CloseFile(openFilesTable[tablePos].fileDesc));

  free(openFilesTable[tablePos].fileName);
  openFilesTable[tablePos].fileName = NULL;
  openFiles--;
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  int retval;

  if(beingScanned(openFilesTable[fileDesc].fileDesc))
    return setAndReturn(AME_FILE_SCANNING);

  BF_Block *block;
  BF_Block_Init(&block);

  if(openFilesTable[fileDesc].rootIndex == 0) { /* first insertion */

    int leftBlockKey,rightBlockKey;


    /*  make left and right data blocks  */
    if((rightBlockKey = makeDataBlock(0,block,fileDesc)) < 0)
      return AM_errno;
    insertRecord(block, fileDesc, value1, value2);
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    if((leftBlockKey = makeDataBlock(rightBlockKey,block,fileDesc)) < 0)
      return AM_errno;
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    /*   make root index block  */
    int root;
    if((root = makeIndexBlock(block,fileDesc)) < 0)
      return AM_errno;

    insertPointer(block, fileDesc, leftBlockKey);
    insertKey(block, fileDesc, value1);
    insertPointer(block,fileDesc, rightBlockKey);

    // update the open files table and the file itself
    updateFileTable(openFilesTable[fileDesc].fileName, root);
    retval = updateMetadata(openFilesTable[fileDesc].fileDesc, root);
    if(retval != AME_OK)
      return AM_errno;
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }
  else { // tree exists
    int tableSize = openFilesTable[fileDesc].depth + 1;
    int pathTable[tableSize];
    void * newKey;
    if((newKey = malloc(openFilesTable[fileDesc].sizeOfKey)) == NULL)
      return setAndReturn(AME_MEM_ERR);
    pathTable[0] = openFilesTable[fileDesc].rootIndex;
    for (int i = 1; i < tableSize; ++i) { // reach the data block and store the path in the pathTable
      CALL_BF(BF_GetBlock(openFilesTable[fileDesc].fileDesc,pathTable[i - 1],block))
      pathTable[i] = nextDepthBlock(block,value1,fileDesc);
      CALL_BF(BF_UnpinBlock(block))
    }
    CALL_BF(BF_GetBlock(openFilesTable[fileDesc].fileDesc,pathTable[tableSize - 1],block))

    if(blockIsFull(block,openFilesTable[fileDesc].fileDesc,'D')) { // need to split the data block
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      int bnum = makeDataBlock(getNextBlck(block),newBlock,fileDesc); // make the new block
      if(bnum < 0)
        return AM_errno;

      setNextBlck(block,bnum); // update the next data block pointer of the full block
      retval = splitDataBl(block,newBlock,value1,value2,fileDesc,newKey);
      if(retval != AME_OK)
        return AM_errno;
      BF_Block_SetDirty(block);
      BF_Block_SetDirty(newBlock);
      CALL_BF(BF_UnpinBlock(block))
      CALL_BF(BF_UnpinBlock(newBlock))

      int bpointr = bnum;
      for (int i = tableSize - 2; i >= 0; --i) { // go up the path and insert (key,pointer) pairs

        CALL_BF(BF_GetBlock(openFilesTable[fileDesc].fileDesc,pathTable[i],block))
        if(blockIsFull(block,openFilesTable[fileDesc].fileDesc,'I')) { // need to split the index block
          bnum = makeIndexBlock(newBlock,fileDesc); // make new index block
          if(bnum < 0)
            return AM_errno;

          retval = splitIndexBl(block,newBlock,newKey,&bpointr,fileDesc,newKey);
          if(retval != AME_OK)
            return AM_errno;
          bpointr = bnum;
          BF_Block_SetDirty(block);
          BF_Block_SetDirty(newBlock);
          CALL_BF(BF_UnpinBlock(block))
          CALL_BF(BF_UnpinBlock(newBlock))
          if(i == 0) { // root has been reached, make new root
            printf("Making new root\n");
            int newR = makeIndexBlock(block,fileDesc);
            if(newR < 0)
              return AM_errno;

            insertPointer(block, fileDesc, pathTable[0]);
            insertKey(block, fileDesc, newKey);
            insertPointer(block,fileDesc, bpointr);

            // update the open files table and the file itself
            updateFileTable(openFilesTable[fileDesc].fileName, newR);
            retval = updateMetadata(openFilesTable[fileDesc].fileDesc, newR);
            if(retval != AME_OK)
              return AM_errno;
            BF_Block_SetDirty(block);
            CALL_BF(BF_UnpinBlock(block))
          }
        }
        else { // index block has space, insert (key,pointer) pair and sort
          insertKey(block,fileDesc,newKey);
          insertPointer(block,fileDesc,bpointr);
          sortBlock(block,fileDesc,'I');
          BF_Block_SetDirty(block);
          CALL_BF(BF_UnpinBlock(block))
          break;
        }

      }
      BF_Block_Destroy(&newBlock);
    }
    else { // data block has space, insert record and sort
      insertRecord(block,fileDesc,value1,value2);
      sortBlock(block,fileDesc,'D');
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block))
    }
    free(newKey);
  }

  BF_Block_Destroy(&block);
  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  if(scanTableIsFull())
    return setAndReturn(AME_MAX_SCANS);

  if(wrongOp(op))
    return setAndReturn(AME_OP_ERR);

  int tablePos;
  for (int i = 0; i < MAXSCANS; ++i) {
    if(!scanTable[i].isUsed){
      tablePos = i;
      break;
    }
  }
  scanTable[tablePos].fileDesc = openFilesTable[fileDesc].fileDesc;
  scanTable[tablePos].op = op;
  scanTable[tablePos].isUsed = true;
  scanTable[tablePos].lastRec = 0;
  scanTable[tablePos].fileIndex = fileDesc;

  if((scanTable[tablePos].key = malloc(openFilesTable[fileDesc].sizeOfKey)) == NULL)
    return setAndReturn(AME_MEM_ERR);
  memcpy(scanTable[tablePos].key,value,openFilesTable[fileDesc].sizeOfKey);

  if((scanTable[tablePos].retVal = malloc(openFilesTable[fileDesc].sizeOfSecField)) == NULL)
    return setAndReturn(AME_MEM_ERR);

  if(op == EQUAL || op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL)
    scanTable[tablePos].block = getDataBlock(fileDesc,value); // get number of the data block in which the key should be located
  else
    scanTable[tablePos].block = getDataBlock(fileDesc,NULL); // get number of the most left data block

  if(scanTable[tablePos].block < 0)
    return AM_errno;

  scanningFiles++;
  return tablePos;
}


void *AM_FindNextEntry(int scanDesc) {
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_BF_NULL(BF_GetBlock(scanTable[scanDesc].fileDesc,scanTable[scanDesc].block,block))

  // if all records of current block have been scanned, end scan or go to next block
  if(scanTable[scanDesc].op == EQUAL) {
    if(scanTable[scanDesc].lastRec == getCounter(block))
      return setAndReturnNULL(AME_EOF);
  } else {
    if(scanTable[scanDesc].lastRec == getCounter(block)) {
      if(getNextBlck(block) == 0)
        return setAndReturnNULL(AME_EOF);
      else {
        scanTable[scanDesc].block = getNextBlck(block);
        CALL_BF_NULL(BF_UnpinBlock(block))
        CALL_BF_NULL(BF_GetBlock(scanTable[scanDesc].fileDesc,scanTable[scanDesc].block,block))
        scanTable[scanDesc].lastRec = 0;
      }
    }
  }


  switch (scanTable[scanDesc].op)
  {
    case EQUAL:
      while(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) > 0) { // skip records with smaller key values
        scanTable[scanDesc].lastRec += 1;

        if(scanTable[scanDesc].lastRec == getCounter(block)) // end scan if all records have been scanned
          return setAndReturnNULL(AME_EOF);
      }

      if(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),scanTable[scanDesc].fileIndex) < 0)
        return setAndReturnNULL(AME_EOF); // end scan if next record has greater key value

      break;

    case NOT_EQUAL:
      while(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) == 0) { // skip records with equal key values
        scanTable[scanDesc].lastRec += 1;

        if(scanTable[scanDesc].lastRec == getCounter(block)) {
          if(getNextBlck(block) == 0)
            return setAndReturnNULL(AME_EOF);
          else {
            scanTable[scanDesc].block = getNextBlck(block);
            CALL_BF_NULL(BF_UnpinBlock(block))
            CALL_BF_NULL(BF_GetBlock(scanTable[scanDesc].fileDesc,scanTable[scanDesc].block,block))
            scanTable[scanDesc].lastRec = 0;
          }
        }
      }

      break;

    case LESS_THAN:
      if(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) == 0) // end scan if next record has equal key value
        return setAndReturnNULL(AME_EOF);

      break;

    case GREATER_THAN:
      while(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) >= 0) { // skip records with equal or smaller keys
        scanTable[scanDesc].lastRec += 1;

        if(scanTable[scanDesc].lastRec == getCounter(block)) {
          if(getNextBlck(block) == 0)
            return setAndReturnNULL(AME_EOF);
          else {
            scanTable[scanDesc].block = getNextBlck(block);
            CALL_BF_NULL(BF_UnpinBlock(block))
            CALL_BF_NULL(BF_GetBlock(scanTable[scanDesc].fileDesc,scanTable[scanDesc].block,block))
            scanTable[scanDesc].lastRec = 0;
          }
        }
      }

      break;

    case LESS_THAN_OR_EQUAL:
      if(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) < 0) // end scan if next record has greater key value
        return setAndReturnNULL(AME_EOF);

      break;

    case GREATER_THAN_OR_EQUAL:
      while(cmpValues((char *)scanTable[scanDesc].key,(char *)getNextKeyField(block,scanDesc),\
          scanTable[scanDesc].fileIndex) > 0) { // skip records with smaller key values
        scanTable[scanDesc].lastRec += 1;

        if(scanTable[scanDesc].lastRec == getCounter(block)) {
          if(getNextBlck(block) == 0)
            return setAndReturnNULL(AME_EOF);
          else {
            scanTable[scanDesc].block = getNextBlck(block);
            CALL_BF_NULL(BF_UnpinBlock(block))
            CALL_BF_NULL(BF_GetBlock(scanTable[scanDesc].fileDesc,scanTable[scanDesc].block,block))
            scanTable[scanDesc].lastRec = 0;
          }
        }
      }

      break;
  }

  memcpy(scanTable[scanDesc].retVal, getNextSecField(block,scanDesc), openFilesTable[scanTable[scanDesc].fileIndex].sizeOfSecField);
  scanTable[scanDesc].lastRec += 1;
  CALL_BF_NULL(BF_UnpinBlock(block))
  BF_Block_Destroy(&block);
  return scanTable[scanDesc].retVal;
}


int AM_CloseIndexScan(int scanDesc) {
  if(!scanTable[scanDesc].isUsed)
    return setAndReturn(AME_NO_SCAN);

  scanTable[scanDesc].isUsed = false;
  free(scanTable[scanDesc].retVal);
  free(scanTable[scanDesc].key);

  scanningFiles--;
  return AME_OK;
}


void AM_PrintError(char *errString) {
  printf("%s\n",errString);

  switch (AM_errno)
  {
    case AME_MEM_ERR:
      printf("Error during heap allocation.\n");
      break;

    case AME_INPUT_ERR:
      printf("Wrong input regarding the record fields.\n");
      break;

    case AME_BF_ERR:
      printf("Error in BF level.\n");
      break;

    case AME_FILE_OPEN:
      printf("The file couldn't be deleted because it is still open.\n");
      break;

    case AME_DISK_ERR:
      printf("Error during deletion of file.\n");
      break;

    case AME_MAX_FILES:
      printf("Open files limit reached.\n");
      break;

    case AME_FILE_SCANNING:
      printf("The file couldn't be closed because it is being scanned.\n");
      break;

    case AME_MAX_SCANS:
      printf("Scans limit reached.\n");
      break;

    case AME_OP_ERR:
      printf("Wrong scan operator.\n");
      break;

    case AME_NO_SCAN:
      printf("Scan has been already closed or doesn't exist.\n");
      break;

    case AME_FILE_EXISTS:
      printf("A file with the same name exists.\n");
      break;
  }
}

void AM_Close() {
  free(openFilesTable);
  free(scanTable);

  BF_Close();
}
