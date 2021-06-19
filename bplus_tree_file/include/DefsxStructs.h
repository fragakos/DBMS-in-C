//
// Created by theodore on 15/12/19.
//

#ifndef BPLUS_TREE_FILE_DEFSXSTRUCTS_H
#define BPLUS_TREE_FILE_DEFSXSTRUCTS_H

#include <stdbool.h>

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6
#define MAXOPENFILES 20
#define MAXSCANS 20

/* Error codes */
#define AME_OK 0
#define AME_EOF -1

#define AME_MEM_ERR -2
#define AME_INPUT_ERR -3
#define AME_BF_ERR -4
#define AME_FILE_OPEN -5
#define AME_DISK_ERR -6
#define AME_MAX_FILES -7
#define AME_FILE_SCANNING -8
#define AME_MAX_SCANS -9
#define AME_OP_ERR -10
#define AME_NO_SCAN -11
#define AME_FILE_EXISTS -12
/********************/

typedef struct info
{
    int fileDesc;
    char *fileName;
    char keyField, secondField;
    int sizeOfKey, sizeOfSecField;
    int rootIndex;
    int depth;
}info;

typedef struct scanInfo
{
    int fileDesc;
    int lastRec;
    int fileIndex;
    bool isUsed;
    void * key;
    void * retVal;
    int op;
    int block;
}scanInfo;

#define CALL_BF(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      AM_errno = AME_BF_ERR;  \
      return AM_errno;        \
    }                         \
  }

#define CALL_BF_NULL(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      AM_errno = AME_BF_ERR;  \
      return NULL;        \
    }                         \
  }

#endif //BPLUS_TREE_FILE_DEFSXSTRUCTS_H
