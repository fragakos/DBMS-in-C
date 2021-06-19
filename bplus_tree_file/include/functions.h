//
// Created by theodore on 15/12/19.
//

#ifndef BPLUS_TREE_FILE_FUNCTIONS_H
#define BPLUS_TREE_FILE_FUNCTIONS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "DefsxStructs.h"

extern scanInfo *scanTable;
extern info *openFilesTable;
extern int openFiles; //Count the open files
extern int scanningFiles; //Count the scanning files

bool wrongInput(const char attrType,const int attrLength);
bool nameExists(const char * const fileName);
bool tableIsFull();
bool scanTableIsFull();
int setAndReturn(const int value);
bool beingScanned(const int fileDesc);
int maxIndexPointers(const int fileIndex);
int maxDataRecords(const int fileIndex);
int makeDataBlock(int nextBlock, BF_Block *block, int fileIndex);
char *getToEnd(char *data,int counter, int size1, int size2);
void insertRecord(BF_Block *block, int fileIndex, void *value1, void *value2);
int updateMetadata(int fileDesc, int newRoot);
void updateFileTable(char * fileName, int newRoot);
void insertKey(BF_Block *block, int fileIndex,void *value);
void insertPointer(BF_Block *block, int fileIndex,int value);
int makeIndexBlock(BF_Block *block, int fileIndex);
bool blockIsFull(BF_Block *block,int fileIndex, char blockType);
int swapValues(char *v1 ,char *v2, char blockType, int fileIndex);
int sortBlock(BF_Block *block,int fileIndex, char blockType);
void printBlock(BF_Block *block, int fileIndex);
bool indexBlock(BF_Block *block);
void setCounter(BF_Block *block, int val);
void setNextBlck(BF_Block *block, int val);
int getNextBlck(BF_Block *block);
int getCounter(BF_Block *block);
int cmpValues(char * val1,char *val2,int fileIndex);
int splitDataBl(BF_Block *lblock, BF_Block *rblock, void *v1, void *v2, int fileIndex, void *retKey);
int splitIndexBl(BF_Block *lblock, BF_Block *rblock, void *v1, void *v2, int fileIndex, void *retKey);
int nextDepthBlock(BF_Block * block, void * key, int fileIndex);
int getDataBlock(int fileIndex, void * value);
bool wrongOp(int op);
void *getNextSecField(BF_Block *block, int scanIndex);
void *getNextKeyField(BF_Block *block, int scanIndex);
void *setAndReturnNULL(const int value);

#endif //BPLUS_TREE_FILE_FUNCTIONS_H
