#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1700 // you can change it if you want
#define RECORDS_NUM_TEST_0_ 17000
#define RECORDS_NUM_TEST_1_ 100000
#define BUCKETS_NUM 13  // you can change it if you want
#define BUCKETS_NUM_TEST_0_ 100
#define BUCKETS_NUM_TEST_1_ 1000
#define FILE_NAME "data.db"
#define FILE_NAME_TEST_0_ "data_test_0_.db"
#define FILE_NAME_TEST_1_ "data_test_1_.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);

  CALL_OR_DIE(HT_Init());

  int indexDesc, indexDesc_test_0_, indexDesc_test_1_;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc));

  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_TEST_0_, BUCKETS_NUM_TEST_0_));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_TEST_0_, &indexDesc_test_0_));

  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_TEST_1_, BUCKETS_NUM_TEST_1_));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_TEST_1_, &indexDesc_test_1_));
  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record)); //FILE_NAME
  }

  for (int id = 0; id < RECORDS_NUM_TEST_0_; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc_test_0_, record)); //FILE_NAME_TEST_0_
  }

  for (int id = 0; id < RECORDS_NUM_TEST_1_; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc_test_1_, record)); //FILE_NAME_TEST_1_
  }

  printf("RUN PrintAllEntries\n\n");
  int id = rand() % RECORDS_NUM;
  int id2 = rand() % RECORDS_NUM_TEST_0_;
  int id3 = rand() % RECORDS_NUM_TEST_1_;

  //FILE_NAME
  printf("$$$$$ %s $$$$$\n" , FILE_NAME);
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

  //FILE_NAME
  printf("Delete Entry with id = %d from %s\n" ,id , FILE_NAME);
  CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));

  //FILE_NAME
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

  //FILE_NAME_TEST_0_
  printf("$$$$$ %s $$$$$\n" , FILE_NAME_TEST_0_);
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc_test_0_, &id2));

  //FILE_NAME_TEST_0_
  printf("Delete Entry with id = %d from %s\n" ,id2, FILE_NAME_TEST_0_);
  CALL_OR_DIE(HT_DeleteEntry(indexDesc_test_0_, id2));

  //FILE_NAME_TEST_0_
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc_test_0_, &id2));

  //FILE_NAME_TEST_1_
  printf("$$$$$ %s $$$$$\n" , FILE_NAME_TEST_1_);
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc_test_1_, &id3));

  //FILE_NAME_TEST_1_
  printf("Delete Entry with id = %d from %s\n" ,id3, FILE_NAME_TEST_1_);
  CALL_OR_DIE(HT_DeleteEntry(indexDesc_test_1_, id3));

  //FILE_NAME_TEST_1_
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc_test_1_, &id3));


  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc_test_0_));
  CALL_OR_DIE(HT_CloseFile(indexDesc_test_1_));
  BF_Close();
}