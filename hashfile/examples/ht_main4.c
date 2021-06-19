#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1883 // you can change it if you want
#define BUCKETS_NUM 133  // you can change it if you want
#define FILE_NAME "data.db"

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

  int indexDesc, indexDesc_2;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc_2));

  Record record;
  srand(time(0));
  int r, r1;
  printf("Insert %d Entries to the file with IndexDesc = %d\n", RECORDS_NUM / 2, indexDesc);
  for (int id = 0; id < (RECORDS_NUM / 2); ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
  }

  printf("RUN PrintAllEntries for IndexDesc = %d\n", indexDesc);
  // int id = rand() % RECORDS_NUM;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

  printf("\n //////////EDITING %s FROM OTHER IndexDesc///////////// \n\n\n",FILE_NAME );

  printf("Insert another %d Entries to the same file but with IndexDesc = %d\n", RECORDS_NUM / 2, indexDesc_2);
  for (int id_2 = (RECORDS_NUM / 2); id_2 < RECORDS_NUM; ++id_2) {
    record.id = id_2;
    r1 = rand() % 12;
    memcpy(record.name, names[r1], strlen(names[r1]) + 1);
    r1 = rand() % 12;
    memcpy(record.surname, surnames[r1], strlen(surnames[r1]) + 1);
    r1 = rand() % 10;
    memcpy(record.city, cities[r1], strlen(cities[r1]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc_2, record));
  }

  printf("RUN PrintAllEntries for IndexDesc = %d\n", indexDesc_2);
  // int id_2 = rand() % RECORDS_NUM;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc_2, NULL));


  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc_2));
  BF_Close();
}
