#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INDEX_BLOCK_SIZE 54

const int MAX_CHUNK_SIZE = 75;

FILE *fp;

int checkParams(int argc, char *argv[]) {
  if (argc <= 1) {
    printf("No arguments were given.\n");
    return (0);
  }

  return (1);
}

int createTable(char *tableName) {
  if (strlen(tableName) > 255) {
    printf("Table name cannot excede 255 characters.\n");
    return (0);
  }
  char d_filePath[25 + strlen(tableName) + 5 + 6], i_filePath[26 + strlen(tableName) + 5 + 6];

  // TODO make sure we dont excede 2**32-1 files
  uint32_t zero = 0;

  strcpy(d_filePath, "/var/lib/tinydb/d_");
  strcat(d_filePath, tableName);
  strcat(d_filePath, ".tydb");

  fp = fopen(d_filePath, "wb");
  fwrite(&zero, sizeof(zero), 1, fp);
  fclose(fp);

  strcpy(i_filePath, "/var/lib/tinydb/i_");
  strcat(i_filePath, tableName);
  strcat(i_filePath, ".tydb");

  fp = fopen(i_filePath, "wb");
  fwrite(&zero, sizeof(zero), 1, fp);

  fclose(fp);

  return (1);
}

int keysDifference(uint8_t k1[50], uint8_t k2[50]) {
  for (int i = 0; i < 50; i++) {
    if (k1[i] > k2[i]) {
      return (1);
    } else if (k1[i] < k2[i]) {
      return (-1);
    }
  }

  return (0);
}

// sets last read key to c_key and returns position where to insert. I
// if returned position is negative then key is already in table and actual position is -returned_position.
// (Cursor doesn't have to be in returned position)
uint8_t c_key[54];
double findIndex(uint8_t k_block[54], uint32_t rowCount) {
  int start = 0, end = rowCount - 1;
  int pos = start + (end - start) / 2, p_pos = -1;

  // binary search for position
  for (int i = 0; i < rowCount; i++) {
    fread(&c_key, 54, 1, fp);

    int kd = keysDifference(c_key, k_block);

    // printf("start: %d, end: %d\n", start, end);
    // if input key bigger than current key
    if (kd == 1) {
      if (end - start == 0) {
        return ((ftell(fp) - 54) / 54);
        // fwrite(&k_block, sizeof(k_block), 1, fp); fwrite(&newRowCount, sizeof(newRowCount), 1, fp);
      }

      end = pos - 1;
      pos = start + (end - start) / 2;

    }
    // if input key smaller than current key
    else if (kd == -1) {
      if (end - start == 0) {
        return ((ftell(fp) - 4) / 54);
      }

      start = pos + 1;
      pos = start + (end - start) / 2;

    }
    // if keys match
    else {
      return -1.0 * ((ftell(fp) - 54) / 54);
    }

    fseek(fp, 4 + INDEX_BLOCK_SIZE * pos, SEEK_SET);
  }
}

void writeRowCount(uint32_t newRowCount) {
  fseek(fp, 0, SEEK_SET);
  fwrite(&newRowCount, sizeof(newRowCount), 1, fp);
}

char *findInTable(char *tableName, char *key) {
  int out = 0;

  char d_filePath[25 + strlen(tableName) + 5 + 6], i_filePath[26 + strlen(tableName) + 5 + 6];

  if (strlen(key) > 50) {
    printf("Key cannot excede 50 characters.\n");
    out = 0;
    goto ret;
  }

  strcpy(d_filePath, "/var/lib/tinydb/d_");
  strcat(d_filePath, tableName);
  strcat(d_filePath, ".tydb");

  strcpy(i_filePath, "/var/lib/tinydb/i_");
  strcat(i_filePath, tableName);
  strcat(i_filePath, ".tydb");

  fp = fopen(i_filePath, "rb+	");

  if (fp == NULL) {
    printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
    out = 0;
    goto ret;
  }

  /*
    50 bytes for key
    4 bytes for locator

    = total of 54 bytes/block
  */

  uint32_t rowCount, newRowCount;
  fread(&rowCount, sizeof(rowCount), 1, fp);
  newRowCount = rowCount + 1;

  uint8_t k_block[54];
  for (int i = 0; i < strlen(key); i++) {
    k_block[i] = key[i];
  }
  for (int i = 0; i < 50 - strlen(key); i++) {
    // TODO bad implementation to end key with null bytes
    k_block[i + strlen(key)] = 0;
  }

  double indexPosition = findIndex(k_block, rowCount);

  fclose(fp);

  double nzero = -0.0;

  if (indexPosition < 0 || (memcmp(&nzero, &indexPosition, sizeof(nzero)) == 0)) {
    uint32_t pos;
    pos |= (uint32_t)c_key[50] << 24;
    pos |= (uint32_t)c_key[51] << 16;
    pos |= (uint32_t)c_key[52] << 8;
    pos |= (uint32_t)c_key[53];

    // printf("%d\n", pos);
    fp = fopen(d_filePath, "rb+	");

    if (fp == NULL) {
      printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
      out = 0;
      goto ret;
    }

    fseek(fp, pos, SEEK_SET);
    uint8_t d_a_size[4];
    fread(&d_a_size, 4, 1, fp);

    uint32_t d_size;
    d_size |= (uint32_t)d_a_size[0] << 24;
    d_size |= (uint32_t)d_a_size[1] << 16;
    d_size |= (uint32_t)d_a_size[2] << 8;
    d_size |= (uint32_t)d_a_size[3];

    char *data = malloc(d_size);
    fread(data, d_size, 1, fp);

    fclose(fp);

    return (data);

  } else {
    printf("Key does not exist.\n");
    out = 2;
    goto ret;
  }

ret:
  if (out == 2) {
    out = 0;
    fclose(fp);
  } else if (out == 3) {
    out = 1;
    fclose(fp);
  }

  return ("hello");
}

int addToTable(char *tableName, char *key, char *value) {
  int out = 1;

  char d_filePath[25 + strlen(tableName) + 5 + 6], i_filePath[26 + strlen(tableName) + 5 + 6];

  /*
    4 bytes for value size
    ? bytes for value
    = tota 4 + ?
  */
  uint8_t d_block[4 + strlen(value)];

  if (strlen(key) > 50) {
    printf("Key cannot excede 50 characters.\n");
    out = 0;
    goto ret;
  }

  strcpy(d_filePath, "/var/lib/tinydb/d_");
  strcat(d_filePath, tableName);
  strcat(d_filePath, ".tydb");

  strcpy(i_filePath, "/var/lib/tinydb/i_");
  strcat(i_filePath, tableName);
  strcat(i_filePath, ".tydb");

  uint32_t dataSize = strlen(value);

  // printf("dataSize: %d\n", dataSize);

  // size of value
  d_block[0] = dataSize << 24;
  d_block[1] = dataSize << 16;
  d_block[2] = dataSize << 8;
  d_block[3] = dataSize;

  // value
  for (int i = 0; i < strlen(value); i++) {
    d_block[4 + i] = value[i];
  }

  fp = fopen(d_filePath, "rb+	");

  if (fp == NULL) {
    printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
    out = 0;
    goto ret;
  }

  fseek(fp, 0, SEEK_END);
  uint32_t d_positoin = ftell(fp);

  fwrite(&d_block, sizeof(d_block), 1, fp);

  fclose(fp);

  fp = fopen(i_filePath, "rb+	");

  if (fp == NULL) {
    printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
    out = 0;
    goto ret;
  }

  /*
    50 bytes for key
    4 bytes for locator

    = total of 54 bytes/block
  */

  uint32_t rowCount, newRowCount;
  fread(&rowCount, sizeof(rowCount), 1, fp);
  newRowCount = rowCount + 1;

  uint8_t k_block[54];
  for (int i = 0; i < strlen(key); i++) {
    k_block[i] = key[i];
  }
  for (int i = 0; i < 50 - strlen(key); i++) {
    // TODO bad implementation to end key with null bytes
    k_block[i + strlen(key)] = 0;
  }

  k_block[50] = d_positoin >> 24;
  k_block[51] = d_positoin >> 16;
  k_block[52] = d_positoin >> 8;
  k_block[53] = d_positoin >> 0;

  uint32_t pos;
  pos |= (uint32_t)k_block[50] << 24;
  pos |= (uint32_t)k_block[51] << 16;
  pos |= (uint32_t)k_block[52] << 8;
  pos |= (uint32_t)k_block[53];

  // just insert if first entry
  if (rowCount == 0) {
    fwrite(&k_block, sizeof(k_block), 1, fp);
    writeRowCount(newRowCount);
    out == 3;
    goto ret;
  }

  double indexPosition = findIndex(k_block, rowCount);

  double nzero = -0.0;

  if (indexPosition < 0 || (memcmp(&nzero, &indexPosition, sizeof(nzero)) == 0)) {
    printf("Key already exists in table.\n");
    out = 2;
    goto ret;
  } else {
    // TODO bug with order of c & d sometimes
    if (rowCount - (uint32_t)indexPosition > 0) {
      // int moveCount = rowCount - (uint32_t)indexPosition;
      uint8_t movable_sector[MAX_CHUNK_SIZE * INDEX_BLOCK_SIZE + 5];

      int moveCount = rowCount - (uint32_t)indexPosition, i = 0;

      int passAmmount = moveCount / MAX_CHUNK_SIZE, chunk = MAX_CHUNK_SIZE;
      if (moveCount % MAX_CHUNK_SIZE != 0) passAmmount++;

      fseek(fp, 0, SEEK_END);
      for (int i = 0; i < passAmmount; i++) {
        if (i + 1 < passAmmount)
          chunk = MAX_CHUNK_SIZE;
        else
          chunk = moveCount % MAX_CHUNK_SIZE;

        fseek(fp, -(chunk)*INDEX_BLOCK_SIZE, SEEK_CUR);
        fread(&movable_sector, chunk * INDEX_BLOCK_SIZE, 1, fp);

        fseek(fp, -(chunk - 1) * INDEX_BLOCK_SIZE, SEEK_CUR);

        fwrite(&movable_sector, chunk * INDEX_BLOCK_SIZE, 1, fp);
        fseek(fp, -((2 + chunk) * INDEX_BLOCK_SIZE), SEEK_CUR);
      }
    }

    fseek(fp, (uint32_t)indexPosition * INDEX_BLOCK_SIZE + 4, SEEK_SET);

    fwrite(&k_block, 54, 1, fp);

    writeRowCount(newRowCount);
  }

  fclose(fp);

  fp = fopen(d_filePath, "rb+	");

  if (fp == NULL) {
    printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
    out = 0;
    goto ret;
  }

  fclose(fp);

ret:
  if (out == 2) {
    out = 0;
    fclose(fp);
  } else if (out == 3) {
    out = 1;
    fclose(fp);
  }

  return (out);
}

char *main(int argc, char *argv[]) {
  int out = 0;
  if (!checkParams(argc, argv)) {
    out = 1;
    goto end;
  }

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "create") == 0) {
      if (argc - 1 < i + 1) {
        printf("You need to pass the name of your table.\n\n    tinydb create NAME\n\n\n");
        out = 1;
        goto end;
      }

      if (!createTable(argv[2])) {
        out = 2;
        goto end;
      }

      i++;
    } else if (strcmp(argv[i], "add") == 0) {
      if (argc - 1 < i + 1) {
        printf("You need to pass the name of your table.\n\n    tinydb add NAME key value\n\n\n");
        out = 1;
        goto end;
      }
      if (argc - 1 < i + 3) {
        printf("You need to pass the key and value you'd like to add to the database.\n\n    tinydb add name KEY VALUE\n\n\n");
        out = 1;
        goto end;
      }

      if (!addToTable(argv[2], argv[3], argv[4])) {
        out = 2;
        goto end;
      }

      i += 3;
    } else if (strcmp(argv[i], "find") == 0) {
      if (argc - 1 < i + 1) {
        printf("You need to pass the name of your table.\n\n    tinydb add NAME key value\n\n\n");
        out = 1;
        goto end;
      }
      if (argc - 1 < i + 2) {
        printf("You need to pass the key you'd like to find in the database.\n\n    tinydb find name KEY\n\n\n");
        out = 1;
        goto end;
      }

      char *value = findInTable(argv[2], argv[3]);
      // printf("%s\n", value);
      return (value);
      free(value);
      // if (!) {
      //   out = 2;
      //   goto end;
      // }

      i += 2;
    } else {
      printf("Unknown argument: '%s'\n", argv[i]);
      out = 1;
      goto end;
    }
  }

end:
  if (out == 1) printf("Use --h for help.\n");

  // return (out);
}
