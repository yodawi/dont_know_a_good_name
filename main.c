#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define INDEX_BLOCK_SIZE 54
int out = 0;
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
  // zero++;
  // fwrite(&zero, sizeof(zero), 1, fp);
  fclose(fp);

  return (1);
}

int keysDifference(uint8_t k1[50], uint8_t k2[50]) {
  for (int i = 0; i < 50; i++) {
    printf("%d %d\n", k1[i], k2[i]);
    if (k1[i] > k2[i]) {
      return (1);
    } else if (k1[i] < k2[i]) {
      return (-1);
    }
  }

  return (0);
}

void findIndex() {}

int addToTable(char *tableName, char *key, char *value) {
  if (strlen(key) > 50) {
    printf("Key name cannot excede 50 characters.\n");
    return (0);
  }

  char d_filePath[25 + strlen(tableName) + 5 + 6], i_filePath[26 + strlen(tableName) + 5 + 6];

  strcpy(d_filePath, "/var/lib/tinydb/d_");
  strcat(d_filePath, tableName);
  strcat(d_filePath, ".tydb");

  strcpy(i_filePath, "/var/lib/tinydb/i_");
  strcat(i_filePath, tableName);
  strcat(i_filePath, ".tydb");

  fp = fopen(i_filePath, "rb+	");

  if (fp == NULL) {
    printf("Table '%s' does not exist. To create a table use:\n\n    tinydb create NAME\n", tableName);
    return (0);
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

  k_block[49] = newRowCount << 24;
  k_block[50] = newRowCount << 16;
  k_block[51] = newRowCount << 8;
  k_block[52] = newRowCount;

  int start = 0, end = rowCount - 1;
  int pos = start + (end - start) / 2, p_pos = -1;
  uint8_t c_key[54];

  // just insert if first entry
  if (rowCount == 0) {
    fwrite(&k_block, sizeof(k_block), 1, fp);

    uint32_t prwc = rowCount + 1;
    fwrite(&prwc, sizeof(prwc), 1, fp);
  }

  // binary search for position
  for (int i = 0; i < rowCount; i++) {
    fread(&c_key, 54, 1, fp);
    // printf("%d %d %d\n", start, pos, end);

    int kd = keysDifference(c_key, k_block);

    if (kd == 1) {
      if (end - start == 0) {
        fseek(fp, -50, SEEK_CUR);
        printf("pos: %ld\n", (ftell(fp) - 4) / 54);
        // fwrite(&k_block, sizeof(k_block), 1, fp);
        // fwrite(&newRowCount, sizeof(newRowCount), 1, fp);
        printf("done!");
        break;
      }

      end = pos;
      pos = start + (end - start) / 2;

    } else if (kd == -1) {
      if (end - start == 0) {
        // fwrite(&k_block, sizeof(k_block), 1, fp);
        // fwrite(&newRowCount, sizeof(newRowCount), 1, fp);
        printf("pos: %ld\n", (ftell(fp) - 4) / 54);
        printf("done!");
        break;
      }

      start = pos;
      pos = start + (end - start) / 2;

    } else {
      printf("Key already exists.\n");
      return (0);
    }

    fseek(fp, 4 + INDEX_BLOCK_SIZE * pos, SEEK_SET);
  }

  fseek(fp, 0, SEEK_SET);
  newRowCount = 1;
  fwrite(&newRowCount, sizeof(newRowCount), 1, fp);

  fclose(fp);

  return (1);
}

int main(int argc, char *argv[]) {
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
    } else {
      printf("Unknown argument: '%s'\n", argv[i]);
      out = 1;
      goto end;
    }
  }

end:
  if (out == 1) printf("Use --h for help.\n");

  return (out);
}
