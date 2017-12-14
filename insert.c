#include <stdio.h>
#include <string.h>
#include "ntrdb.h"

extern insert_t insert_info;
void page_read(char buffer[], int page_num, FILE *fp, int size);
void page_write(char buffer[], int page_num, FILE *fp, int size);

/* insert, insert tuples into *.tbl in db/, 
   return 0 on success, return -1 if the table doesn't exist,
   return -2 if col num wrong, return -3 if col type wrong */
int insert(void)
{
    FILE *fp;
    char filename[200];
    metadata_t metadata;
    attr_t attr;
    
    /* open file */
    strcpy(filename, "db/");
    strcat(filename, insert_info.table_name);
    strcat(filename, ".tbl");
    fp = fopen(filename, "r+b");
    if (fp == NULL) {
        return -1;
    }
    
    /* read metadata and check */
    page_read((char *)&metadata, 0, fp, sizeof(metadata_t));
    if (metadata.attr_count != insert_info.attr_count) {
        return -2;
    }
    
    /* read attr and check */
    int i;
    for (i = 0; i < insert_info.attr_count; i++) {
        page_read((char *)&attr, i + 1, fp, sizeof(attr_t));
        if (attr.type != insert_info.type[i]) {
            return -3;
        }
    }
    
    /* insert values */
    char buffer[4096];
    int *attr_p;
    int string_offset;
    char *string_p;
    attr_p = (int *)buffer;
    string_offset = 4 * insert_info.attr_count;
    string_p = &buffer[string_offset];
    for (i = 0; i < insert_info.attr_count; i++) {
        if (insert_info.type[i] == INT) {
            *attr_p++ = insert_info.attr_val[i].num;
        } else if (insert_info.type[i] == VARCHAR) {
            *attr_p++ = string_offset;
            strcpy(string_p, insert_info.attr_val[i].string);
            string_offset += strlen(insert_info.attr_val[i].string) + 1;
            string_p = &buffer[string_offset];
        }
    }
    page_write(buffer, metadata.tuple_start + metadata.tuple_count, fp, 4096);
    
    /* update metadata */
    metadata.tuple_count++;
    page_write((char *)&metadata, 0, fp, sizeof(metadata_t));
    
    fclose(fp);
    return 0;
}
