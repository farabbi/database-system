#include <stdio.h>
#include <string.h>
#include "ntrdb.h"

extern create_t create_info;
void page_write(char buffer[], int page_num, FILE *fp, int size);

int check_attr(void);

/* create table, create *.tbl in db/, 
   return 0 on success, return -1 if the file already exists,
   return -2 if two attrs have same name */
int create_table(void)
{
    FILE *fp;
    char filename[200];
    metadata_t metadata;
    attr_t attr;
    
    /* check attr name */
    int res;
    res = check_attr();
    if (res == -1) {
        return -2;
    }
    
    /* create file */
    strcpy(filename, "db/");
    strcat(filename, create_info.table_name);
    strcat(filename, ".tbl");
    fp = fopen(filename, "rb");
    if (fp != NULL) {
        return -1;
    } else {
        fp = fopen(filename, "w+b");
    }
    
    /* write metadata */
    metadata.attr_start = 1;
    metadata.attr_count = create_info.attr_count;
    metadata.tuple_start = create_info.attr_count + 1;
    metadata.tuple_count = 0;
    page_write((char *)&metadata, 0, fp, sizeof(metadata_t));
    
    /* write attr pages */
    int i;
    for (i = 0; i < create_info.attr_count; i++) {
        strcpy(attr.attr_name, create_info.attr_name[i]);
        strcpy(attr.rel_name, create_info.table_name);
        attr.type = create_info.type[i];
        page_write((char *)&attr, i + 1, fp, sizeof(attr_t));
    }
    
    fclose(fp);
    return 0;
}

/* check attr name, return 0 on success, return -1 on error */
int check_attr(void)
{
    int i, j, res;
    
    /* check same name */
    for (i = 0; i < create_info.attr_count; i++) {
        for (j = i + 1; j < create_info.attr_count; j++) {
            res = strcmp(create_info.attr_name[i], create_info.attr_name[j]);
            if (res == 0) {
                return -1;
            }
        }
    }
    
    return 0;
}
