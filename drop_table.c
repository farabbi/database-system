#include <stdio.h>
#include <string.h>
#include "ntrdb.h"

extern drop_t drop_info;

/* drop table, drop *.tbl in db/,
   return 0 on success, return -1 if the file does not exist */
int drop_table(void)
{
    int res;
    char filename[200];
    
    /* drop file */
    strcpy(filename, "db/");
    strcat(filename, drop_info.table_name);
    strcat(filename, ".tbl");
    res = remove(filename);
    if (res == 0) {
        return 0;
    } else {
        return -1;
    }
}
