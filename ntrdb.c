#include <stdio.h>
#include "ntrdb.h"

query_type_t query_type;
create_t create_info;
drop_t drop_info;
insert_t insert_info;
select_t select_info;

int getquery(FILE *queryfile);
int create_table(void);
int drop_table(void);
int insert(void);
int selection(FILE *outputfile);

void renew(void);

/* ntrdb deal with one queryfile each time */
void ntrdb(FILE *queryfile, FILE *outputfile)
{
    int res;
    
getquery:
    //renew();
    res = getquery(queryfile);
    if (res == 1) {
        if (query_type == CREATE_TABLE) {
            res = create_table();
            if (res == 0) {
                fprintf(outputfile, "Successfully created table %s\n", create_info.table_name);
                goto getquery;
            } else if (res == -1) {
                fprintf(outputfile, "Can't create table %s\n", create_info.table_name);
                goto getquery;
            } else if (res == -2) {
                fprintf(outputfile, "Two columns have a same name\n");
                goto getquery;
            } else {
                /* unexpected error */
                goto getquery;
            }
        } else if (query_type == DROP_TABLE) {
            res = drop_table();
            if (res == 0) {
                fprintf(outputfile, "Successfully dropped table %s\n", drop_info.table_name);
                goto getquery;
            } else {
                fprintf(outputfile, "Can't drop table %s\n", drop_info.table_name);
                goto getquery;
            }
        } else if (query_type == INSERT) {
            res = insert();
            if (res == 0) {
                goto getquery;
            } else if (res == -1) {
                fprintf(outputfile, "Table %s doesn't exist\n", insert_info.table_name);
                goto getquery;
            } else if (res == -2) {
                fprintf(outputfile, "Wrong number of columns\n");
                goto getquery;
            } else if (res == -3) {
                fprintf(outputfile, "Value and column type mismatch\n");
                goto getquery;
            } else {
                /* unexpected error */
                goto getquery;
            }
        } else if (query_type == SELECT) {
            res = selection(outputfile);
            if (res == 0) {
                goto getquery;
            } else if (res == -1) {
                fprintf(outputfile, "Too difficult\n");
                goto getquery;
            } else if (res == -2) {
                fprintf(outputfile, "Table %s doesn't exist\n", 
                        select_info.from_list.rel_name[select_info.errorno]);
                goto getquery;
            } else if (res == -3) {
                fprintf(outputfile, "Ambiguous column %s\n", 
                        select_info.select_list.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -4) {
                fprintf(outputfile, "Column %s doesn't exist\n", 
                        select_info.select_list.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -5) {
                fprintf(outputfile, "Predicate %s %s %s error\n", 
                        select_info.where_conditions.attr_name[select_info.errorno],
                        select_info.where_conditions.op_origin[select_info.errorno],
                        select_info.where_conditions.constant[select_info.errorno]);
                goto getquery;
            } else if (res == -7) {
                /* same as -3, but from where conditions */
                fprintf(outputfile, "Ambiguous column %s\n", 
                        select_info.where_conditions.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -8) {
                /* same as -4, but from where conditions */
                fprintf(outputfile, "Column %s doesn't exist\n", 
                        select_info.where_conditions.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -9) {
                fprintf(outputfile, "Join predicata error\n");
                goto getquery;
            } else if (res == -10) {
                fprintf(outputfile, "Ambiguous column %s\n", 
                        select_info.group_list.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -11) {
                fprintf(outputfile, "Column %s doesn't exist\n", 
                        select_info.group_list.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -12) {
                fprintf(outputfile, "Column %s is not int and can't be used in aggregation\n", 
                        select_info.aggr_list.attr_name[select_info.errorno]);
                goto getquery;
            } else if (res == -13) {
                fprintf(outputfile, "Non-group-by column %s in select list\n", 
                        select_info.select_list.attr_name[select_info.errorno]);
                goto getquery;
            } else {
                /* unexpected error */
                fprintf(outputfile, "Unexpected error\n");
                goto getquery;
            }
        } else {
            /* unexpected query_type */
            goto getquery;
        }
    } else if (res == -1) {
        fprintf(outputfile, "Syntax error\n");
        goto getquery;
    } else {
        return;
    }
}

void renew(void)
{
    create_info.attr_count = 0;
    insert_info.attr_count = 0;
    select_info.select_list.attr_count = 0;
    select_info.from_list.rel_count = 0;
    select_info.where_conditions.condition_count = 0;
    select_info.group_list.attr_count = 0;
    select_info.aggr_list.attr_count = 0;
}
