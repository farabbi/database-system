#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "ntrdb.h"

extern query_type_t query_type;
extern create_t create_info;
extern drop_t drop_info;
extern insert_t insert_info;
extern select_t select_info;

int isnum(char word[]);
int isstring(char word[]);
char *save_string(char *buffer_p, char word[]);

/* zip_query is a DFA, it returns ERROR when error, 
   returns FINISH when query ends, returns INQUERY when query doesn't end */
inquery_t zip_query(char word[])
{
    static state_t state = START;
    static int attr_count = 0;
    static int aggr_count = 0;
    static char *buffer_p;
    
    if (!isstring(word)) {
        int i;
        int len;
        len = strlen(word);
        for (i = 0; i < len; i++) {
            word[i] = tolower(word[i]);
        }
    }
    
    
    if (state == START) {
        if (strcmp(word, "create") == 0) {
            state = CREATE_TABLE_1;
            return INQUERY;
        } else if (strcmp(word, "drop") == 0) {
            state = DROP_TABLE_1;
            return INQUERY;
        } else if (strcmp(word, "insert") == 0) {
            state = INSERT_1;
            return INQUERY;
        } else if (strcmp(word, "select") == 0) {
            attr_count = 0;
            aggr_count = 0;
            state = SELECT_1;
            return INQUERY;
        } else {
            /* maybe we should clean all static variables when return ERROR,
               but finally I decided to clean them before use,
               so here we just reset state */
            state = START;
            return ERROR;
        }
    } else if (state == CREATE_TABLE_1) {
        if (strcmp(word, "table") == 0) {
            state = CREATE_TABLE_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == CREATE_TABLE_2) {
        strcpy(create_info.table_name, word);
        state = CREATE_TABLE_3;
        return INQUERY;
    } else if (state == CREATE_TABLE_3) {
        if (strcmp(word, "(") == 0) {
            attr_count = 0;
            state = CREATE_TABLE_4;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == CREATE_TABLE_4) {
        strcpy(create_info.attr_name[attr_count], word);
        state = CREATE_TABLE_5;
        return INQUERY;
    } else if (state == CREATE_TABLE_5) {
        if (strcmp(word, "int") == 0) {
            create_info.type[attr_count] = INT;
            state = CREATE_TABLE_6;
            return INQUERY;
        } else if (strcmp(word, "varchar") == 0) {
            create_info.type[attr_count] = VARCHAR;
            state = CREATE_TABLE_6;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == CREATE_TABLE_6) {
        if (strcmp(word, ",") == 0) {
            attr_count++;
            state = CREATE_TABLE_4;
            return INQUERY;
        } else if (strcmp(word, ")") == 0) {
            attr_count++;
            create_info.attr_count = attr_count;
            state = CREATE_TABLE_7;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        } 
    } else if (state == CREATE_TABLE_7) {
        if (strcmp(word, ";") == 0) {
            query_type = CREATE_TABLE;
            state = START;
            return FINISH;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == DROP_TABLE_1) {
        if (strcmp(word, "table") == 0) {
            state = DROP_TABLE_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == DROP_TABLE_2) {
        strcpy(drop_info.table_name, word);
        state = DROP_TABLE_3;
        return INQUERY;
    } else if (state == DROP_TABLE_3) {
        if (strcmp(word, ";") == 0) {
            query_type = DROP_TABLE;
            state = START;
            return FINISH;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_1) {
        if (strcmp(word, "into") == 0) {
            state = INSERT_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_2) {
        strcpy(insert_info.table_name, word);
        state = INSERT_3;
        return INQUERY;
    } else if (state == INSERT_3) {
        if (strcmp(word, "values") == 0) {
            state = INSERT_4;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_4) {
        if (strcmp(word, "(") == 0) {
            attr_count = 0;
            buffer_p = insert_info.buffer;
            state = INSERT_5;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_5) {
        if (isnum(word)) {
            /* Assume the num isn't out of the range */
            insert_info.attr_val[attr_count].num = atoi(word);
            insert_info.type[attr_count] = INT;
            state = INSERT_6;
            return INQUERY;
        } else if (isstring(word)) {
            insert_info.attr_val[attr_count].string = buffer_p;
            buffer_p = save_string(buffer_p, word);
            insert_info.type[attr_count] = VARCHAR;
            state = INSERT_6;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_6) {
        if (strcmp(word, ",") == 0) {
            attr_count++;
            state = INSERT_5;
            return INQUERY;
        } else if (strcmp(word, ")") == 0) {
            attr_count++;
            insert_info.attr_count = attr_count;
            state = INSERT_7;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == INSERT_7) {
        if (strcmp(word, ";") == 0) {
            query_type = INSERT;
            state = START;
            return FINISH;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == SELECT_1) {
        if (strcmp(word, "sum") == 0) {
            select_info.aggr_list.aggr_type[aggr_count] = A_SUM;
            state = AGGREGATION_1;
            return INQUERY;
        } else if (strcmp(word, "count") == 0) {
            select_info.aggr_list.aggr_type[aggr_count] = A_COUNT;
            state = AGGREGATION_1;
            return INQUERY;
        } else if (strcmp(word, "avg") == 0) {
            select_info.aggr_list.aggr_type[aggr_count] = A_AVG;
            state = AGGREGATION_1;
            return INQUERY;
        } else if (strcmp(word, "min") == 0) {
            select_info.aggr_list.aggr_type[aggr_count] = A_MIN;
            state = AGGREGATION_1;
            return INQUERY;
        } else if (strcmp(word, "max") == 0) {
            select_info.aggr_list.aggr_type[aggr_count] = A_MAX;
            state = AGGREGATION_1;
            return INQUERY;
        } else {
            strcpy(select_info.select_list.attr_name[attr_count], word);
            state = SELECT_2;
            return INQUERY;
        }
    } else if (state == SELECT_2) {
        if (strcmp(word, ",") == 0) {
            attr_count++;
            state = SELECT_1;
            return INQUERY;
        } else if (strcmp(word, "from") == 0) {
            attr_count++;
            select_info.select_list.attr_count = attr_count;
            select_info.aggr_list.attr_count = aggr_count;
            attr_count = 0;
            state = SELECT_3;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == SELECT_3) {
        /* just reuse attr_count */
        strcpy(select_info.from_list.rel_name[attr_count], word);
        state = SELECT_4;
        return INQUERY;
    } else if (state == SELECT_4) {
        if (strcmp(word, ",") == 0) {
            attr_count++;
            state = SELECT_3;
            return INQUERY;
        } else if (strcmp(word, "where") == 0) {
            attr_count++;
            select_info.from_list.rel_count = attr_count;
            attr_count = 0;
            state = SELECT_5;
            return INQUERY;
        } else if (strcmp(word, ";") == 0) {
            attr_count++;
            select_info.from_list.rel_count = attr_count;
            attr_count = 0;
            select_info.where_conditions.condition_count = attr_count;
            select_info.group_list.attr_count = attr_count;
            query_type = SELECT;
            state = START;
            return FINISH;
        } else if (strcmp(word, "group") == 0) {
            attr_count++;
            select_info.from_list.rel_count = attr_count;
            attr_count = 0;
            select_info.where_conditions.condition_count = attr_count;
            state = GROUP_BY_1;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == SELECT_5) {
        /* just reuse attr_count */
        strcpy(select_info.where_conditions.attr_name[attr_count], word);
        state = SELECT_6;
        return INQUERY;
    } else if (state == SELECT_6) {
        if (strcmp(word, "<") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = LESS;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, "<=") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = LESS_OR_EQUAL;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, ">") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = GREATER;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, ">=") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = GREATER_OR_EQUAL;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, "=") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = EQUAL;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, "!=") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = NOT_EQUAL;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, "like") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], word);
            select_info.where_conditions.op[attr_count] = LIKE;
            state = SELECT_7;
            return INQUERY;
        } else if (strcmp(word, "not") == 0) {
            state = SELECT_6_;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == SELECT_6_) {
        if (strcmp(word, "like") == 0) {
            strcpy(select_info.where_conditions.op_origin[attr_count], "not like");
            select_info.where_conditions.op[attr_count] = NOT_LIKE;
            state = SELECT_7;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == SELECT_7) {
        if (isnum(word)) {
            strcpy(select_info.where_conditions.constant[attr_count], word);
            select_info.where_conditions.type[attr_count] = INT;
            state = SELECT_8;
            return INQUERY;
        } else if (isstring(word)) {
            strcpy(select_info.where_conditions.constant[attr_count], word);
            select_info.where_conditions.type[attr_count] = VARCHAR;
            state = SELECT_8;
            return INQUERY;
        } else {
            /* just reuse constant */
            strcpy(select_info.where_conditions.constant[attr_count], word);
            select_info.where_conditions.type[attr_count] = ATTR;
            state = SELECT_8;
            return INQUERY;
        }
    } else if (state == SELECT_8) {
        if (strcmp(word, "and") == 0) {
            attr_count++;
            state = SELECT_5;
            return INQUERY;
        } else if (strcmp(word, ";") == 0) {
            attr_count++;
            select_info.where_conditions.condition_count = attr_count;
            query_type = SELECT;
            state = START;
            return FINISH;
        } else if (strcmp(word, "group") == 0) {
            attr_count++;
            select_info.where_conditions.condition_count = attr_count;
            attr_count = 0;
            state = GROUP_BY_1;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == AGGREGATION_1) {
        if (strcmp(word, "(") == 0) {
            state = AGGREGATION_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == AGGREGATION_2) {
        strcpy(select_info.aggr_list.attr_name[aggr_count], word);
        strcpy(select_info.select_list.attr_name[attr_count], word);
        state = AGGREGATION_3;
        return INQUERY;
    } else if (state == AGGREGATION_3) {
        if (strcmp(word, ")") == 0) {
            aggr_count++;
            state = SELECT_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == GROUP_BY_1) {
        if (strcmp(word, "by") == 0) {
            state = GROUP_BY_2;
            return INQUERY;
        } else {
            state = START;
            return ERROR;
        }
    } else if (state == GROUP_BY_2) {
        strcpy(select_info.group_list.attr_name[attr_count], word);
        state = GROUP_BY_3;
        return INQUERY;
    } else if (state == GROUP_BY_3) {
        if (strcmp(word, ",") == 0) {
            attr_count++;
            state = GROUP_BY_2;
            return INQUERY;
        } else if (strcmp(word, ";") == 0) {
            attr_count++;
            select_info.group_list.attr_count = attr_count;
            query_type = SELECT;
            state = START;
            return FINISH;
        } else {
            state = START;
            return ERROR;
        }
    } else {
        /* we should never be here */
        state = START;
        return ERROR;
    }
}
