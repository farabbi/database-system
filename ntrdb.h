#ifndef NTRDB
#define NTRDB

/* page struct: metadata | attr | ... | attr | tuple | ... | tuple */

/* length */
#define MAX_NAME_LENGTH 128
#define MAX_ATTR_NUMBER 1024

/* general use */
typedef enum {
    INT = 1,
    VARCHAR = 2,
    ATTR = 3
} type_t;

typedef struct {
    int attr_start;
    int attr_count;
    int tuple_start;
    int tuple_count;
} metadata_t;

typedef struct {
    char attr_name[MAX_NAME_LENGTH];
    char rel_name[MAX_NAME_LENGTH];
    type_t type;
} attr_t;

typedef struct {
    char table_name[MAX_NAME_LENGTH];
    char attr_name[MAX_ATTR_NUMBER][MAX_NAME_LENGTH];
    type_t type[MAX_ATTR_NUMBER];
    int attr_count;
} create_t;

typedef struct {
    char table_name[MAX_NAME_LENGTH];
} drop_t;

typedef union {
    int num;
    char *string;
} union_t;

typedef struct {
    char table_name[MAX_NAME_LENGTH];
    union_t attr_val[MAX_ATTR_NUMBER];
    type_t type[MAX_ATTR_NUMBER];
    int attr_count;
    char buffer[4096];
} insert_t;

typedef struct {
    char attr_name[MAX_ATTR_NUMBER * 2][MAX_NAME_LENGTH];
    int attr_count;
} select_list_t;

typedef struct {
    char rel_name[10][MAX_NAME_LENGTH];
    int rel_count;
} from_list_t;

typedef enum {
    LESS,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
    EQUAL,
    NOT_EQUAL,
    LIKE,
    NOT_LIKE
} op_t;

typedef struct {
    char attr_name[10][MAX_NAME_LENGTH];
    char op_origin[10][10];  //use when print error
    op_t op[10];
    char constant[10][4096];
    type_t type[10];
    int condition_count;
} where_conditions_t;

typedef struct {
    char attr_name[10][MAX_NAME_LENGTH];
    int attr_count;
} group_list_t;

typedef enum {
    A_SUM,
    A_COUNT,
    A_AVG,
    A_MIN,
    A_MAX
} aggregation_type_t;

typedef struct {
    char attr_name[MAX_ATTR_NUMBER * 2][MAX_NAME_LENGTH];
    int attr_count;
    aggregation_type_t aggr_type[MAX_ATTR_NUMBER * 2];
} aggregation_list_t;

typedef struct {
    select_list_t select_list;
    from_list_t from_list;
    where_conditions_t where_conditions;
    group_list_t group_list;
    aggregation_list_t aggr_list;
    int errorno;  //error use
} select_t;

typedef enum {
    START,
    CREATE_TABLE_1,  //check "table"
    CREATE_TABLE_2,  //save table_name
    CREATE_TABLE_3,  //check "("
    CREATE_TABLE_4,  //save attr_name
    CREATE_TABLE_5,  //check and save type
    CREATE_TABLE_6,  //check "," or ")", then goto 4 or 7
    CREATE_TABLE_7,  //check ";", mark query_type as CREATE_TABLE
    DROP_TABLE_1,  //check "table"
    DROP_TABLE_2,  //save table_name
    DROP_TABLE_3,  //check ";", mark query_type as DROP_TABLE
    INSERT_1,  //check "into"
    INSERT_2,  //save table_name
    INSERT_3,  //check "values"
    INSERT_4,  //check "("
    INSERT_5,  //check and save num or string
    INSERT_6,  //check "," or ")", then goto 5 or 7
    INSERT_7,  //check ";", mark query_type as INSERT
    SELECT_1,  //save attr_name, or check and save aggregation type, then goto s_2 or a_1
    SELECT_2,  //check "," or "from", then goto 1 or 3
    SELECT_3,  //save rel_name
    SELECT_4,  //check "," or "where" or ";" or "group", then goto 3 or 5 or finish or g_1
    SELECT_5,  //save attr_name
    SELECT_6,  //save op
    SELECT_6_,  //op(not like) needs this state
    SELECT_7,  //save constant or attr_name
    SELECT_8,  //check "and" or ";" or "group", then goto 5 or finish or g_1
    AGGREGATION_1,  //check "("
    AGGREGATION_2,  //save attr_name
    AGGREGATION_3,  //check ")", then goto s_2
    GROUP_BY_1,  //check "by"
    GROUP_BY_2,  //save attr_name
    GROUP_BY_3  //check "," or ";", then goto 2 or finish
} state_t;

typedef enum {
    CREATE_TABLE,
    DROP_TABLE,
    INSERT,
    SELECT
} query_type_t;

typedef enum {
    FIRST_TIME,
    INQUERY,
    FINISH,
    ERROR
} inquery_t;

/* select use */
typedef struct {
    char origin[2 * MAX_NAME_LENGTH + 1];
    char attr[MAX_NAME_LENGTH];
    char rel[MAX_NAME_LENGTH];
    int attrno;
    type_t type;
    int condno;  //do_selection use this, saving where condition no; join reuse this to save select_fp tableno
} name_t;

#endif
