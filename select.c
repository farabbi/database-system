#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ntrdb.h"

extern select_t select_info;
void page_read(char buffer[], int page_num, FILE *fp, int size);
void page_write(char buffer[], int page_num, FILE *fp, int size);
char *save_string(char *buffer_p, char word[]);

int full_name(char *name);
void break_full_name(name_t *name);
int fill_name(name_t *name, char *attr_name);
FILE *do_selection(int nameno, int tableno);
FILE *do_join(FILE *fp_b, FILE *fp_s);
void do_projection(FILE *f, FILE *outputfile);
void group_and_aggr(FILE *src, FILE *dst);
void group_only(FILE *src, FILE *dst);
void aggr_only(FILE *src, FILE *dst);

unsigned hash(union_t key, type_t type);
int build_hashtab(FILE *fp, int attrno, type_t type);
void probe(FILE *fp_b, FILE *fp_s, int attrno, type_t type, FILE *output);
void free_hashtab(type_t type);
void copy_name(name_t *nd, name_t *ns);
int same_name(name_t *nd, name_t *ns);
void add_prefix(char *origin, char *prefix);
struct nlist *group_lookupbykey(union_t key, type_t type, int aggrno);
struct nlist *group_lookupbytuple(int tupleno, int aggrno);

name_t select_name[MAX_ATTR_NUMBER * 2];
name_t where_name[2];
name_t join_name[2];
name_t group_name[1];
name_t aggr_name[MAX_ATTR_NUMBER * 2];

FILE *fp[2];
metadata_t metadata[2];

int join;

/* select, return 0 on success, return -1 if the query is too difficult,
   return -2 if table doesn't exist, return -3 if attr name is ambiguous,
   return -4 if attr name doesn't exist, return -5 if where type mismatch,
   others return -6, return -7/-8 same as -3/-4 but from where,
   return -9 if join type mismatch, return -10/-11 same as -3/-4 but from group,
   return -12 if * not in count or not int, return -13 non-group-column */
int selection(FILE *outputfile)
{
    int i, j, k, res;
    char filename[200];
   
    /* check rel_count */
    if (select_info.from_list.rel_count == 1) {
        /* maybe we can do somthing here */
        join = 0;
    } else if (select_info.from_list.rel_count == 2) {
        /* maybe we can do somthing here */
        join = 1;
    } else {
        return -1;
    }
    
    /* check group count */
    if (select_info.group_list.attr_count > 1) {
        return -1;
    }
    
    /* open file */
    for (i = 0; i < select_info.from_list.rel_count; i++) {
        strcpy(filename, "db/");
        strcat(filename, select_info.from_list.rel_name[i]);
        strcat(filename, ".tbl");
        fp[i] = fopen(filename, "r+b");
        if (fp[i] == NULL) {
            select_info.errorno = i;
            return -2;
        }
    }
    
    /* read metadata */
    for (i = 0; i < select_info.from_list.rel_count; i++) {
        page_read((char *)&metadata[i], 0, fp[i], sizeof(metadata_t));
    }
    
    /* check attr_name and save attrno */
    for (i = 0; i < select_info.select_list.attr_count; i++) {
        if (strcmp(select_info.select_list.attr_name[i], "*") == 0) {
            strcpy(select_name[i].origin, "*");
        } else {
            res = fill_name(&select_name[i], select_info.select_list.attr_name[i]);
            if (res != 0) {
                select_info.errorno = i;
                return res;
            }
        }
    }
    
    /* check where conditions */
    if (join == 0) {
        if (select_info.where_conditions.condition_count > 1) {
            return -1;
        }
    } else {
        if (select_info.where_conditions.condition_count > 3 
         || select_info.where_conditions.condition_count < 1) {
            return -1;
        }
    }
    int where_name_count = 0;
    for (i = 0; i < select_info.where_conditions.condition_count; i++) {
        type_t type;
        type = select_info.where_conditions.type[i];
        if (type == INT) {
            if (select_info.where_conditions.op[i] == LESS 
             || select_info.where_conditions.op[i] == LESS_OR_EQUAL 
             || select_info.where_conditions.op[i] == GREATER 
             || select_info.where_conditions.op[i] == GREATER_OR_EQUAL 
             || select_info.where_conditions.op[i] == EQUAL 
             || select_info.where_conditions.op[i] == NOT_EQUAL) {
                res = fill_name(&where_name[where_name_count], select_info.where_conditions.attr_name[i]);
                if (res != 0) {
                    select_info.errorno = i;
                    return res - 4;
                }
                if (where_name[where_name_count].type != type) {
                    select_info.errorno = i;
                    return -5;
                }
                where_name[where_name_count].condno = i;
                where_name_count++;
            } else {
                select_info.errorno = i;
                return -5;
            }
        } else if (type == VARCHAR) {
            if (select_info.where_conditions.op[i] == EQUAL
             || select_info.where_conditions.op[i] == NOT_EQUAL
             || select_info.where_conditions.op[i] == LIKE
             || select_info.where_conditions.op[i] == NOT_LIKE) {
                res = fill_name(&where_name[where_name_count], select_info.where_conditions.attr_name[i]);
                if (res != 0) {
                    select_info.errorno = i;
                    return res - 4;
                }
                if (where_name[where_name_count].type != type) {
                    select_info.errorno = i;
                    return -5;
                }
                where_name[where_name_count].condno = i;
                where_name_count++;
            } else {
                select_info.errorno = i;
                return -5;
            }
        } else if (type == ATTR) {
            if (select_info.where_conditions.op[i] == EQUAL) {
                res = fill_name(&join_name[0], select_info.where_conditions.attr_name[i]);
                if (res != 0) {
                    select_info.errorno = i;
                    return res - 4;
                }
                res = fill_name(&join_name[1], select_info.where_conditions.constant[i]);
                if (res != 0) {
                    select_info.errorno = i;
                    return res - 4;
                }
                if (join_name[0].type != join_name[1].type) {
                    return -9;
                }
            } else {
                select_info.errorno = i;
                return -5;
            }
        } else {
            /* unknown type */
            select_info.errorno = i;
            return -5;
        }
    }
    for (i = where_name_count; i < select_info.from_list.rel_count; i++) {
        where_name[i].attrno = -1;
    }

    /* check group again */
    for (i = 0; i < select_info.group_list.attr_count; i++) {
        res = fill_name(&group_name[i], select_info.group_list.attr_name[i]);
        if (res != 0) {
            select_info.errorno = i;
            return res - 7;
        }
    }
    for (i = 0; i < select_info.aggr_list.attr_count; i++) {
        if (strcmp(select_info.aggr_list.attr_name[i], "*") == 0) {
            if (select_info.aggr_list.aggr_type[i] == A_COUNT) {
                strcpy(aggr_name[i].origin, "*");
            } else {
                select_info.errorno = i;
                return -12;
            }
        } else {
            res = fill_name(&aggr_name[i], select_info.aggr_list.attr_name[i]);
            if (res != 0) {
                select_info.errorno = i;
                return res -7;
            }
            if (aggr_name[i].type != INT) {
                if (select_info.aggr_list.aggr_type[i] != A_COUNT) {
                    select_info.errorno = i;
                    return -12;
                }
            }
        }
    }
    for (i = 0; i < select_info.group_list.attr_count; i++) {
        for (j = 0; j < select_info.select_list.attr_count; j++) {
            if (!same_name(&group_name[i], &select_name[j])) {
                for (k = 0; k < select_info.aggr_list.attr_count; k++) {
                    if (same_name(&select_name[j], &aggr_name[k])) {
                        goto ok;
                    }
                }
                select_info.errorno = j;
                return -13;
            }
            ok:
            continue;
        }
    }
    
    /* selection, one condition on one table */
    FILE *select_fp[2];
    /* check order */
    if (select_info.from_list.rel_count == 2 && strcmp(where_name[0].rel, select_info.from_list.rel_name[1]) == 0) {
        name_t temp;
        copy_name(&temp, &where_name[0]);
        copy_name(&where_name[0], &where_name[1]);
        copy_name(&where_name[1], &temp);
    }
    for (i = 0; i < select_info.from_list.rel_count; i++) {
        if (where_name[i].attrno == -1) {
            select_fp[i] = fp[i];
        } else {
            select_fp[i] = do_selection(i, i);
            if (select_fp[i] == NULL) {
                return -6;
            }
        }
    }
    
    /* join */
    FILE *join_fp;
    if (join == 1) {
        for (i = 0; i < 2; i++) {
            for (j = 0; j < 2; j++) {
                if (strcmp(join_name[i].rel, select_info.from_list.rel_name[j]) == 0) {
                    join_name[i].condno = j;
                }
            }
        }
        /* condno is tableno, big table in front */
        metadata_t m[2];
        page_read((char *)&m[0], 0, select_fp[join_name[0].condno], sizeof(metadata_t));
        page_read((char *)&m[1], 0, select_fp[join_name[1].condno], sizeof(metadata_t));
        if (m[0].tuple_count < m[1].tuple_count) {
            name_t temp;
            copy_name(&temp, &join_name[1]);
            copy_name(&join_name[1], &join_name[0]);
            copy_name(&join_name[0], &temp);
        }
        join_fp = do_join(select_fp[join_name[0].condno], select_fp[join_name[1].condno]);
        if (join_fp == NULL) {
            return -6;
        }
    } else {
        join_fp = select_fp[0];
    }
    
    /* group by and aggregation */
    /* we can't handle two aggregation to one col at the same time */
    /* edit origin */
    for (i = 0; i < select_info.aggr_list.attr_count; i++) {
        if (select_info.aggr_list.aggr_type[i] == A_SUM) {
            add_prefix(aggr_name[i].origin, "SUM");
        } else if (select_info.aggr_list.aggr_type[i] == A_COUNT) {
            add_prefix(aggr_name[i].origin, "COUNT");
        } else if (select_info.aggr_list.aggr_type[i] == A_AVG) {
            add_prefix(aggr_name[i].origin, "AVG");
        } else if (select_info.aggr_list.aggr_type[i] == A_MIN) {
            add_prefix(aggr_name[i].origin, "MIN");
        } else if (select_info.aggr_list.aggr_type[i] == A_MAX) {
            add_prefix(aggr_name[i].origin, "MAX");
        }
    }
    if (select_info.group_list.attr_count != 0 && select_info.aggr_list.attr_count != 0) {
        group_and_aggr(join_fp, outputfile);
    } else if (select_info.group_list.attr_count != 0 && select_info.aggr_list.attr_count == 0) {
        group_only(join_fp, outputfile);
    } else if (select_info.group_list.attr_count == 0 && select_info.aggr_list.attr_count != 0) {
        aggr_only(join_fp, outputfile);
    }
    
    /* projection */
    if (select_info.group_list.attr_count == 0 && select_info.aggr_list.attr_count == 0) {
        do_projection(join_fp, outputfile);
    }
    
    /* close file */
    for (i = 0; i < select_info.from_list.rel_count; i++) {
        fclose(fp[i]);
    }
    for (i = 0; i < select_info.from_list.rel_count; i++) {
        if (where_name[i].attrno != -1) {
            fclose(select_fp[i]);
        }
    }
    if (join == 1) {
        fclose(join_fp);
    }
    
    return 0;
}

/* project fp into outputfile */
void do_projection(FILE *f, FILE *outputfile)
{
    int row_count = 0;

    metadata_t m;
    attr_t attr;
    page_read((char *)&m, 0, f, sizeof(metadata_t));

    /* table header */
    int i, j, k;
    for (i = 0; i < select_info.select_list.attr_count; i++) {
        if (strcmp(select_name[i].origin, "*") == 0) {
            for (j = 0; j < m.attr_count; j++) {
                page_read((char *)&attr, m.attr_start + j, f, sizeof(attr_t));
                fprintf(outputfile, "%s.%s", attr.rel_name, attr.attr_name);
                fprintf(outputfile, "|");
            }
        } else {
            fprintf(outputfile, "%s", select_name[i].origin);
            fprintf(outputfile, "|");
        }
    }
    fprintf(outputfile, "\n");
    
    /* table data */
    char buffer[4096];
    char buffer_s[4096];
    int *v;
    for (k = 0; k < m.tuple_count; k++) {
        if (join == 0) {
            page_read(buffer, m.tuple_start + k, f, 4096);
            for (i = 0; i < select_info.select_list.attr_count; i++) {
                if (strcmp(select_name[i].origin, "*") == 0) {
                    for (j = 0; j < m.attr_count; j++) {
                        page_read((char *)&attr, m.attr_start + j, f, sizeof(attr_t));
                        v = (int *)&buffer[4 * j];
                        if (attr.type == INT) {
                            fprintf(outputfile, "%d", *v);
                        } else if (attr.type == VARCHAR) {
                            fprintf(outputfile, "%s", &buffer[*v]);
                        }
                        fprintf(outputfile, "|");
                    }
                } else {
                    v = (int *)&buffer[4 * select_name[i].attrno];
                    if (select_name[i].type == INT) {
                        fprintf(outputfile, "%d", *v);
                    } else if (select_name[i].type == VARCHAR) {
                        fprintf(outputfile, "%s", &buffer[*v]);
                    }
                    fprintf(outputfile, "|");
                }
            }
            fprintf(outputfile, "\n");
            row_count++;
        } else {
            page_read(buffer, m.tuple_start + 2 * k, f, 4096);
            page_read(buffer_s, m.tuple_start + 2 * k + 1, f, 4096);
            for (i = 0; i < select_info.select_list.attr_count; i++) {
                if (strcmp(select_name[i].origin, "*") == 0) {
                    int count = 0;
                    for (j = 0; j < m.attr_count; j++) {
                        page_read((char *)&attr, m.attr_start + j, f, sizeof(attr_t));
                        /* I'm sorry but it is too difficult to deal with */
                        if (strcmp(attr.rel_name, join_name[0].rel) == 0) {
                            count++;
                            v = (int *)&buffer[4 * j];
                            if (attr.type == INT) {
                                fprintf(outputfile, "%d", *v);
                            } else if (attr.type == VARCHAR) {
                                fprintf(outputfile, "%s", &buffer[*v]);
                            }
                            fprintf(outputfile, "|");
                        } else {
                            v = (int *)&buffer_s[4 * (j-count)];
                            if (attr.type == INT) {
                                fprintf(outputfile, "%d", *v);
                            } else if (attr.type == VARCHAR) {
                                fprintf(outputfile, "%s", &buffer_s[*v]);
                            }
                            fprintf(outputfile, "|");
                        }
                    }
                } else {
                    if (strcmp(select_name[i].rel, join_name[0].rel) == 0) {
                        v = (int *)&buffer[4 * select_name[i].attrno];
                        if (select_name[i].type == INT) {
                            fprintf(outputfile, "%d", *v);
                        } else if (select_name[i].type == VARCHAR) {
                            fprintf(outputfile, "%s", &buffer[*v]);
                        }
                        fprintf(outputfile, "|");
                    } else {
                        v = (int *)&buffer_s[4 * select_name[i].attrno];
                        if (select_name[i].type == INT) {
                            fprintf(outputfile, "%d", *v);
                        } else if (select_name[i].type == VARCHAR) {
                            fprintf(outputfile, "%s", &buffer_s[*v]);
                        }
                        fprintf(outputfile, "|");
                    }
                }
            }
            fprintf(outputfile, "\n");
            row_count++;
        }
    }
    
    if (row_count > 1) {
        fprintf(outputfile, "(%d rows)\n", row_count);
    } else {
        fprintf(outputfile, "(%d row)\n", row_count);
    }
}

/* join f1, f2, return join_fp, big table in front */
FILE *do_join(FILE *fp_b, FILE *fp_s)
{
    FILE *temp;
    temp = tmpfile();
    if (temp == NULL) {
        return temp;
    }
    
    metadata_t m_b;
    page_read((char *)&m_b, 0, fp_b, sizeof(metadata_t));
    metadata_t m_s;
    page_read((char *)&m_s, 0, fp_s, sizeof(metadata_t));
    
    metadata_t m;
    m.attr_start = 1;
    m.attr_count = m_b.attr_count;
    m.tuple_start = m_b.tuple_start;
    m.tuple_count = 0;
    m.attr_count += m_s.attr_count;
    m.tuple_start += m_s.attr_count;
    page_write((char *)&m, 0, temp, sizeof(metadata_t));
    
    attr_t attr;
    int i;
    for (i = 0; i < m_b.attr_count; i++) {
        page_read((char *)&attr, m_b.attr_start + i, fp_b, sizeof(attr_t));
        page_write((char *)&attr, m.attr_start + i, temp, sizeof(attr_t));
    }
    for (i = 0; i < m_s.attr_count; i++) {
        page_read((char *)&attr, m_s.attr_start + i, fp_s, sizeof(attr_t));
        page_write((char *)&attr, m.attr_start + m_b.attr_count + i, temp, sizeof(attr_t));
    }
    
    /* build hashtable */
    int res;
    res = build_hashtab(fp_s, join_name[1].attrno, join_name[1].type);
    if (res != 0) {
        return NULL;
    }
    
    /* hash, join and save results */
    probe(fp_b, fp_s, join_name[0].attrno, join_name[0].type, temp);
    
    /* free hashtable */
    free_hashtab(join_name[1].type);
    
    return temp;
}

/* select tableno with nameno, return selected file */
FILE *do_selection(int nameno, int tableno)
{
    FILE *temp;
    temp = tmpfile();
    if (temp == NULL) {
        return temp;
    }
    
    metadata_t m;
    m.attr_start = 1;
    m.attr_count = metadata[tableno].attr_count;
    m.tuple_start = metadata[tableno].tuple_start;
    m.tuple_count = 0;
    
    attr_t attr;
    int i;
    for (i = 0; i < metadata[tableno].attr_count; i++) {
        page_read((char *)&attr, metadata[tableno].attr_start + i, fp[tableno], sizeof(attr_t));
        page_write((char *)&attr, m.attr_start + i, temp, sizeof(attr_t));
    }
    
    char buffer[4096];
    if (where_name[nameno].type == INT) {
        int constant;
        constant = atoi(select_info.where_conditions.constant[where_name[nameno].condno]);
        int *value;
        value = (int *)&buffer[4 * where_name[nameno].attrno];
        if (select_info.where_conditions.op[where_name[nameno].condno] == LESS) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value < constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == LESS_OR_EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value <= constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == GREATER) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value > constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == GREATER_OR_EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value >= constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value == constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == NOT_EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (*value != constant) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else {
            /* unexpected op */
            return NULL;
        }
    } else if (where_name[nameno].type == VARCHAR) {
        char constant[4096];
        save_string(constant, select_info.where_conditions.constant[where_name[nameno].condno]);
        int *offset;
        offset = (int *)&buffer[4 * where_name[nameno].attrno];
        if (select_info.where_conditions.op[where_name[nameno].condno] == EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (strcmp(constant, &buffer[*offset]) == 0) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == NOT_EQUAL) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (strcmp(constant, &buffer[*offset]) != 0) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == LIKE) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (strstr(&buffer[*offset], constant) != NULL) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else if (select_info.where_conditions.op[where_name[nameno].condno] == NOT_LIKE) {
            for (i = 0; i < metadata[tableno].tuple_count; i++) {
                page_read(buffer, metadata[tableno].tuple_start + i, fp[tableno], 4096);
                if (strstr(&buffer[*offset], constant) == NULL) {
                    page_write(buffer, m.tuple_start + m.tuple_count, temp, 4096);
                    m.tuple_count++;
                }
            }
        } else {
            /* unexpected op */
            return NULL;
        }
    } else {
        /* unexpected type */
        return NULL;
    }

    page_write((char *)&m, 0, temp, sizeof(metadata_t));
    return temp;
}

/* check and fill attr name info, return 0 on success */
int fill_name(name_t *name, char *attr_name)
{
    int i, j;
    attr_t attr;
    
    name->attrno = -1;
    strcpy(name->origin, attr_name);
    
    /* check full_name */
    if (full_name(name->origin)) {
        break_full_name(name);
        for (i = 0; i < select_info.from_list.rel_count; i++) {
            if (strcmp(name->rel, select_info.from_list.rel_name[i]) == 0) {
                for (j = 0; j < metadata[i].attr_count; j++) {
                    page_read((char *)&attr, metadata[i].attr_start + j, fp[i], sizeof(attr_t));
                    if (strcmp(attr.attr_name, name->attr) == 0) {
                        name->type = attr.type;
                        name->attrno = j;
                    }
                }
            }
        }
    } else {
        strcpy(name->attr, attr_name);
        for (i = 0; i < select_info.from_list.rel_count; i++) {
            for (j = 0; j < metadata[i].attr_count; j++) {
                page_read((char *)&attr, metadata[i].attr_start + j, fp[i], sizeof(attr_t));
                if (strcmp(attr.attr_name, name->attr) == 0) {
                    if (name->attrno == -1) {
                        strcpy(name->rel, attr.rel_name);
                        name->type = attr.type;
                        name->attrno = j;
                    } else {
                        return -3;
                    }
                }
            }
        }
    }

    if (name->attrno == -1) {
        return -4;
    }
    
    return 0;
}

/* break full name into rel_name and attr_name and save them in name_t */
void break_full_name(name_t *name)
{
    int i, j;
    i = 0;
    j = 0;
    while (name->origin[i] != '.') {
        name->rel[j++] = name->origin[i++];
    }
    name->rel[j] = '\0';
    i++;
    j = 0;
    while (name->origin[i] != '\0') {
        name->attr[j++] = name->origin[i++];
    }
    name->attr[j] = '\0';
}

/* check full name, return 1 on true, return 0 on false */
int full_name(char *name)
{
    /* you can't use '.' anywhere except for full name, I think */
    while (*name != '\0') {
        if (*name == '.') {
            return 1;
        }
        name++;
    }
    return 0;
}

/* simple hash join */
struct nlist {
    struct nlist *next;
    int tupleno;
    int aggrno;
    union_t key;
    type_t type;
    int a_sum;
    int a_count;
    int a_min;
    int a_max;
};

#define HASHSIZE 101

static struct nlist *hashtab[HASHSIZE];

/* hash function */
unsigned hash(union_t key, type_t type)
{
    unsigned hashval;
    
    if (type == VARCHAR) {
        for (hashval = 0; *key.string != '\0'; key.string++) {
            hashval = *key.string + 31 * hashval;
        }
    } else {
        hashval = key.num;
    }
    return hashval % HASHSIZE;
}

/* group lookup by key */
struct nlist *group_lookupbykey(union_t key, type_t type, int aggrno)
{
    struct nlist *np;
    
    for (np = hashtab[hash(key, type)]; np != NULL; np = np->next) {
        if (type == INT) {
            if (key.num == np->key.num && np->aggrno == aggrno) {
                return np;
            }
        } else {
            if (strcmp(key.string, np->key.string) == 0 && np->aggrno == aggrno) {
                return np;
            }
        }
    }
    return NULL;
}

/* group lookup by tupleno */
struct nlist *group_lookupbytuple(int tupleno, int aggrno)
{
    struct nlist *np;
    
    int i;
    for (i = 0; i < HASHSIZE; i++) {
        for (np = hashtab[i]; np != NULL; np = np->next) {
            if (np->tupleno == tupleno && np->aggrno == aggrno) {
                return np;
            }
        }
    }
    return NULL;
}

/* group by and aggregation */
void group_and_aggr(FILE *src, FILE *dst)
{
    int row_count = 0;
    int i, j;
    
    metadata_t m_src;
    page_read((char *)&m_src, 0, src, sizeof(metadata_t));
    
    /* table head */
    fprintf(dst, "%s", group_name[0].origin);
    fprintf(dst, "|");
    for (i = 0; i < select_info.aggr_list.attr_count; i++) {
        fprintf(dst, "%s", aggr_name[i].origin);
        fprintf(dst, "|");
    }
    fprintf(dst, "\n");
    
    /* do aggregation */
    int *g;
    int *v;
    unsigned hashval;
    union_t key;
    type_t type;
    type = group_name[0].type;
    struct nlist *np;
    char buffer[4096];
    char buffer_s[4096];
    if (join == 0) {
        g = (int *)&buffer[4 * group_name[0].attrno];
    } else {
        if (strcmp(group_name[0].rel, join_name[0].rel) == 0) {
            g = (int *)&buffer[4 * group_name[0].attrno];
        } else {
            g = (int *)&buffer_s[4 * group_name[0].attrno];
        }
    }
    /* tuple one by one */
    for (i = 0; i < m_src.tuple_count; i++) {
        if (join == 0) {
            page_read(buffer, m_src.tuple_start + i, src, 4096);
        } else {
            page_read(buffer, m_src.tuple_start + 2 * i, src, 4096);
            page_read(buffer_s, m_src.tuple_start + 2 * i + 1, src, 4096);
        }
        if (type == INT) {
            key.num = *g;
        } else {
            if (join == 0) {
                key.string = &buffer[*g];
            } else {
                key.string = &buffer_s[*g];
            }
        }
        /* aggr one by one */
        for (j = 0; j < select_info.aggr_list.attr_count; j++) {
            if ((np = group_lookupbykey(key, type, j)) == NULL) {
                /* init */
                np = (struct nlist *) malloc(sizeof(*np));
                if (np == NULL) {
                    fprintf(dst, "unexpected error\n");
                    return;
                }
                np->tupleno = i;
                np->aggrno = j;
                if (type == INT) {
                    np->key.num = key.num;
                } else {
                    np->key.string = strdup(key.string);
                    if (np->key.string == NULL) {
                        fprintf(dst, "unexpected error\n");
                        return;
                    }
                }
                np->a_sum = 0;
                np->a_count = 0;
                np->a_min = 1e9;
                np->a_max = -1e9;
                hashval = hash(key, type);
                np->next = hashtab[hashval];
                hashtab[hashval] = np;
            }
            if (join == 0) {
                v = (int *)&buffer[4 * aggr_name[j].attrno];
            } else {
                if (strcmp(aggr_name[j].rel, join_name[0].rel) == 0) {
                    v = (int *)&buffer[4 * aggr_name[j].attrno];
                } else {
                    v = (int *)&buffer_s[4 * aggr_name[j].attrno];
                }
            }
            if (select_info.aggr_list.aggr_type[j] == A_SUM) {
                np->a_sum += *v;
            } else if (select_info.aggr_list.aggr_type[j] == A_COUNT) {
                np->a_count += 1;
            } else if (select_info.aggr_list.aggr_type[j] == A_AVG) {
                np->a_sum += *v;
                np->a_count += 1;
            } else if (select_info.aggr_list.aggr_type[j] == A_MIN) {
                if (np->a_min > *v) {
                    np->a_min = *v;
                }
            } else if (select_info.aggr_list.aggr_type[j] == A_MAX) {
                if (np->a_max < *v) {
                    np->a_max = *v;
                }
            } else {
                /* unexpected type */
                fprintf(dst, "unexpected error\n");
                return;
            }
        }
    }
    
    /* table data */
    for (i = 0; i < m_src.tuple_count; i++) {
        for (j = 0; j < select_info.aggr_list.attr_count; j++) {
            np = group_lookupbytuple(i, j);
            if (np == NULL) {
                continue;
            }
            if (j == 0) {
                if (type == VARCHAR) {
                    fprintf(dst, "%s", np->key.string);
                } else {
                    fprintf(dst, "%d", np->key.num);
                }
                fprintf(dst, "|");
            }
            if (select_info.aggr_list.aggr_type[j] == A_SUM) {
                fprintf(dst, "%d", np->a_sum);
                fprintf(dst, "|");
            } else if (select_info.aggr_list.aggr_type[j] == A_COUNT) {
                fprintf(dst, "%d", np->a_count);
                fprintf(dst, "|");
            } else if (select_info.aggr_list.aggr_type[j] == A_AVG) {
                fprintf(dst, "%d", np->a_sum / np->a_count);
                fprintf(dst, "|");
            } else if (select_info.aggr_list.aggr_type[j] == A_MIN) {
                fprintf(dst, "%d", np->a_min);
                fprintf(dst, "|");
            } else if (select_info.aggr_list.aggr_type[j] == A_MAX) {
                fprintf(dst, "%d", np->a_max);
                fprintf(dst, "|");
            }
            if (j == select_info.aggr_list.attr_count - 1) {
                fprintf(dst, "\n");
                row_count++;
            }
        }
    }
    
    free_hashtab(type);
    
    if (row_count > 1) {
        fprintf(dst, "(%d rows)\n", row_count);
    } else {
        fprintf(dst, "(%d row)\n", row_count);
    }
}

/* group only */
void group_only(FILE *src, FILE *dst)
{
    int row_count = 0;
    int i;
    
    metadata_t m_src;
    page_read((char *)&m_src, 0, src, sizeof(metadata_t));
    
    /* table head */
    fprintf(dst, "%s", group_name[0].origin);
    fprintf(dst, "|");
    fprintf(dst, "\n");
    
    /* do aggregation */
    int *g;
    unsigned hashval;
    union_t key;
    type_t type;
    type = group_name[0].type;
    struct nlist *np;
    char buffer[4096];
    char buffer_s[4096];
    if (join == 0) {
        g = (int *)&buffer[4 * group_name[0].attrno];
    } else {
        if (strcmp(group_name[0].rel, join_name[0].rel) == 0) {
            g = (int *)&buffer[4 * group_name[0].attrno];
        } else {
            g = (int *)&buffer_s[4 * group_name[0].attrno];
        }
    }
    /* tuple one by one */
    for (i = 0; i < m_src.tuple_count; i++) {
        if (join == 0) {
            page_read(buffer, m_src.tuple_start + i, src, 4096);
        } else {
            page_read(buffer, m_src.tuple_start + 2 * i, src, 4096);
            page_read(buffer_s, m_src.tuple_start + 2 * i + 1, src, 4096);
        }
        if (type == INT) {
            key.num = *g;
        } else {
            if (join == 0) {
                key.string = &buffer[*g];
            } else {
                key.string = &buffer_s[*g];
            }
        }
        if ((np = group_lookupbykey(key, type, 0)) == NULL) {
            /* init */
            np = (struct nlist *) malloc(sizeof(*np));
            if (np == NULL) {
                fprintf(dst, "unexpected error\n");
                return;
            }
            np->tupleno = i;
            np->aggrno = 0;
            if (type == INT) {
                np->key.num = key.num;
            } else {
                np->key.string = strdup(key.string);
                if (np->key.string == NULL) {
                    fprintf(dst, "unexpected error\n");
                    return;
                }
            }
            np->a_sum = 0;
            np->a_count = 0;
            np->a_min = 1e9;
            np->a_max = -1e9;
            hashval = hash(key, type);
            np->next = hashtab[hashval];
            hashtab[hashval] = np;
        }
    }
    
    /* table data */
    for (i = 0; i < m_src.tuple_count; i++) {
        np = group_lookupbytuple(i, 0);
        if (np == NULL) {
            continue;
        }
        if (type == VARCHAR) {
            fprintf(dst, "%s", np->key.string);
        } else {
            fprintf(dst, "%d", np->key.num);
        }
        fprintf(dst, "|");
        fprintf(dst, "\n");
        row_count++;
    }
    
    free_hashtab(type);
    
    if (row_count > 1) {
        fprintf(dst, "(%d rows)\n", row_count);
    } else {
        fprintf(dst, "(%d row)\n", row_count);
    }
}

/* aggregation only */
void aggr_only(FILE *src, FILE *dst)
{
    int row_count = 0;
    int i, j;
    
    metadata_t m_src;
    page_read((char *)&m_src, 0, src, sizeof(metadata_t));
    
    /* table head */
    for (i = 0; i < select_info.aggr_list.attr_count; i++) {
        fprintf(dst, "%s", aggr_name[i].origin);
        fprintf(dst, "|");
    }
    fprintf(dst, "\n");
    
    /* do aggregation */
    int *v;
    unsigned hashval;
    union_t key;
    key.num = 0;
    type_t type;
    type = INT;
    struct nlist *np;
    char buffer[4096];
    char buffer_s[4096];
    /* tuple one by one */
    for (i = 0; i < m_src.tuple_count; i++) {
        if (join == 0) {
            page_read(buffer, m_src.tuple_start + i, src, 4096);
        } else {
            page_read(buffer, m_src.tuple_start + 2 * i, src, 4096);
            page_read(buffer_s, m_src.tuple_start + 2 * i + 1, src, 4096);
        }
        /* aggr one by one */
        for (j = 0; j < select_info.aggr_list.attr_count; j++) {
            if ((np = group_lookupbykey(key, type, j)) == NULL) {
                /* init */
                np = (struct nlist *) malloc(sizeof(*np));
                if (np == NULL) {
                    fprintf(dst, "unexpected error\n");
                    return;
                }
                np->tupleno = 0;
                np->aggrno = j;
                if (type == INT) {
                    np->key.num = key.num;
                } else {
                    np->key.string = strdup(key.string);
                    if (np->key.string == NULL) {
                        fprintf(dst, "unexpected error\n");
                        return;
                    }
                }
                np->a_sum = 0;
                np->a_count = 0;
                np->a_min = 1e9;
                np->a_max = -1e9;
                hashval = hash(key, type);
                np->next = hashtab[hashval];
                hashtab[hashval] = np;
            }
            if (join == 0) {
                v = (int *)&buffer[4 * aggr_name[j].attrno];
            } else {
                if (strcmp(aggr_name[j].rel, join_name[0].rel) == 0) {
                    v = (int *)&buffer[4 * aggr_name[j].attrno];
                } else {
                    v = (int *)&buffer_s[4 * aggr_name[j].attrno];
                }
            }
            if (select_info.aggr_list.aggr_type[j] == A_SUM) {
                np->a_sum += *v;
            } else if (select_info.aggr_list.aggr_type[j] == A_COUNT) {
                np->a_count += 1;
            } else if (select_info.aggr_list.aggr_type[j] == A_AVG) {
                np->a_sum += *v;
                np->a_count += 1;
            } else if (select_info.aggr_list.aggr_type[j] == A_MIN) {
                if (np->a_min > *v) {
                    np->a_min = *v;
                }
            } else if (select_info.aggr_list.aggr_type[j] == A_MAX) {
                if (np->a_max < *v) {
                    np->a_max = *v;
                }
            } else {
                /* unexpected type */
                fprintf(dst, "unexpected error\n");
                return;
            }
        }
    }
    
    /* table data */
    for (j = 0; j < select_info.aggr_list.attr_count; j++) {
        np = group_lookupbytuple(0, j);
        if (np == NULL) {
            continue;
        }
        if (select_info.aggr_list.aggr_type[j] == A_SUM) {
            fprintf(dst, "%d", np->a_sum);
            fprintf(dst, "|");
        } else if (select_info.aggr_list.aggr_type[j] == A_COUNT) {
            fprintf(dst, "%d", np->a_count);
            fprintf(dst, "|");
        } else if (select_info.aggr_list.aggr_type[j] == A_AVG) {
            fprintf(dst, "%d", np->a_sum / np->a_count);
            fprintf(dst, "|");
        } else if (select_info.aggr_list.aggr_type[j] == A_MIN) {
            fprintf(dst, "%d", np->a_min);
            fprintf(dst, "|");
        } else if (select_info.aggr_list.aggr_type[j] == A_MAX) {
            fprintf(dst, "%d", np->a_max);
            fprintf(dst, "|");
        }
        if (j == select_info.aggr_list.attr_count - 1) {
            fprintf(dst, "\n");
            row_count++;
        }
    }
    
    free_hashtab(type);
    
    if (row_count > 1) {
        fprintf(dst, "(%d rows)\n", row_count);
    } else {
        fprintf(dst, "(%d row)\n", row_count);
    }
}

/* build hashtable, return 0 on success, return -1 on error */
int build_hashtab(FILE *fp, int attrno, type_t type)
{
    metadata_t m;
    page_read((char *)&m, 0, fp, sizeof(metadata_t));
    
    struct nlist *np;
    char buffer[4096];
    int *v;
    union_t key;
    unsigned hashval;

    v = (int *)&buffer[attrno * 4];
    int i;
    for (i = 0; i < m.tuple_count; i++) {
        np = (struct nlist *) malloc(sizeof(*np));
        if (np == NULL) {
            return -1;
        }
        page_read(buffer, m.tuple_start + i, fp, 4096);
        if (type == VARCHAR) {
            key.string = strdup(&buffer[*v]);
            if (key.string == NULL) {
                return -1;
            }
            np->tupleno = i;
            np->key.string = key.string;
            np->type = type;
        } else {
            key.num = *v;
            np->tupleno = i;
            np->key.num = key.num;
            np->type = type;
        }
        hashval = hash(key, type);
        np->next = hashtab[hashval];
        hashtab[hashval] = np;
    }
    
    return 0;
}

/* hash and join, save results */
/* I don't think in this project hash is faster than nested loop,
   for hash can deal with more data, but it needs more io than nested loop */
/* big table page is in front of small table page */
void probe(FILE *fp_b, FILE *fp_s, int attrno, type_t type, FILE *output)
{
    metadata_t m_b;
    page_read((char *)&m_b, 0, fp_b, sizeof(metadata_t));
    metadata_t m_s;
    page_read((char *)&m_s, 0, fp_s, sizeof(metadata_t));
    metadata_t m;
    page_read((char *)&m, 0, output, sizeof(metadata_t));
    
    struct nlist *np;
    char buffer_b[4096];
    char buffer_s[4096];
    int *v;
    union_t key;
    unsigned hashval;
    
    v = (int *)&buffer_b[attrno * 4];
    int i;
    for (i = 0; i < m_b.tuple_count; i++){
        page_read(buffer_b, m_b.tuple_start + i, fp_b, 4096);
        if (type == VARCHAR) {
            key.string = &buffer_b[*v];
        } else {
            key.num = *v;
        }
        hashval = hash(key, type);
        for (np = hashtab[hashval]; np != NULL; np = np->next) {
            if (type == VARCHAR) {
                if (strcmp(np->key.string, key.string) == 0) {
                    page_write(buffer_b, m.tuple_start + 2 * m.tuple_count, output, 4096);
                    page_read(buffer_s, m_s.tuple_start + np->tupleno, fp_s, 4096);
                    page_write(buffer_s, m.tuple_start + 2 * m.tuple_count + 1, output, 4096);
                    m.tuple_count++;
                }
            } else {
                if (np->key.num == key.num) {
                    page_write(buffer_b, m.tuple_start + 2 * m.tuple_count, output, 4096);
                    page_read(buffer_s, m_s.tuple_start + np->tupleno, fp_s, 4096);
                    page_write(buffer_s, m.tuple_start + 2 * m.tuple_count + 1, output, 4096);
                    m.tuple_count++;
                }
            }
        }
    }
    
    page_write((char *)&m, 0, output, sizeof(metadata_t));
}

/* free hashtable */
void free_hashtab(type_t type)
{
    struct nlist *np;
    struct nlist *next;
    int i;
    if (type == VARCHAR) {
        for (i = 0; i < HASHSIZE; i++) {
            for (np = hashtab[i]; np != NULL; np = np->next) {
                free((void *)np->key.string);
            }
        }
    }
    for (i = 0; i < HASHSIZE; i++) {
        for (np = hashtab[i]; np != NULL; np = next) {
            next = np->next;
            free((void *)np);
        }
        hashtab[i] = NULL;
    }
}

/* copy name struct */
void copy_name(name_t *nd, name_t *ns)
{
    strcpy(nd->origin, ns->origin);
    strcpy(nd->attr, ns->attr);
    strcpy(nd->rel, ns->rel);
    nd->attrno = ns->attrno;
    nd->type = ns->type;
    nd->condno = ns->condno;
}

/* campare two name, return 1 on true, return 0 on false */
int same_name(name_t *nd, name_t *ns)
{
    if (strcmp(nd->attr, ns->attr) == 0 && strcmp(nd->rel, ns->rel) == 0) {
        return 1;
    }
    return 0;
}

/* add prefix as prefix(origin) */
void add_prefix(char *origin, char *prefix)
{
    char buffer[200];
    strcpy(buffer, prefix);
    strcat(buffer, "(");
    strcat(buffer, origin);
    strcat(buffer, ")");
    strcpy(origin, buffer);
}
