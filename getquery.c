#include <stdio.h>
#include <string.h>
#include "ntrdb.h"

#define MAX_BUFFER_LENGTH 5000

int fgetline(char s[], int lim, FILE *queryfile);
int getword(char word[], char s[], int s_offset);
inquery_t zip_query(char word[]);

/* get a query from queryfile, return 1 when success, 
   return -1 when error, return 0 when reach EOF */
int getquery(FILE *queryfile)
{
    inquery_t inquery = FIRST_TIME;

    char buffer[MAX_BUFFER_LENGTH];
    int buffer_len;

    /* getline */    
getline:
    buffer_len = fgetline(buffer, MAX_BUFFER_LENGTH, queryfile);
    if (buffer_len > 0) {
        if (buffer[0] == '-' && buffer[1] == '-') {
            if (inquery == FIRST_TIME) {
                goto getline;
            } else if (inquery == INQUERY) {
                return -1;
            }
        }
    } else {
        /* EOF case */
        if (inquery == FIRST_TIME) {
            return 0;
        } else if (inquery == INQUERY){
            return -1;
        }
    }
    
    /* getword */
    char word[MAX_BUFFER_LENGTH];
    int buffer_offset = 0;
    while (buffer_offset < buffer_len) {
        buffer_offset = getword(word, buffer, buffer_offset);
        if (buffer_offset < 0) {
            /* a quote meets '\0' before '\'' */ 
            return -1;
        }
        if (strlen(word) > 0) {
            /* Assume there're no word behind ';' */
            inquery = zip_query(word);
        }
    }
    
    if (inquery == FIRST_TIME) {
        goto getline;
    } else if (inquery == INQUERY) {
        goto getline;
    } else if (inquery == FINISH){
        return 1;
    } else {
        /* we should never be here */
        return -1;
    }
}

