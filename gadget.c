#include <stdio.h>
#include <ctype.h>

/* getline from queryfile, return length of s */
int fgetline(char s[], int lim, FILE *queryfile)
{
    int c, i;
    i = 0;
    while (--lim > 0 && (c=fgetc(queryfile)) != EOF && c != '\n') {
        s[i++] = c;
    }
    if (c == '\n') {
        s[i++] = c;
    }
    s[i] = '\0';
    return i;
}

/* get a word from a string, start at s_offset, automatically skip space,
   return next s_offset on success, return -1 on error, 
   ( ) , ' ; are dealt separately */
int getword(char word[], char s[], int s_offset)
{
    int i = 0;
    while (isspace(s[s_offset])) {
        s_offset++;
    }
    
getchar:
    word[i] = s[s_offset];
    if (word[i] == '\0') {
        /* no word remain */
        return s_offset;
    } else if (word[i] == '\'') {
        /* quote */
        i++;
        s_offset++;
        while (s[s_offset] != '\'' && s[s_offset] != '\0') {
            word[i++] = s[s_offset++];
        }
        if (s[s_offset] == '\'') {
            word[i++] = s[s_offset++];
            word[i] = '\0';
            return s_offset;
        } else {
            word[i] = s[s_offset];
            return -1;
        }
    } else if (word[i] == '(' || word[i] == ')' || word[i] == ',' 
            || word[i] == ';' || word[i] == '=' || word[i] == '*') {
        /* return with one symbol */
        i++;
        word[i] = '\0';
        s_offset++;
        return s_offset;
    } else if (word[i] == '<' || word[i] == '>') {
        /* check whether it has a partner '=' */
        i++;
        s_offset++;
        if (s[s_offset] == '=') {
            word[i++] = s[s_offset++];
            word[i] = '\0';
            return s_offset;
        } else {
            word[i] = '\0';
            return s_offset;
        }
    } else if (word[i] == '!') {
        /* must compare with '=' */
        i++;
        s_offset++;
        if (s[s_offset] == '=') {
            word[i++] = s[s_offset++];
            word[i] = '\0';
            return s_offset;
        } else {
            word[i] = '\0';
            return -1;
        }
    } else {
        /* it is a normal word */
        s_offset++;
        if (isspace(s[s_offset]) || s[s_offset] == '(' || s[s_offset] == ')' 
           || s[s_offset] == ',' || s[s_offset] == '\'' || s[s_offset] == ';' 
           || s[s_offset] == '\0' || s[s_offset] == '<' || s[s_offset] == '>'
           || s[s_offset] == '=' || s[s_offset] == '!' || s[s_offset] == '*') {
            i++;
            word[i] = '\0';
            return s_offset;
        } else {
            i++;
            goto getchar;
        }
    }
}

/* read a page from a file, which is open with "b", 
   buffer should be larger than 4096 bytes, page_num start at 0 */
void page_read(char buffer[], int page_num, FILE *fp, int size)
{
    /* Assume there is no error, so we don't check the error */
    fseek(fp, 4096 * page_num, SEEK_SET);
    fread(buffer, size, 1, fp);
}

/* write a page to a file, which is open with "b", 
   buffer should be larger than 4096 bytes, page_num start at 0 */ 
void page_write(char buffer[], int page_num, FILE *fp, int size)
{
    /* Assume there is no error, so we don't check the error */
    fseek(fp, 4096 * page_num, SEEK_SET);
    fwrite(buffer, size, 1, fp);
}

/* check whether a string is a num, return 1 on true, return 0 on false */
int isnum(char word[])
{
    int i = 0;
    if (!isdigit(word[i]) && word[i] != '-') {
        return 0;
    }
    i++;
    while (word[i] != '\0') {
        if (!isdigit(word[i])) {
            return 0;
        }
        i++;
    }
    return 1;
}

/* check whether a string is quote by '', return 1 on true, return 0 on false */
int isstring(char word[])
{
    int i = 0;
    if (word[i] != '\'') {
        return 0;
    }
    while (word[i] != '\0') {
        i++;
    }
    if (word[i-1] != '\'') {
        return 0;
    }
    return 1;
}

/* save a quoted string without quote marks in buffer, begin with buffer_p,
   return next string start position */
char *save_string(char *buffer_p, char word[])
{
    int i = 0;
    while (word[i] != '\0') {
        if (word[i] == '\'') {
            i++;
        } else {
            *buffer_p = word[i];
            buffer_p++;
            i++;
        }
    }
    *buffer_p = word[i];
    buffer_p++;
    return buffer_p;
}

