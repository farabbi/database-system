#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[])
{
    FILE *queryfile;
    void ntrdb(FILE *, FILE *);
    char *prog = argv[0];
    
    if (argc == 1) {
        ntrdb(stdin, stdout);
    } else {
        while (--argc > 0) {
            if ((queryfile = fopen(*++argv, "r")) == NULL) {
                fprintf(stderr, "%s: can't open %s\n", prog, *argv);
                exit(1);
            } else {
                ntrdb(queryfile, stdout);
                fclose(queryfile);
            }
        }
    }
    if (ferror(stdout)) {
        fprintf(stderr, "%s: error writing stdout\n", prog);
        exit(2);
    }
    exit(0);
}
