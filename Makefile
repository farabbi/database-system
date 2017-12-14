SRC = main.c ntrdb.c create_table.c drop_table.c insert.c select.c getquery.c zip_query.c gadget.c ntrdb.h

ntrdb : $(SRC)
	mkdir db
	gcc -Wall -o ntrdb $(SRC)
	
clean : 
	rm ntrdb
	rm -r db
