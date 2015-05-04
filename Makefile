LDLIBS	= -lraptor2 -lpcre
CFLAGS	= -fblocks -g -pedantic -Wall -I../src -I/usr/local/include/raptor2 -I/usr/include/raptor2
# CFLAGS	= -fblocks -O3 -pedantic -Wall -I../src -I/usr/local/include/raptor2 -I/usr/include/raptor2

CC		= clang

%.o: %.h %.c

#import: import.c
#	$(CC) $(CFLAGS) $(LDLIBS) import.c -o import

ts: triplestore.o avl.o linenoise.o
	$(CC) $(LDLIBS) $(CFLAGS) -o ts ts.c triplestore.o avl.o linenoise.o

clean:
	rm -f ts
	rm -f avl.o linenoise.o triplestore.o
	rm -rf *.dSYM

.PHONY	: clean
