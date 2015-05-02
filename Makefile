LDLIBS	= -lraptor2
# CFLAGS	= -fblocks -g -pedantic -Wall -I../src -I/usr/local/include/raptor2 -I/usr/include/raptor2
CFLAGS	= -fblocks -O3 -pedantic -Wall -I../src -I/usr/local/include/raptor2 -I/usr/include/raptor2

CC		= clang

%.o: %.h %.c

#import: import.c
#	$(CC) $(CFLAGS) $(LDLIBS) import.c -o import

triplestore: triplestore.c triplestore.h avl.o linenoise.o
	$(CC) $(LDLIBS) $(CFLAGS) -o triplestore triplestore.c avl.o linenoise.o

clean:
	rm -f triplestore
	rm -f avl.o linenoise.o
	rm -rf *.dSYM

.PHONY	: clean
