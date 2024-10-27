CC := gcc
CFLAGS := -g -Wall -w
SRC :=  dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c test_assign2_2.c
OBJ := dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

default: test_assign2_1 test_assign2_2

test_assign2_1: $(OBJ) test_assign2_1.o
	$(CC) $(CFLAGS) -o $@ $^

test_assign2_2: $(OBJ) test_assign2_2.o
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $^

clean:
	rm -f test_assign2_1 test_assign2_2 *.o *.bin