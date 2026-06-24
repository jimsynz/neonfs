# Build + test the vfs_neonfs ETF wire client.
#
# erl_interface (ei) headers/libs are discovered from the running Erlang
# install, so no Samba dependency and nothing to configure beyond a C
# compiler and Erlang on PATH.

ERL ?= erl
EI_DIR := $(shell $(ERL) -noshell -eval 'io:format("~ts", [code:lib_dir(erl_interface)]), halt().')
EI_INCLUDE := $(EI_DIR)/include
EI_LIB := $(EI_DIR)/lib

CC ?= cc
CFLAGS ?= -O2 -g -std=c11 -Wall -Wextra -Werror
CPPFLAGS += -I$(EI_INCLUDE) -I.
LDFLAGS += -L$(EI_LIB)
LDLIBS += -lei -lpthread

OBJS := wire.o
TEST_BIN := test_wire

.PHONY: all test clean
all: $(TEST_BIN)

wire.o: wire.c wire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ wire.c

$(TEST_BIN): test_wire.c $(OBJS) wire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ test_wire.c $(OBJS) $(LDFLAGS) $(LDLIBS)

test: $(TEST_BIN)
	./$(TEST_BIN)

clean:
	rm -f $(OBJS) $(TEST_BIN)
