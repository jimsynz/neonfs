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

OBJS := wire.o probe_ops.o
TEST_BIN := test_wire
PROBE_BIN := wire_probe

.PHONY: all test clean
all: $(TEST_BIN) $(PROBE_BIN)

wire.o: wire.c wire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ wire.c

probe_ops.o: probe_ops.c probe_ops.h wire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ probe_ops.c

$(TEST_BIN): test_wire.c $(OBJS) wire.h probe_ops.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ test_wire.c $(OBJS) $(LDFLAGS) $(LDLIBS)

$(PROBE_BIN): wire_probe.c $(OBJS) wire.h probe_ops.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ wire_probe.c $(OBJS) $(LDFLAGS) $(LDLIBS)

# Run the in-process mock-responder harness (ei on both sides).
test: $(TEST_BIN)
	./$(TEST_BIN)

clean:
	rm -f $(OBJS) $(TEST_BIN) $(PROBE_BIN)
