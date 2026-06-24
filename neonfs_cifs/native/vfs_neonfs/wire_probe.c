/*
 * wire_probe.c — drive the wire client against a *live* neonfs_cifs listener.
 *
 * Usage: wire_probe <unix-socket-path>
 *
 * Connects to a running NeonFS.CIFS.Listener over its Unix socket and runs
 * the shared op sequence (probe_ops.c). Unlike test_wire's in-process mock
 * (which encodes replies with `ei`), this exercises the real Elixir
 * `term_to_binary` / `binary_to_term` path. Exits 0 when every check passes.
 *
 * The Elixir side is started by the accompanying ExUnit test, which binds a
 * listener with a canned-reply handler whose replies match probe_ops.c's
 * expectations.
 */

#include "probe_ops.h"
#include "wire.h"

#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
  nw_conn conn;
  int failures;

  if (argc != 2) {
    fprintf(stderr, "usage: %s <unix-socket-path>\n", argv[0]);
    return 2;
  }

  if (nw_dial(&conn, argv[1]) != 0) {
    perror("nw_dial");
    return 2;
  }

  failures = nw_probe_run(&conn);
  nw_close_conn(&conn);

  if (failures == 0) {
    printf("ok: all wire round-trips passed against live listener\n");
    return 0;
  }
  fprintf(stderr, "%d check(s) failed\n", failures);
  return 1;
}
