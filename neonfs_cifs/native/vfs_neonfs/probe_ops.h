/*
 * probe_ops.h — the canned op-drive sequence shared by both test binaries.
 *
 * `nw_probe_run` drives an already-connected wire client through every op
 * (plus the error-arm sentinels) and asserts the decoded structs / errnos
 * against the canned contract. It is reused by:
 *
 *   - test_wire   — against an in-process mock responder over a socketpair
 *                   (`ei` on both sides);
 *   - wire_probe  — against a live NeonFS.CIFS.Listener over a Unix socket
 *                   (real `term_to_binary` / `binary_to_term` on the Elixir
 *                   side).
 *
 * Keeping the sequence and the canned values in one place stops the two
 * responders (the C mock and the Elixir canned handler) from drifting.
 */

#ifndef NEONFS_VFS_PROBE_OPS_H
#define NEONFS_VFS_PROBE_OPS_H

#include "wire.h"

/* Run the full op sequence over `conn`. Returns the number of failed checks
 * (0 = all passed); diagnostics are printed to stderr. */
int nw_probe_run(nw_conn *conn);

#endif /* NEONFS_VFS_PROBE_OPS_H */
