/*
 * wire.h — client side of the neonfs_cifs ETF protocol.
 *
 * The protocol half of the vfs_neonfs.so Samba VFS shim, with no Samba
 * dependency. Speaks the same wire contract as NeonFS.CIFS.Handler:
 *
 *   - Framing: 4-byte big-endian length prefix + ETF body. The Elixir
 *     listener runs the socket in `packet: 4` mode, so it auto-frames its
 *     side; this client adds the prefix on send and strips it on receive.
 *   - Request: an ETF `{op_atom, args_map}` tuple. Argument-map keys are
 *     ETF binaries ("path", "handle", …); values are binaries or integers.
 *   - Reply: `{:ok, payload_map}` (payload-map keys are atoms) or
 *     `{:error, errno_atom}`.
 *
 * Every op returns 0 on the `{:ok, _}` arm (filling the out-parameter) or
 * -1 on the `{:error, _}` arm with `errno` set from the reply atom. A
 * transport failure also returns -1 with `errno` set (EIO / the syscall's
 * own errno).
 */

#ifndef NEONFS_VFS_WIRE_H
#define NEONFS_VFS_WIRE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* An open connection to a neonfs_cifs listener. */
typedef struct {
  int fd;
} nw_conn;

/* File kind, decoded from the reply's `:kind` atom. */
typedef enum {
  NW_KIND_FILE = 0,
  NW_KIND_DIRECTORY = 1
} nw_kind;

/* Decoded `stat` payload (`%{stat: %{...}}`). */
typedef struct {
  uint64_t size;
  uint32_t mode;
  int64_t atime;
  int64_t mtime;
  int64_t ctime;
  nw_kind kind;
} nw_stat_t;

/* Decoded `readdir` entry (`%{entry: %{name:, kind:}}`). */
#define NW_NAME_MAX 256
typedef struct {
  char name[NW_NAME_MAX];
  nw_kind kind;
} nw_dirent_t;

/* Decoded `disk_free` / `fstatvfs` payload. */
typedef struct {
  uint64_t total_bytes;
  uint64_t free_bytes;
  uint64_t available_bytes;
} nw_statvfs_t;

/*
 * Connect to a neonfs_cifs Unix domain socket at `path`. Returns 0 and
 * fills `conn` on success, -1 with errno set on failure. The returned
 * connection owns the fd; release it with nw_disconnect()/nw_close_conn().
 */
int nw_dial(nw_conn *conn, const char *path);

/* Wrap an already-connected stream fd (used by the test harness). */
void nw_attach(nw_conn *conn, int fd);

/* Close the underlying fd. Idempotent. */
void nw_close_conn(nw_conn *conn);

/* Lifecycle */
int nw_connect(nw_conn *conn, const char *volume);
int nw_disconnect(nw_conn *conn);

/* Metadata */
int nw_stat(nw_conn *conn, const char *path, nw_stat_t *out);
int nw_lstat(nw_conn *conn, const char *path, nw_stat_t *out);
int nw_fstat(nw_conn *conn, uint64_t handle, nw_stat_t *out);
int nw_fchmod(nw_conn *conn, uint64_t handle, uint32_t mode);
int nw_fchown(nw_conn *conn, uint64_t handle, uint32_t uid, uint32_t gid);
int nw_fntimes(nw_conn *conn, uint64_t handle, int64_t atime, int64_t mtime);

/* File I/O */
int nw_openat(nw_conn *conn, const char *path, int32_t flags, uint32_t mode,
              uint64_t *handle_out);
int nw_close(nw_conn *conn, uint64_t handle);
int nw_pread(nw_conn *conn, uint64_t handle, int64_t offset, uint64_t size,
             uint8_t *buf, size_t buf_cap, size_t *len_out);
int nw_pwrite(nw_conn *conn, uint64_t handle, int64_t offset,
              const uint8_t *data, size_t len, uint64_t *written_out);
int nw_ftruncate(nw_conn *conn, uint64_t handle, uint64_t size);
int nw_fsync(nw_conn *conn, uint64_t handle);

/* Directories */
int nw_fdopendir(nw_conn *conn, const char *path, uint64_t *handle_out);
int nw_readdir(nw_conn *conn, uint64_t handle, nw_dirent_t *out, int *eof_out);
int nw_closedir(nw_conn *conn, uint64_t handle);
int nw_mkdirat(nw_conn *conn, const char *path, uint32_t mode);

/* Mutations */
int nw_unlinkat(nw_conn *conn, const char *path);
int nw_renameat(nw_conn *conn, const char *old_path, const char *new_path);

/* Filesystem */
int nw_disk_free(nw_conn *conn, nw_statvfs_t *out);
int nw_fstatvfs(nw_conn *conn, nw_statvfs_t *out);

/*
 * Map a neonfs_cifs reply errno atom (e.g. "enoent") to a POSIX errno.
 * Atoms with no POSIX equivalent map to EIO. Exposed for the harness.
 */
int nw_errno_of_atom(const char *atom);

#ifdef __cplusplus
}
#endif

#endif /* NEONFS_VFS_WIRE_H */
