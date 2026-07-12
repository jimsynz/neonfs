/* wire.c — see wire.h for the protocol contract. */

#include "wire.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <ei.h>

/* ------------------------------------------------------------------ */
/* errno atom table                                                    */
/* ------------------------------------------------------------------ */

struct errno_entry {
  const char *atom;
  int err;
};

/*
 * Mirrors NeonFS.CIFS.Handler's errno_for/1 vocabulary plus the POSIX
 * atoms the handler emits directly (:ebadf, :enotconn, :enosys, …).
 * Atoms with no POSIX equivalent fall through to EIO.
 */
static const struct errno_entry errno_table[] = {
    {"enoent", ENOENT},   {"eacces", EACCES},     {"eexist", EEXIST},
    {"eagain", EAGAIN},   {"enotempty", ENOTEMPTY}, {"exdev", EXDEV},
    {"eio", EIO},         {"einval", EINVAL},     {"enotconn", ENOTCONN},
    {"ebadf", EBADF},     {"enosys", ENOSYS},     {"eperm", EPERM},
    {"enospc", ENOSPC},   {"erofs", EROFS},       {"eisdir", EISDIR},
    {"enotdir", ENOTDIR},
};

int nw_errno_of_atom(const char *atom) {
  size_t i;
  for (i = 0; i < sizeof(errno_table) / sizeof(errno_table[0]); i++) {
    if (strcmp(atom, errno_table[i].atom) == 0) {
      return errno_table[i].err;
    }
  }
  return EIO;
}

/* ------------------------------------------------------------------ */
/* framed transport                                                    */
/* ------------------------------------------------------------------ */

static int write_all(int fd, const char *buf, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = write(fd, buf + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) {
      errno = EIO;
      return -1;
    }
    off += (size_t)n;
  }
  return 0;
}

static int read_all(int fd, char *buf, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = read(fd, buf + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) {
      errno = ECONNRESET;
      return -1;
    }
    off += (size_t)n;
  }
  return 0;
}

/* Frame an ei buffer (4-byte big-endian length prefix) and send it. */
static int send_frame(nw_conn *conn, ei_x_buff *x) {
  unsigned char prefix[4];
  uint32_t len = (uint32_t)x->index;
  prefix[0] = (unsigned char)(len >> 24);
  prefix[1] = (unsigned char)(len >> 16);
  prefix[2] = (unsigned char)(len >> 8);
  prefix[3] = (unsigned char)(len);
  if (write_all(conn->fd, (const char *)prefix, 4) < 0) return -1;
  return write_all(conn->fd, x->buff, (size_t)x->index);
}

/* Read one frame; caller frees *buf_out. */
static int recv_frame(nw_conn *conn, char **buf_out, int *len_out) {
  unsigned char prefix[4];
  uint32_t len;
  char *buf;
  if (read_all(conn->fd, (char *)prefix, 4) < 0) return -1;
  len = ((uint32_t)prefix[0] << 24) | ((uint32_t)prefix[1] << 16) |
        ((uint32_t)prefix[2] << 8) | (uint32_t)prefix[3];
  buf = malloc(len > 0 ? len : 1);
  if (buf == NULL) {
    errno = ENOMEM;
    return -1;
  }
  if (read_all(conn->fd, buf, len) < 0) {
    free(buf);
    return -1;
  }
  *buf_out = buf;
  *len_out = (int)len;
  return 0;
}

/* ------------------------------------------------------------------ */
/* request encoding                                                    */
/* ------------------------------------------------------------------ */

/* Open a request: version, `{op, args_map}` tuple head, args-map head. */
static int req_open(ei_x_buff *x, const char *op, int map_arity) {
  if (ei_x_new_with_version(x) != 0) return -1;
  if (ei_x_encode_tuple_header(x, 2) != 0) return -1;
  if (ei_x_encode_atom(x, op) != 0) return -1;
  return ei_x_encode_map_header(x, map_arity);
}

static int put_str(ei_x_buff *x, const char *key, const char *val) {
  if (ei_x_encode_binary(x, key, (long)strlen(key)) != 0) return -1;
  return ei_x_encode_binary(x, val, (long)strlen(val));
}

static int put_bin(ei_x_buff *x, const char *key, const void *val, long len) {
  if (ei_x_encode_binary(x, key, (long)strlen(key)) != 0) return -1;
  return ei_x_encode_binary(x, val, len);
}

static int put_int(ei_x_buff *x, const char *key, long long val) {
  if (ei_x_encode_binary(x, key, (long)strlen(key)) != 0) return -1;
  return ei_x_encode_longlong(x, val);
}

/* ------------------------------------------------------------------ */
/* reply decoding                                                      */
/* ------------------------------------------------------------------ */

/*
 * Receive a reply frame and decode the `{:ok, payload}` / `{:error, atom}`
 * envelope. On the ok arm, returns 0 with the buf/idx out-params positioned
 * at the payload term (caller decodes it, then frees buf). On the error arm
 * or a transport failure, returns -1 with errno set and buf already freed.
 */
static int recv_envelope(nw_conn *conn, char **buf_out, int *idx_out) {
  char *buf = NULL;
  int len = 0;
  int idx = 0;
  int version = 0;
  int arity = 0;
  char tag[MAXATOMLEN + 1];

  if (recv_frame(conn, &buf, &len) < 0) return -1;

  if (ei_decode_version(buf, &idx, &version) != 0 ||
      ei_decode_tuple_header(buf, &idx, &arity) != 0 || arity != 2 ||
      ei_decode_atom(buf, &idx, tag) != 0) {
    free(buf);
    errno = EPROTO;
    return -1;
  }

  if (strcmp(tag, "ok") == 0) {
    *buf_out = buf;
    *idx_out = idx;
    return 0;
  }

  if (strcmp(tag, "error") == 0) {
    char reason[MAXATOMLEN + 1];
    int err = EIO;
    if (ei_decode_atom(buf, &idx, reason) == 0) err = nw_errno_of_atom(reason);
    free(buf);
    errno = err;
    return -1;
  }

  free(buf);
  errno = EPROTO;
  return -1;
}

/* Send a request whose reply payload is ignored (`%{}`-style ok arm). */
static int call_void(nw_conn *conn, ei_x_buff *x) {
  char *buf = NULL;
  int idx = 0;
  int rc;
  if (send_frame(conn, x) < 0) {
    ei_x_free(x);
    return -1;
  }
  ei_x_free(x);
  rc = recv_envelope(conn, &buf, &idx);
  if (rc == 0) free(buf);
  return rc;
}

/* Decode a binary term into a fixed buffer, NUL-terminating. */
static int decode_binary_str(const char *buf, int *idx, char *out, size_t cap) {
  int type = 0;
  int size = 0;
  long got = 0;
  if (ei_get_type(buf, idx, &type, &size) != 0) return -1;
  if ((size_t)size >= cap) return -1;
  if (ei_decode_binary(buf, idx, out, &got) != 0) return -1;
  out[got] = '\0';
  return 0;
}

static int decode_kind(const char *buf, int *idx, nw_kind *out) {
  char atom[MAXATOMLEN + 1];
  if (ei_decode_atom(buf, idx, atom) != 0) return -1;
  *out = (strcmp(atom, "directory") == 0) ? NW_KIND_DIRECTORY : NW_KIND_FILE;
  return 0;
}

/* ------------------------------------------------------------------ */
/* connection lifecycle                                                */
/* ------------------------------------------------------------------ */

int nw_dial(nw_conn *conn, const char *path) {
  struct sockaddr_un addr;
  int fd;

  if (strlen(path) >= sizeof(addr.sun_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }

  fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    int saved = errno;
    close(fd);
    errno = saved;
    return -1;
  }

  conn->fd = fd;
  return 0;
}

void nw_attach(nw_conn *conn, int fd) { conn->fd = fd; }

void nw_close_conn(nw_conn *conn) {
  if (conn->fd >= 0) {
    close(conn->fd);
    conn->fd = -1;
  }
}

/* ------------------------------------------------------------------ */
/* ops                                                                 */
/* ------------------------------------------------------------------ */

int nw_connect(nw_conn *conn, const char *volume) {
  ei_x_buff x;
  if (req_open(&x, "connect", 1) != 0 || put_str(&x, "volume", volume) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_disconnect(nw_conn *conn) {
  ei_x_buff x;
  if (req_open(&x, "disconnect", 0) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

static int do_stat(nw_conn *conn, const char *op, const char *path,
                   uint64_t handle, int by_handle, nw_stat_t *out) {
  ei_x_buff x;
  char *buf = NULL;
  int idx = 0;
  int outer = 0;
  int inner = 0;
  char key[MAXATOMLEN + 1];
  int i;

  if (req_open(&x, op, 1) != 0 ||
      (by_handle ? put_int(&x, "handle", (long long)handle)
                 : put_str(&x, "path", path)) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  if (send_frame(conn, &x) < 0) {
    ei_x_free(&x);
    return -1;
  }
  ei_x_free(&x);

  if (recv_envelope(conn, &buf, &idx) < 0) return -1;

  /* payload = %{stat: %{...}} */
  if (ei_decode_map_header(buf, &idx, &outer) != 0 || outer != 1 ||
      ei_decode_atom(buf, &idx, key) != 0 || strcmp(key, "stat") != 0 ||
      ei_decode_map_header(buf, &idx, &inner) != 0) {
    free(buf);
    errno = EPROTO;
    return -1;
  }

  memset(out, 0, sizeof(*out));
  for (i = 0; i < inner; i++) {
    long long n = 0;
    if (ei_decode_atom(buf, &idx, key) != 0) goto bad;
    if (strcmp(key, "kind") == 0) {
      if (decode_kind(buf, &idx, &out->kind) != 0) goto bad;
      continue;
    }
    if (ei_decode_longlong(buf, &idx, &n) != 0) goto bad;
    if (strcmp(key, "size") == 0)
      out->size = (uint64_t)n;
    else if (strcmp(key, "mode") == 0)
      out->mode = (uint32_t)n;
    else if (strcmp(key, "atime") == 0)
      out->atime = n;
    else if (strcmp(key, "mtime") == 0)
      out->mtime = n;
    else if (strcmp(key, "ctime") == 0)
      out->ctime = n;
  }

  free(buf);
  return 0;

bad:
  free(buf);
  errno = EPROTO;
  return -1;
}

int nw_stat(nw_conn *conn, const char *path, nw_stat_t *out) {
  return do_stat(conn, "stat", path, 0, 0, out);
}

int nw_lstat(nw_conn *conn, const char *path, nw_stat_t *out) {
  return do_stat(conn, "lstat", path, 0, 0, out);
}

int nw_fstat(nw_conn *conn, uint64_t handle, nw_stat_t *out) {
  return do_stat(conn, "fstat", NULL, handle, 1, out);
}

int nw_fchmod(nw_conn *conn, uint64_t handle, uint32_t mode) {
  ei_x_buff x;
  if (req_open(&x, "fchmod", 2) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "mode", (long long)mode) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_fchown(nw_conn *conn, uint64_t handle, uint32_t uid, uint32_t gid) {
  ei_x_buff x;
  if (req_open(&x, "fchown", 3) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "uid", (long long)uid) != 0 ||
      put_int(&x, "gid", (long long)gid) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_fntimes(nw_conn *conn, uint64_t handle, int64_t atime, int64_t mtime) {
  ei_x_buff x;
  if (req_open(&x, "fntimes", 3) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "atime", (long long)atime) != 0 ||
      put_int(&x, "mtime", (long long)mtime) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

/* Decode a single-field `%{key: integer}` ok-arm payload. */
static int call_uint_field(nw_conn *conn, ei_x_buff *x, const char *field,
                           uint64_t *out) {
  char *buf = NULL;
  int idx = 0;
  int arity = 0;
  char key[MAXATOMLEN + 1];
  unsigned long long v = 0;

  if (send_frame(conn, x) < 0) {
    ei_x_free(x);
    return -1;
  }
  ei_x_free(x);

  if (recv_envelope(conn, &buf, &idx) < 0) return -1;

  if (ei_decode_map_header(buf, &idx, &arity) != 0 || arity != 1 ||
      ei_decode_atom(buf, &idx, key) != 0 || strcmp(key, field) != 0 ||
      ei_decode_ulonglong(buf, &idx, &v) != 0) {
    free(buf);
    errno = EPROTO;
    return -1;
  }

  free(buf);
  *out = (uint64_t)v;
  return 0;
}

int nw_openat(nw_conn *conn, const char *path, int32_t flags, uint32_t mode,
              uint64_t *handle_out) {
  ei_x_buff x;
  if (req_open(&x, "openat", 3) != 0 || put_str(&x, "path", path) != 0 ||
      put_int(&x, "flags", (long long)flags) != 0 ||
      put_int(&x, "mode", (long long)mode) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_uint_field(conn, &x, "handle", handle_out);
}

int nw_close(nw_conn *conn, uint64_t handle) {
  ei_x_buff x;
  if (req_open(&x, "close", 1) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_pread(nw_conn *conn, uint64_t handle, int64_t offset, uint64_t size,
             uint8_t *buf, size_t buf_cap, size_t *len_out) {
  ei_x_buff x;
  char *rbuf = NULL;
  int idx = 0;
  int arity = 0;
  int type = 0;
  int dsize = 0;
  long got = 0;
  char key[MAXATOMLEN + 1];

  if (req_open(&x, "pread", 3) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "offset", (long long)offset) != 0 ||
      put_int(&x, "size", (long long)size) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  if (send_frame(conn, &x) < 0) {
    ei_x_free(&x);
    return -1;
  }
  ei_x_free(&x);

  if (recv_envelope(conn, &rbuf, &idx) < 0) return -1;

  if (ei_decode_map_header(rbuf, &idx, &arity) != 0 || arity != 1 ||
      ei_decode_atom(rbuf, &idx, key) != 0 || strcmp(key, "data") != 0 ||
      ei_get_type(rbuf, &idx, &type, &dsize) != 0) {
    free(rbuf);
    errno = EPROTO;
    return -1;
  }
  if ((size_t)dsize > buf_cap) {
    free(rbuf);
    errno = ERANGE;
    return -1;
  }
  if (ei_decode_binary(rbuf, &idx, buf, &got) != 0) {
    free(rbuf);
    errno = EPROTO;
    return -1;
  }

  free(rbuf);
  *len_out = (size_t)got;
  return 0;
}

int nw_pwrite(nw_conn *conn, uint64_t handle, int64_t offset,
              const uint8_t *data, size_t len, uint64_t *written_out) {
  ei_x_buff x;
  if (req_open(&x, "pwrite", 3) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "offset", (long long)offset) != 0 ||
      put_bin(&x, "data", data, (long)len) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_uint_field(conn, &x, "written", written_out);
}

int nw_ftruncate(nw_conn *conn, uint64_t handle, uint64_t size) {
  ei_x_buff x;
  if (req_open(&x, "ftruncate", 2) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0 ||
      put_int(&x, "size", (long long)size) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_fsync(nw_conn *conn, uint64_t handle) {
  ei_x_buff x;
  if (req_open(&x, "fsync", 1) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_fdopendir(nw_conn *conn, const char *path, uint64_t *handle_out) {
  ei_x_buff x;
  if (req_open(&x, "fdopendir", 1) != 0 || put_str(&x, "path", path) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_uint_field(conn, &x, "handle", handle_out);
}

int nw_readdir(nw_conn *conn, uint64_t handle, nw_dirent_t *out, int *eof_out) {
  ei_x_buff x;
  char *buf = NULL;
  int idx = 0;
  int arity = 0;
  int i;
  int saw_eof = 0;
  int eof = 0;
  int have_entry = 0;

  if (req_open(&x, "readdir", 1) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  if (send_frame(conn, &x) < 0) {
    ei_x_free(&x);
    return -1;
  }
  ei_x_free(&x);

  if (recv_envelope(conn, &buf, &idx) < 0) return -1;

  /* payload = %{eof: true} | %{entry: %{...}, eof: false} */
  if (ei_decode_map_header(buf, &idx, &arity) != 0) {
    free(buf);
    errno = EPROTO;
    return -1;
  }

  memset(out, 0, sizeof(*out));
  for (i = 0; i < arity; i++) {
    char key[MAXATOMLEN + 1];
    if (ei_decode_atom(buf, &idx, key) != 0) goto bad;

    if (strcmp(key, "eof") == 0) {
      char b[MAXATOMLEN + 1];
      if (ei_decode_boolean(buf, &idx, &eof) != 0) {
        /* Some encoders surface booleans as the atoms true/false. */
        if (ei_decode_atom(buf, &idx, b) != 0) goto bad;
        eof = (strcmp(b, "true") == 0);
      }
      saw_eof = 1;
    } else if (strcmp(key, "entry") == 0) {
      int inner = 0;
      int j;
      if (ei_decode_map_header(buf, &idx, &inner) != 0) goto bad;
      for (j = 0; j < inner; j++) {
        char ek[MAXATOMLEN + 1];
        if (ei_decode_atom(buf, &idx, ek) != 0) goto bad;
        if (strcmp(ek, "name") == 0) {
          if (decode_binary_str(buf, &idx, out->name, sizeof(out->name)) != 0)
            goto bad;
        } else if (strcmp(ek, "kind") == 0) {
          if (decode_kind(buf, &idx, &out->kind) != 0) goto bad;
        } else {
          goto bad;
        }
      }
      have_entry = 1;
    } else {
      goto bad;
    }
  }

  free(buf);
  if (!saw_eof) {
    errno = EPROTO;
    return -1;
  }
  *eof_out = eof;
  if (!eof && !have_entry) {
    errno = EPROTO;
    return -1;
  }
  return 0;

bad:
  free(buf);
  errno = EPROTO;
  return -1;
}

int nw_closedir(nw_conn *conn, uint64_t handle) {
  ei_x_buff x;
  if (req_open(&x, "closedir", 1) != 0 ||
      put_int(&x, "handle", (long long)handle) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_mkdirat(nw_conn *conn, const char *path, uint32_t mode) {
  ei_x_buff x;
  if (req_open(&x, "mkdirat", 2) != 0 || put_str(&x, "path", path) != 0 ||
      put_int(&x, "mode", (long long)mode) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_unlinkat(nw_conn *conn, const char *path) {
  ei_x_buff x;
  if (req_open(&x, "unlinkat", 1) != 0 || put_str(&x, "path", path) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

int nw_renameat(nw_conn *conn, const char *old_path, const char *new_path) {
  ei_x_buff x;
  if (req_open(&x, "renameat", 2) != 0 ||
      put_str(&x, "old_path", old_path) != 0 ||
      put_str(&x, "new_path", new_path) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  return call_void(conn, &x);
}

static int do_statvfs(nw_conn *conn, const char *op, nw_statvfs_t *out) {
  ei_x_buff x;
  char *buf = NULL;
  int idx = 0;
  int arity = 0;
  int i;

  if (req_open(&x, op, 0) != 0) {
    ei_x_free(&x);
    errno = EPROTO;
    return -1;
  }
  if (send_frame(conn, &x) < 0) {
    ei_x_free(&x);
    return -1;
  }
  ei_x_free(&x);

  if (recv_envelope(conn, &buf, &idx) < 0) return -1;

  if (ei_decode_map_header(buf, &idx, &arity) != 0) {
    free(buf);
    errno = EPROTO;
    return -1;
  }

  memset(out, 0, sizeof(*out));
  for (i = 0; i < arity; i++) {
    char key[MAXATOMLEN + 1];
    unsigned long long v = 0;
    if (ei_decode_atom(buf, &idx, key) != 0 ||
        ei_decode_ulonglong(buf, &idx, &v) != 0) {
      free(buf);
      errno = EPROTO;
      return -1;
    }
    if (strcmp(key, "total_bytes") == 0)
      out->total_bytes = (uint64_t)v;
    else if (strcmp(key, "free_bytes") == 0)
      out->free_bytes = (uint64_t)v;
    else if (strcmp(key, "available_bytes") == 0)
      out->available_bytes = (uint64_t)v;
  }

  free(buf);
  return 0;
}

int nw_disk_free(nw_conn *conn, nw_statvfs_t *out) {
  return do_statvfs(conn, "disk_free", out);
}

int nw_fstatvfs(nw_conn *conn, nw_statvfs_t *out) {
  return do_statvfs(conn, "fstatvfs", out);
}
