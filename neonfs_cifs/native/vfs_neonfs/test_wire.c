/*
 * test_wire.c — mock-responder unit harness for the vfs_neonfs wire client.
 *
 * A paired Unix socket (`socketpair`) separates a client (the parent, which
 * drives every op through wire.c) from a mock responder (the child, which
 * decodes each request — asserting the encoded `{op, args}` round-trips —
 * and writes back a canned ETF reply matching NeonFS.CIFS.Handler's reply
 * shapes). The parent then asserts the decoded C structs / errnos.
 *
 * The child exits non-zero on any request-decode mismatch; the parent
 * asserts both its own decoded values and the child's exit status.
 */

#include "wire.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <ei.h>

static int g_failures = 0;

#define CHECK(cond, msg)                                              \
  do {                                                               \
    if (!(cond)) {                                                   \
      fprintf(stderr, "FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
      g_failures++;                                                  \
    }                                                                \
  } while (0)

/* ------------------------------------------------------------------ */
/* framed transport (harness copies; wire.c's are static)              */
/* ------------------------------------------------------------------ */

static int read_all_h(int fd, char *buf, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = read(fd, buf + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) return -1;
    off += (size_t)n;
  }
  return 0;
}

static int write_all_h(int fd, const char *buf, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = write(fd, buf + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    off += (size_t)n;
  }
  return 0;
}

static int recv_frame_h(int fd, char *buf, size_t cap, int *len_out) {
  unsigned char p[4];
  uint32_t len;
  if (read_all_h(fd, (char *)p, 4) < 0) return -1;
  len = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
        ((uint32_t)p[2] << 8) | (uint32_t)p[3];
  if (len > cap) return -1;
  if (read_all_h(fd, buf, len) < 0) return -1;
  *len_out = (int)len;
  return 0;
}

static int send_frame_h(int fd, ei_x_buff *x) {
  unsigned char p[4];
  uint32_t len = (uint32_t)x->index;
  p[0] = (unsigned char)(len >> 24);
  p[1] = (unsigned char)(len >> 16);
  p[2] = (unsigned char)(len >> 8);
  p[3] = (unsigned char)(len);
  if (write_all_h(fd, (const char *)p, 4) < 0) return -1;
  return write_all_h(fd, x->buff, (size_t)x->index);
}

/* ------------------------------------------------------------------ */
/* mock responder (child)                                              */
/* ------------------------------------------------------------------ */

/* String args collected from a request map, keyed by name. */
typedef struct {
  char keys[8][32];
  char vals[8][512];
  long vlens[8];
  int n;
} strargs;

static const char *arg_str(const strargs *a, const char *key) {
  int i;
  for (i = 0; i < a->n; i++)
    if (strcmp(a->keys[i], key) == 0) return a->vals[i];
  return NULL;
}

static long arg_len(const strargs *a, const char *key) {
  int i;
  for (i = 0; i < a->n; i++)
    if (strcmp(a->keys[i], key) == 0) return a->vlens[i];
  return -1;
}

/* Decode `{op, args_map}` from a request frame into op + collected args. */
static int decode_request(const char *buf, char *op, strargs *a) {
  int idx = 0, ver = 0, arity = 0, marity = 0, i;
  a->n = 0;
  if (ei_decode_version(buf, &idx, &ver) != 0) return -1;
  if (ei_decode_tuple_header(buf, &idx, &arity) != 0 || arity != 2) return -1;
  if (ei_decode_atom(buf, &idx, op) != 0) return -1;
  if (ei_decode_map_header(buf, &idx, &marity) != 0) return -1;

  for (i = 0; i < marity; i++) {
    char key[512];
    long klen = 0;
    int type = 0, size = 0;
    if (ei_decode_binary(buf, &idx, key, &klen) != 0) return -1;
    key[klen] = '\0';
    if (ei_get_type(buf, &idx, &type, &size) != 0) return -1;
    if (type == ERL_BINARY_EXT) {
      if (a->n < 8 && (size_t)klen < sizeof(a->keys[0]) &&
          (size_t)size < sizeof(a->vals[0])) {
        long got = 0;
        memcpy(a->keys[a->n], key, (size_t)klen);
        a->keys[a->n][klen] = '\0';
        ei_decode_binary(buf, &idx, a->vals[a->n], &got);
        a->vals[a->n][got] = '\0';
        a->vlens[a->n] = got;
        a->n++;
      } else {
        ei_skip_term(buf, &idx);
      }
    } else {
      /* integer or other scalar — consume it */
      ei_skip_term(buf, &idx);
    }
  }
  return 0;
}

static void reply_ok_empty(ei_x_buff *x) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 0);
}

static void reply_error(ei_x_buff *x, const char *atom) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "error");
  ei_x_encode_atom(x, atom);
}

static void reply_handle(ei_x_buff *x, long long handle) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 1);
  ei_x_encode_atom(x, "handle");
  ei_x_encode_longlong(x, handle);
}

static void reply_stat(ei_x_buff *x) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 1);
  ei_x_encode_atom(x, "stat");
  ei_x_encode_map_header(x, 6);
  ei_x_encode_atom(x, "size");
  ei_x_encode_longlong(x, 1234);
  ei_x_encode_atom(x, "mode");
  ei_x_encode_longlong(x, 0100644);
  ei_x_encode_atom(x, "atime");
  ei_x_encode_longlong(x, 111);
  ei_x_encode_atom(x, "mtime");
  ei_x_encode_longlong(x, 222);
  ei_x_encode_atom(x, "ctime");
  ei_x_encode_longlong(x, 333);
  ei_x_encode_atom(x, "kind");
  ei_x_encode_atom(x, "file");
}

static int g_readdir_calls = 0;

static void reply_readdir(ei_x_buff *x) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  if (g_readdir_calls == 0) {
    ei_x_encode_map_header(x, 2);
    ei_x_encode_atom(x, "entry");
    ei_x_encode_map_header(x, 2);
    ei_x_encode_atom(x, "name");
    ei_x_encode_binary(x, "file.txt", 8);
    ei_x_encode_atom(x, "kind");
    ei_x_encode_atom(x, "file");
    ei_x_encode_atom(x, "eof");
    ei_x_encode_boolean(x, 0);
  } else {
    ei_x_encode_map_header(x, 1);
    ei_x_encode_atom(x, "eof");
    ei_x_encode_boolean(x, 1);
  }
  g_readdir_calls++;
}

static void reply_data(ei_x_buff *x, const char *data, long len) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 1);
  ei_x_encode_atom(x, "data");
  ei_x_encode_binary(x, data, len);
}

static void reply_written(ei_x_buff *x, long long n) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 1);
  ei_x_encode_atom(x, "written");
  ei_x_encode_longlong(x, n);
}

static void reply_statvfs(ei_x_buff *x) {
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "ok");
  ei_x_encode_map_header(x, 3);
  ei_x_encode_atom(x, "total_bytes");
  ei_x_encode_longlong(x, 1000);
  ei_x_encode_atom(x, "free_bytes");
  ei_x_encode_longlong(x, 400);
  ei_x_encode_atom(x, "available_bytes");
  ei_x_encode_longlong(x, 300);
}

/* Returns 0 on clean exit (parent closed), non-zero on a decode mismatch. */
static int run_responder(int fd) {
  char buf[8192];
  int len = 0;

  while (recv_frame_h(fd, buf, sizeof(buf), &len) == 0) {
    char op[MAXATOMLEN + 1];
    strargs a;
    ei_x_buff x;
    const char *sentinel;

    if (decode_request(buf, op, &a) != 0) {
      fprintf(stderr, "responder: bad request frame\n");
      return 2;
    }

    /* Validate a few representative encodings. */
    if (strcmp(op, "connect") == 0) {
      const char *v = arg_str(&a, "volume");
      if (v == NULL || strcmp(v, "vol1") != 0) {
        fprintf(stderr, "responder: connect volume mismatch\n");
        return 3;
      }
    } else if (strcmp(op, "pwrite") == 0) {
      const char *d = arg_str(&a, "data");
      if (d == NULL || arg_len(&a, "data") != 5 || memcmp(d, "hello", 5) != 0) {
        fprintf(stderr, "responder: pwrite data mismatch\n");
        return 4;
      }
    } else if (strcmp(op, "renameat") == 0) {
      const char *o = arg_str(&a, "old_path");
      const char *n = arg_str(&a, "new_path");
      if (o == NULL || n == NULL || strcmp(o, "/a") != 0 ||
          strcmp(n, "/b") != 0) {
        fprintf(stderr, "responder: renameat path mismatch\n");
        return 5;
      }
    }

    /* Error sentinels: any path-ish arg requesting a specific failure. */
    sentinel = arg_str(&a, "path");
    if (sentinel == NULL) sentinel = arg_str(&a, "old_path");
    if (sentinel != NULL && strcmp(sentinel, "__enoent__") == 0) {
      reply_error(&x, "enoent");
      send_frame_h(fd, &x);
      ei_x_free(&x);
      continue;
    }
    if (sentinel != NULL && strcmp(sentinel, "__eexist__") == 0) {
      reply_error(&x, "eexist");
      send_frame_h(fd, &x);
      ei_x_free(&x);
      continue;
    }

    if (strcmp(op, "stat") == 0 || strcmp(op, "lstat") == 0 ||
        strcmp(op, "fstat") == 0) {
      reply_stat(&x);
    } else if (strcmp(op, "openat") == 0 || strcmp(op, "fdopendir") == 0) {
      reply_handle(&x, 42);
    } else if (strcmp(op, "pread") == 0) {
      reply_data(&x, "hello", 5);
    } else if (strcmp(op, "pwrite") == 0) {
      reply_written(&x, 5);
    } else if (strcmp(op, "readdir") == 0) {
      reply_readdir(&x);
    } else if (strcmp(op, "disk_free") == 0 || strcmp(op, "fstatvfs") == 0) {
      reply_statvfs(&x);
    } else if (strcmp(op, "fchown") == 0) {
      reply_error(&x, "enosys");
    } else {
      reply_ok_empty(&x);
    }

    send_frame_h(fd, &x);
    ei_x_free(&x);
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* client driver (parent)                                              */
/* ------------------------------------------------------------------ */

static void drive_client(int fd) {
  nw_conn conn;
  nw_stat_t st;
  nw_dirent_t ent;
  nw_statvfs_t vfs;
  uint64_t handle = 0, written = 0;
  uint8_t rbuf[64];
  size_t rlen = 0;
  int eof = 0;

  nw_attach(&conn, fd);

  CHECK(nw_connect(&conn, "vol1") == 0, "connect");
  CHECK(nw_stat(&conn, "/a.txt", &st) == 0, "stat");
  CHECK(st.size == 1234, "stat.size");
  CHECK(st.mode == 0100644, "stat.mode");
  CHECK(st.atime == 111 && st.mtime == 222 && st.ctime == 333, "stat.times");
  CHECK(st.kind == NW_KIND_FILE, "stat.kind");

  CHECK(nw_lstat(&conn, "/a.txt", &st) == 0, "lstat");
  CHECK(nw_openat(&conn, "/a.txt", 0, 0644, &handle) == 0, "openat");
  CHECK(handle == 42, "openat.handle");
  CHECK(nw_fstat(&conn, handle, &st) == 0, "fstat");
  CHECK(st.size == 1234, "fstat.size");

  CHECK(nw_fchmod(&conn, handle, 0755) == 0, "fchmod");
  CHECK(nw_fntimes(&conn, handle, 10, 20) == 0, "fntimes");

  /* fchown is :enosys on the server side. */
  CHECK(nw_fchown(&conn, handle, 0, 0) == -1, "fchown returns -1");
  CHECK(errno == ENOSYS, "fchown errno ENOSYS");

  CHECK(nw_pread(&conn, handle, 0, 5, rbuf, sizeof(rbuf), &rlen) == 0, "pread");
  CHECK(rlen == 5 && memcmp(rbuf, "hello", 5) == 0, "pread.data");

  CHECK(nw_pwrite(&conn, handle, 0, (const uint8_t *)"hello", 5, &written) == 0,
        "pwrite");
  CHECK(written == 5, "pwrite.written");

  CHECK(nw_ftruncate(&conn, handle, 100) == 0, "ftruncate");
  CHECK(nw_close(&conn, handle) == 0, "close");

  CHECK(nw_fdopendir(&conn, "/dir", &handle) == 0, "fdopendir");
  CHECK(handle == 42, "fdopendir.handle");
  CHECK(nw_readdir(&conn, handle, &ent, &eof) == 0, "readdir first");
  CHECK(eof == 0, "readdir not eof");
  CHECK(strcmp(ent.name, "file.txt") == 0, "readdir.name");
  CHECK(ent.kind == NW_KIND_FILE, "readdir.kind");
  CHECK(nw_readdir(&conn, handle, &ent, &eof) == 0, "readdir second");
  CHECK(eof == 1, "readdir eof");
  CHECK(nw_closedir(&conn, handle) == 0, "closedir");

  CHECK(nw_mkdirat(&conn, "/newdir", 0755) == 0, "mkdirat");
  CHECK(nw_unlinkat(&conn, "/gone") == 0, "unlinkat");
  CHECK(nw_renameat(&conn, "/a", "/b") == 0, "renameat");

  CHECK(nw_disk_free(&conn, &vfs) == 0, "disk_free");
  CHECK(vfs.total_bytes == 1000 && vfs.free_bytes == 400 &&
            vfs.available_bytes == 300,
        "disk_free fields");
  CHECK(nw_fstatvfs(&conn, &vfs) == 0, "fstatvfs");

  /* Error-arm coverage. */
  CHECK(nw_stat(&conn, "__enoent__", &st) == -1, "stat enoent ret");
  CHECK(errno == ENOENT, "stat enoent errno");
  CHECK(nw_openat(&conn, "__eexist__", 0, 0644, &handle) == -1,
        "openat eexist ret");
  CHECK(errno == EEXIST, "openat eexist errno");

  CHECK(nw_disconnect(&conn) == 0, "disconnect");
}

static void test_errno_table(void) {
  CHECK(nw_errno_of_atom("enoent") == ENOENT, "errno enoent");
  CHECK(nw_errno_of_atom("eacces") == EACCES, "errno eacces");
  CHECK(nw_errno_of_atom("enosys") == ENOSYS, "errno enosys");
  CHECK(nw_errno_of_atom("ebadf") == EBADF, "errno ebadf");
  CHECK(nw_errno_of_atom("exdev") == EXDEV, "errno exdev");
  CHECK(nw_errno_of_atom("enotempty") == ENOTEMPTY, "errno enotempty");
  CHECK(nw_errno_of_atom("enotconn") == ENOTCONN, "errno enotconn");
  /* Unknown / NeonFS-specific reasons fall through to EIO. */
  CHECK(nw_errno_of_atom("some_neonfs_reason") == EIO, "errno fallthrough");
}

int main(void) {
  int sv[2];
  pid_t pid;

  test_errno_table();

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
    perror("socketpair");
    return 1;
  }

  pid = fork();
  if (pid < 0) {
    perror("fork");
    return 1;
  }

  if (pid == 0) {
    int rc;
    close(sv[0]);
    rc = run_responder(sv[1]);
    close(sv[1]);
    _exit(rc);
  }

  close(sv[1]);
  drive_client(sv[0]);
  close(sv[0]); /* responder sees EOF and exits */

  {
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "responder clean exit");
  }

  if (g_failures == 0) {
    printf("ok: all wire round-trips passed\n");
    return 0;
  }
  fprintf(stderr, "%d check(s) failed\n", g_failures);
  return 1;
}
