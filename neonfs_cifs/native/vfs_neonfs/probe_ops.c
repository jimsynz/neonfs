/* probe_ops.c — see probe_ops.h. */

#include "probe_ops.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define PCHECK(cond, msg)                                                  \
  do {                                                                     \
    if (!(cond)) {                                                         \
      fprintf(stderr, "FAIL: %s (%s:%d) errno=%d\n", (msg), __FILE__,      \
              __LINE__, errno);                                            \
      failures++;                                                          \
    }                                                                      \
  } while (0)

int nw_probe_run(nw_conn *conn) {
  int failures = 0;
  nw_stat_t st;
  nw_dirent_t ent;
  nw_statvfs_t vfs;
  uint64_t handle = 0, written = 0;
  uint8_t rbuf[64];
  size_t rlen = 0;
  int eof = 0;

  PCHECK(nw_connect(conn, "vol1") == 0, "connect");

  PCHECK(nw_stat(conn, "/a.txt", &st) == 0, "stat");
  PCHECK(st.size == 1234, "stat.size");
  PCHECK(st.mode == 0100644, "stat.mode");
  PCHECK(st.atime == 111 && st.mtime == 222 && st.ctime == 333, "stat.times");
  PCHECK(st.kind == NW_KIND_FILE, "stat.kind");

  PCHECK(nw_lstat(conn, "/a.txt", &st) == 0, "lstat");

  PCHECK(nw_openat(conn, "/a.txt", 0, 0644, &handle) == 0, "openat");
  PCHECK(handle == 42, "openat.handle");
  PCHECK(nw_fstat(conn, handle, &st) == 0, "fstat");
  PCHECK(st.size == 1234, "fstat.size");

  PCHECK(nw_fchmod(conn, handle, 0755) == 0, "fchmod");
  PCHECK(nw_fntimes(conn, handle, 10, 20) == 0, "fntimes");

  /* fchown is :enosys on the server side. */
  PCHECK(nw_fchown(conn, handle, 0, 0) == -1, "fchown returns -1");
  PCHECK(errno == ENOSYS, "fchown errno ENOSYS");

  PCHECK(nw_pread(conn, handle, 0, 5, rbuf, sizeof(rbuf), &rlen) == 0, "pread");
  PCHECK(rlen == 5 && memcmp(rbuf, "hello", 5) == 0, "pread.data");

  PCHECK(nw_pwrite(conn, handle, 0, (const uint8_t *)"hello", 5, &written) == 0,
         "pwrite");
  PCHECK(written == 5, "pwrite.written");

  PCHECK(nw_ftruncate(conn, handle, 100) == 0, "ftruncate");
  PCHECK(nw_fsync(conn, handle) == 0, "fsync");
  PCHECK(nw_close(conn, handle) == 0, "close");

  PCHECK(nw_fdopendir(conn, "/dir", &handle) == 0, "fdopendir");
  PCHECK(handle == 42, "fdopendir.handle");
  PCHECK(nw_readdir(conn, handle, &ent, &eof) == 0, "readdir first");
  PCHECK(eof == 0, "readdir not eof");
  PCHECK(strcmp(ent.name, "file.txt") == 0, "readdir.name");
  PCHECK(ent.kind == NW_KIND_FILE, "readdir.kind");
  PCHECK(nw_readdir(conn, handle, &ent, &eof) == 0, "readdir second");
  PCHECK(eof == 1, "readdir eof");
  PCHECK(nw_closedir(conn, handle) == 0, "closedir");

  PCHECK(nw_mkdirat(conn, "/newdir", 0755) == 0, "mkdirat");
  PCHECK(nw_unlinkat(conn, "/gone") == 0, "unlinkat");
  PCHECK(nw_renameat(conn, "/a", "/b") == 0, "renameat");

  PCHECK(nw_disk_free(conn, &vfs) == 0, "disk_free");
  PCHECK(vfs.total_bytes == 1000 && vfs.free_bytes == 400 &&
             vfs.available_bytes == 300,
         "disk_free fields");
  PCHECK(nw_fstatvfs(conn, &vfs) == 0, "fstatvfs");

  /* Error-arm coverage. */
  PCHECK(nw_stat(conn, "__enoent__", &st) == -1, "stat enoent ret");
  PCHECK(errno == ENOENT, "stat enoent errno");
  PCHECK(nw_openat(conn, "__eexist__", 0, 0644, &handle) == -1,
         "openat eexist ret");
  PCHECK(errno == EEXIST, "openat eexist errno");

  PCHECK(nw_disconnect(conn) == 0, "disconnect");

  return failures;
}
