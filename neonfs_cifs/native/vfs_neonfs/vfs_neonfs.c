/*
 * vfs_neonfs.c — Samba VFS module bridging smbd to a NeonFS volume.
 *
 * A thin shim: every VFS callback marshals to the neonfs_cifs ETF protocol
 * over a Unix domain socket (via the wire client in wire.c) and translates
 * the reply back into what Samba expects. smbd handles all SMB protocol
 * complexity; this module only forwards filesystem ops. Same shape as
 * vfs_ceph (#116 / #384).
 *
 * Handle model (like vfs_ceph_new): the Elixir side mints opaque uint64
 * handles for open files/dirs; we stash them in the files_struct via Samba's
 * FSP extension mechanism, and report a fake (debug-only) fd back to the VFS
 * layer. Paths come straight from `smb_fname->base_name` (share-relative);
 * the neonfs_cifs Handler resolves them within the volume.
 */

#include "includes.h"
#include "smbd/smbd.h"
#include "system/filesys.h"

#include "wire.h"

#undef DBGC_CLASS
#define DBGC_CLASS DBGC_VFS

/* Per-tree-connect state, stored in the VFS handle. */
struct neonfs_config {
	nw_conn conn;
	uint64_t fd_index; /* source of debug-only fake fds */
};

/* Per-open-file/dir state, stored in the files_struct extension. */
struct neonfs_fh {
	uint64_t handle;    /* opaque handle minted by neonfs_cifs */
	struct dirent *de;  /* readdir scratch entry */
	int fd;             /* debug-only fake fd reported to the VFS layer */
	bool is_dir;
};

static const char *neonfs_socket_path(struct vfs_handle_struct *handle)
{
	return lp_parm_const_string(SNUM(handle->conn), "neonfs", "socket",
				    "/run/neonfs/cifs.sock");
}

static const char *neonfs_volume(struct vfs_handle_struct *handle,
				 const char *service)
{
	return lp_parm_const_string(SNUM(handle->conn), "neonfs", "volume",
				    service);
}

static int neonfs_next_fd(struct neonfs_config *cfg)
{
	/* Debug hint only; never used for real I/O (all ops go via the fh). */
	return (int)((cfg->fd_index++ % 1000000) + 1000);
}

static void neonfs_free_config(void **data)
{
	struct neonfs_config *cfg = (struct neonfs_config *)*data;

	if (cfg != NULL) {
		nw_disconnect(&cfg->conn);
		nw_close_conn(&cfg->conn);
	}
}

/* ---- connection lifecycle ---- */

static int neonfs_connect(struct vfs_handle_struct *handle, const char *service,
			  const char *user)
{
	struct neonfs_config *cfg = NULL;
	const char *sock = neonfs_socket_path(handle);
	const char *volume = neonfs_volume(handle, service);

	cfg = talloc_zero(handle->conn, struct neonfs_config);
	if (cfg == NULL) {
		errno = ENOMEM;
		return -1;
	}

	if (nw_dial(&cfg->conn, sock) != 0) {
		DBG_ERR("vfs_neonfs: dial %s failed: %s\n", sock,
			strerror(errno));
		talloc_free(cfg);
		return -1;
	}

	if (nw_connect(&cfg->conn, volume) != 0) {
		DBG_ERR("vfs_neonfs: connect volume %s failed: %s\n", volume,
			strerror(errno));
		nw_close_conn(&cfg->conn);
		talloc_free(cfg);
		return -1;
	}

	SMB_VFS_HANDLE_SET_DATA(handle, cfg, neonfs_free_config,
				struct neonfs_config, return -1);

	DBG_NOTICE("vfs_neonfs: connected volume=%s via %s\n", volume, sock);
	return 0;
}

static void neonfs_disconnect(struct vfs_handle_struct *handle)
{
	/* The handle data's free callback (neonfs_free_config) closes the
	 * wire connection when the config is torn down. */
	DBG_NOTICE("vfs_neonfs: disconnect\n");
}

/* ---- fsp extension helpers ---- */

static struct neonfs_config *neonfs_cfg(struct vfs_handle_struct *handle)
{
	struct neonfs_config *cfg = NULL;

	SMB_VFS_HANDLE_GET_DATA(handle, cfg, struct neonfs_config,
				return NULL);
	return cfg;
}

static struct neonfs_fh *neonfs_fh_get(struct vfs_handle_struct *handle,
				       const struct files_struct *fsp)
{
	return (struct neonfs_fh *)VFS_FETCH_FSP_EXTENSION(handle, fsp);
}

/* ---- stat translation ---- */

static mode_t neonfs_kind_mode(nw_kind kind)
{
	return (kind == NW_KIND_DIRECTORY) ? S_IFDIR : S_IFREG;
}

static void neonfs_fill_stat(SMB_STRUCT_STAT *dst, const nw_stat_t *src)
{
	ZERO_STRUCTP(dst);
	dst->st_ex_size = (off_t)src->size;
	dst->st_ex_mode = (mode_t)src->mode | neonfs_kind_mode(src->kind);
	dst->st_ex_nlink = 1;
	dst->st_ex_atime.tv_sec = (time_t)src->atime;
	dst->st_ex_mtime.tv_sec = (time_t)src->mtime;
	dst->st_ex_ctime.tv_sec = (time_t)src->ctime;
	dst->st_ex_btime.tv_sec = (time_t)src->ctime;
	dst->st_ex_blksize = 512;
	dst->st_ex_blocks = (blkcnt_t)((src->size + 511) / 512);
}

static int neonfs_stat(struct vfs_handle_struct *handle,
		       struct smb_filename *smb_fname)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	nw_stat_t st;

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	if (nw_stat(&cfg->conn, smb_fname->base_name, &st) != 0) {
		return -1;
	}
	neonfs_fill_stat(&smb_fname->st, &st);
	return 0;
}

static int neonfs_lstat(struct vfs_handle_struct *handle,
			struct smb_filename *smb_fname)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	nw_stat_t st;

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	if (nw_lstat(&cfg->conn, smb_fname->base_name, &st) != 0) {
		return -1;
	}
	neonfs_fill_stat(&smb_fname->st, &st);
	return 0;
}

static int neonfs_fstat(struct vfs_handle_struct *handle,
			struct files_struct *fsp, SMB_STRUCT_STAT *sbuf)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);
	nw_stat_t st;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	if (nw_fstat(&cfg->conn, fh->handle, &st) != 0) {
		return -1;
	}
	neonfs_fill_stat(sbuf, &st);
	return 0;
}

/* ---- open / I/O ---- */

static int neonfs_openat(struct vfs_handle_struct *handle,
			 const struct files_struct *dirfsp,
			 const struct smb_filename *smb_fname,
			 struct files_struct *fsp,
			 const struct vfs_open_how *how)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = NULL;
	uint64_t nwh = 0;

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	if (smb_fname->stream_name != NULL) {
		errno = ENOENT;
		return -1;
	}

	if (nw_openat(&cfg->conn, smb_fname->base_name, (int32_t)how->flags,
		      (uint32_t)how->mode, &nwh) != 0) {
		return -1;
	}

	fh = VFS_ADD_FSP_EXTENSION(handle, fsp, struct neonfs_fh, NULL);
	if (fh == NULL) {
		nw_close(&cfg->conn, nwh);
		errno = ENOMEM;
		return -1;
	}
	fh->handle = nwh;
	fh->is_dir = false;
	fh->fd = neonfs_next_fd(cfg);
	return fh->fd;
}

static int neonfs_close(struct vfs_handle_struct *handle,
			struct files_struct *fsp)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);
	int ret = 0;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	ret = nw_close(&cfg->conn, fh->handle);
	VFS_REMOVE_FSP_EXTENSION(handle, fsp);
	return ret;
}

static ssize_t neonfs_pread(struct vfs_handle_struct *handle,
			    struct files_struct *fsp, void *data, size_t n,
			    off_t offset)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);
	size_t got = 0;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	if (nw_pread(&cfg->conn, fh->handle, (int64_t)offset, (uint64_t)n, data,
		     n, &got) != 0) {
		return -1;
	}
	return (ssize_t)got;
}

static ssize_t neonfs_pwrite(struct vfs_handle_struct *handle,
			     struct files_struct *fsp, const void *data,
			     size_t n, off_t offset)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);
	uint64_t written = 0;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	if (nw_pwrite(&cfg->conn, fh->handle, (int64_t)offset,
		      (const uint8_t *)data, n, &written) != 0) {
		return -1;
	}
	return (ssize_t)written;
}

static int neonfs_ftruncate(struct vfs_handle_struct *handle,
			    struct files_struct *fsp, off_t offset)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_ftruncate(&cfg->conn, fh->handle, (uint64_t)offset);
}

static int neonfs_fchmod(struct vfs_handle_struct *handle,
			 struct files_struct *fsp, mode_t mode)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_fchmod(&cfg->conn, fh->handle, (uint32_t)mode);
}

static int neonfs_fchown(struct vfs_handle_struct *handle,
			 struct files_struct *fsp, uid_t uid, gid_t gid)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_fchown(&cfg->conn, fh->handle, (uint32_t)uid, (uint32_t)gid);
}

static int neonfs_fntimes(struct vfs_handle_struct *handle,
			  struct files_struct *fsp, struct smb_file_time *ft)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = neonfs_fh_get(handle, fsp);

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_fntimes(&cfg->conn, fh->handle, (int64_t)ft->atime.tv_sec,
			  (int64_t)ft->mtime.tv_sec);
}

/* ---- directories ---- */

static DIR *neonfs_fdopendir(struct vfs_handle_struct *handle,
			     files_struct *fsp, const char *mask,
			     uint32_t attributes)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = NULL;
	uint64_t nwh = 0;

	if (cfg == NULL) {
		errno = EBADF;
		return NULL;
	}
	if (nw_fdopendir(&cfg->conn, fsp->fsp_name->base_name, &nwh) != 0) {
		return NULL;
	}

	fh = VFS_ADD_FSP_EXTENSION(handle, fsp, struct neonfs_fh, NULL);
	if (fh == NULL) {
		nw_closedir(&cfg->conn, nwh);
		errno = ENOMEM;
		return NULL;
	}
	fh->handle = nwh;
	fh->is_dir = true;
	fh->fd = neonfs_next_fd(cfg);
	return (DIR *)fh;
}

static struct dirent *neonfs_readdir(struct vfs_handle_struct *handle,
				     struct files_struct *dirfsp, DIR *dirp)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = (struct neonfs_fh *)dirp;
	nw_dirent_t ent;
	int eof = 0;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return NULL;
	}
	if (fh->de == NULL) {
		fh->de = talloc_zero_size(dirfsp, sizeof(struct dirent));
		if (fh->de == NULL) {
			errno = ENOMEM;
			return NULL;
		}
	}
	if (nw_readdir(&cfg->conn, fh->handle, &ent, &eof) != 0) {
		return NULL;
	}
	if (eof) {
		return NULL;
	}
	strlcpy(fh->de->d_name, ent.name, sizeof(fh->de->d_name));
	fh->de->d_ino = 1;
	fh->de->d_off = 0;
	fh->de->d_reclen = sizeof(struct dirent);
	fh->de->d_type = (ent.kind == NW_KIND_DIRECTORY) ? DT_DIR : DT_REG;
	return fh->de;
}

static int neonfs_closedir(struct vfs_handle_struct *handle, DIR *dirp)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	struct neonfs_fh *fh = (struct neonfs_fh *)dirp;
	int ret = 0;

	if (cfg == NULL || fh == NULL) {
		errno = EBADF;
		return -1;
	}
	ret = nw_closedir(&cfg->conn, fh->handle);
	TALLOC_FREE(fh->de);
	/* The fsp extension itself is released when the directory fsp is torn
	 * down; closedir only receives the DIR* (our fh), not the fsp. */
	return ret;
}

static int neonfs_mkdirat(struct vfs_handle_struct *handle,
			  struct files_struct *dirfsp,
			  const struct smb_filename *smb_fname, mode_t mode)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_mkdirat(&cfg->conn, smb_fname->base_name, (uint32_t)mode);
}

/* ---- mutations ---- */

static int neonfs_unlinkat(struct vfs_handle_struct *handle,
			   struct files_struct *dirfsp,
			   const struct smb_filename *smb_fname, int flags)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_unlinkat(&cfg->conn, smb_fname->base_name);
}

static int neonfs_renameat(struct vfs_handle_struct *handle,
			   struct files_struct *srcdir_fsp,
			   const struct smb_filename *smb_fname_src,
			   struct files_struct *dstdir_fsp,
			   const struct smb_filename *smb_fname_dst,
			   const struct vfs_rename_how *how)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);

	if (cfg == NULL) {
		errno = EBADF;
		return -1;
	}
	return nw_renameat(&cfg->conn, smb_fname_src->base_name,
			   smb_fname_dst->base_name);
}

/* ---- filesystem ---- */

static uint64_t neonfs_disk_free(struct vfs_handle_struct *handle,
				 const struct smb_filename *smb_fname,
				 uint64_t *bsize, uint64_t *dfree,
				 uint64_t *dsize)
{
	struct neonfs_config *cfg = neonfs_cfg(handle);
	nw_statvfs_t vfs;
	uint64_t bs = 512;

	if (cfg == NULL) {
		errno = EBADF;
		return (uint64_t)-1;
	}
	if (nw_disk_free(&cfg->conn, &vfs) != 0) {
		return (uint64_t)-1;
	}
	*bsize = bs;
	*dfree = vfs.available_bytes / bs;
	*dsize = vfs.total_bytes / bs;
	return *dfree;
}

/* ---- registration ---- */

static struct vfs_fn_pointers vfs_neonfs_fns = {
	.connect_fn = neonfs_connect,
	.disconnect_fn = neonfs_disconnect,
	.disk_free_fn = neonfs_disk_free,
	.stat_fn = neonfs_stat,
	.lstat_fn = neonfs_lstat,
	.fstat_fn = neonfs_fstat,
	.openat_fn = neonfs_openat,
	.close_fn = neonfs_close,
	.pread_fn = neonfs_pread,
	.pwrite_fn = neonfs_pwrite,
	.ftruncate_fn = neonfs_ftruncate,
	.fchmod_fn = neonfs_fchmod,
	.fchown_fn = neonfs_fchown,
	.fntimes_fn = neonfs_fntimes,
	.fdopendir_fn = neonfs_fdopendir,
	.readdir_fn = neonfs_readdir,
	.closedir_fn = neonfs_closedir,
	.mkdirat_fn = neonfs_mkdirat,
	.unlinkat_fn = neonfs_unlinkat,
	.renameat_fn = neonfs_renameat,
};

static_decl_vfs;
NTSTATUS vfs_neonfs_init(TALLOC_CTX *ctx)
{
	return smb_register_vfs(SMB_VFS_INTERFACE_VERSION, "neonfs",
				&vfs_neonfs_fns);
}
