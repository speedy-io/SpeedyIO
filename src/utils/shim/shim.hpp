#ifndef _SHIM_HPP
#define _SHIM_HPP

#include <cstdint>

void link_shim_functions(void);

int real_openat(int dirfd, const char *pathname, int flags, mode_t mode);
int real_open(const char *pathname, int flags, mode_t mode);
int real_creat(const char *pathname, mode_t mode);
FILE *real_fopen(const char *filename, const char *mode);

int real_dup(int oldfd);
int real_dup2(int oldfd, int new_fd);
int real_dup3(int oldfd, int new_fd, int flags);

int real_link(const char *oldpath, const char *newpath);
int real_linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
int real_symlink(const char *target, const char *linkpath);
int real_symlinkat(const char *target, int newdirfd, const char *linkpath);

int real_rename(const char *oldpath, const char *newpath);
int real_renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath);
int real_renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags);

int real_truncate(const char *path, off_t length);
int real_ftruncate(int fd, off_t length);

off_t real_lseek(int fd, off_t offset, int whence);
off64_t real_lseek64(int fd, off64_t offset, int whence);
int real_fseek(FILE *stream, long offset, int whence);
int real_fseeko(FILE *stream, off_t offset, int whence);

size_t real_fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
ssize_t real_pread(int fd, void *data, size_t size, off_t offset);
ssize_t real_pread64(int fd, void *data, size_t size, off64_t offset);
ssize_t real_read(int fd, void *data, size_t size);

ssize_t real_write(int fd, const void *data, size_t size);
ssize_t real_pwrite(int fd, const void *data, size_t size, off_t offset);
ssize_t real_pwrite64(int fd, const void *data, size_t size, off64_t offset);
char *real_fgets( char *str, int num, FILE *stream);
size_t real_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream);

int real_fclose(FILE *stream);
int real_close(int fd);

int real_posix_fadvise(int fd, off_t offset, off_t len, int advice);
int real_posix_fadvise64(int fd, off_t offset, off_t len, int advice);
int real_fadvise(int fd, off_t offset, off_t len, int advice);
int real_fadvise64(int fd, off_t offset, off_t len, int advice);

size_t real_readahead(int fd, off_t offset, size_t count);
int real_madvise(void *addr, size_t length, int advice);

int real_unlink(const char *pathname);
int real_unlinkat(int dirfd, const char *pathname, int flags);

int real_clone(int (*fn)(void *), void *child_stack, int flags, void *arg,
                pid_t *ptid, void *newtls, pid_t *ctid);
uid_t real_getuid();

int real_fcntl(int fd, int cmd, uintptr_t arg);

int real_fsync(int fd);
int real_fdatasync(int fd);

void *real_mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);

#endif // _SHIM_HPP
