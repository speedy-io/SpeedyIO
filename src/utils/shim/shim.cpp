#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>

#include <cstdint>

/*The following are the intercepted function definitions*/
typedef int (*real_open_t)(const char *, int, ...);
typedef int (*real_openat_t)(int, const char *, int, ...);
typedef int (*real_creat_t)(const char *, mode_t);
typedef FILE *(*real_fopen_t)(const char *, const char *);

typedef int(*real_dup_t)(int);
typedef int(*real_dup2_t)(int, int);
typedef int(*real_dup3_t)(int, int, int);

typedef int (*real_link_t)(const char *, const char *);
typedef int (*real_linkat_t)(int, const char *, int, const char *, int);
typedef int (*real_symlink_t)(const char *, const char *);
typedef int (*real_symlinkat_t)(const char *, int, const char *);

typedef int (*real_rename_t)(const char *, const char *);
typedef int (*real_renameat_t)(int, const char *, int, const char *);
typedef int (*real_renameat2_t)(int, const char *, int, const char *, unsigned int);

typedef int (*truncate_func_t)(const char *, off_t);
typedef int (*ftruncate_func_t)(int, off_t);

typedef off_t (*lseek_func_t)(int, off_t, int);
typedef off64_t (*lseek64_func_t)(int, off64_t, int);
typedef int (*fseek_func_t)(FILE *, long, int);
typedef int (*fseeko_func_t)(FILE *, off_t, int);

typedef ssize_t (*real_read_t)(int, void *, size_t);
typedef ssize_t (*real_pread_t)(int, void *, size_t, off_t);
typedef ssize_t (*real_pread64_t)(int, void *, size_t, off64_t);
typedef size_t (*real_fread_t)(void *, size_t, size_t,FILE *);

typedef char *(*real_fgets_t)(char *, int, FILE *);
typedef ssize_t (*real_write_t)(int, const void *, size_t);
typedef ssize_t (*real_pwrite_t)(int, const void *, size_t, off_t);
typedef ssize_t (*real_pwrite64_t)(int, const void *, size_t, off64_t);
typedef size_t (*real_fwrite_t)(const void *, size_t, 
                size_t,FILE *);

typedef int (*real_fclose_t)(FILE *);
typedef int (*real_close_t)(int);
typedef uid_t (*real_getuid_t)(void);

typedef int (*real_unlink_t)(const char *);
typedef int (*real_unlinkat_t)(int, const char *, int flags);

typedef int (*real_posix_fadvise_t)(int, off_t, off_t, int);
typedef int (*real_posix_fadvise64_t)(int, off_t, off_t, int);
typedef int (*real_fadvise_t)(int, off_t, off_t, int);
typedef int (*real_fadvise64_t)(int, off_t, off_t, int);

typedef ssize_t (*real_readahead_t)(int, off64_t, size_t);
typedef int (*real_madvise_t)(void *, size_t, int);

typedef int (*real_clone_t)(int (void*), void *, int, void *, pid_t *, void *, pid_t *);

using real_fcntl_t = int (*)(int, int, ...);

typedef int (*real_fsync_t)(int);
typedef int (*real_fdatasync_t)(int);

typedef void *(*real_mmap_t)(void *, size_t, int, int, int, off_t);

real_fopen_t fopen_ptr = NULL;
real_open_t open_ptr = NULL;
real_openat_t openat_ptr = NULL;
real_creat_t creat_ptr = NULL;

real_dup_t dup_ptr = NULL;
real_dup2_t dup2_ptr = NULL;
real_dup3_t dup3_ptr = NULL;

real_link_t link_ptr = NULL;
real_linkat_t linkat_ptr = NULL;
real_symlink_t symlink_ptr = NULL;
real_symlinkat_t symlinkat_ptr = NULL;

real_rename_t rename_ptr = NULL;
real_renameat_t renameat_ptr = NULL;
real_renameat2_t renameat2_ptr = NULL;

truncate_func_t truncate_ptr = NULL;
ftruncate_func_t ftruncate_ptr = NULL;

lseek_func_t lseek_ptr = NULL;
lseek64_func_t lseek64_ptr = NULL;
fseek_func_t fseek_ptr = NULL;
fseeko_func_t fseeko_ptr = NULL;

real_pread_t pread_ptr = NULL;
real_pread64_t pread64_ptr = NULL;
real_read_t read_ptr = NULL;
real_fgets_t fgets_ptr = NULL;


real_write_t write_ptr = NULL;
real_pwrite_t pwrite_ptr = NULL;
real_pwrite64_t pwrite64_ptr = NULL;
real_fread_t fread_ptr = NULL;
real_fwrite_t fwrite_ptr = NULL;

real_fclose_t fclose_ptr = NULL;
real_close_t close_ptr = NULL;

real_unlink_t unlink_ptr = NULL;
real_unlinkat_t unlinkat_ptr = NULL;

real_clone_t clone_ptr = NULL;

real_fcntl_t fcntl_ptr = NULL;

real_fsync_t fsync_ptr = NULL;
real_fdatasync_t fdatasync_ptr = NULL;

real_mmap_t mmap_ptr = NULL;

/*Advise calls*/
real_posix_fadvise_t posix_fadvise_ptr = NULL;
real_posix_fadvise64_t posix_fadvise64_ptr = NULL;
real_fadvise_t fadvise_ptr = NULL;
real_fadvise64_t fadvise64_ptr = NULL;

real_readahead_t readahead_ptr = NULL;
real_madvise_t madvise_ptr = NULL;


void link_shim_functions(void){

        open_ptr = ((real_open_t)dlsym(RTLD_NEXT, "open"));
        openat_ptr = ((real_openat_t)dlsym(RTLD_NEXT, "openat"));
        creat_ptr = ((real_creat_t)dlsym(RTLD_NEXT, "creat"));
        fopen_ptr = (real_fopen_t)dlsym(RTLD_NEXT, "fopen");

        dup_ptr = ((real_dup_t)dlsym(RTLD_NEXT, "dup"));
        dup2_ptr = ((real_dup2_t)dlsym(RTLD_NEXT, "dup2"));
        dup3_ptr = ((real_dup3_t)dlsym(RTLD_NEXT, "dup3"));

        link_ptr = (real_link_t)dlsym(RTLD_NEXT, "link");
        linkat_ptr = (real_linkat_t)dlsym(RTLD_NEXT, "linkat");
        symlink_ptr = (real_symlink_t)dlsym(RTLD_NEXT, "symlink");
        symlinkat_ptr = (real_symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");

        rename_ptr = (real_rename_t)dlsym(RTLD_NEXT, "rename");
        renameat_ptr = (real_renameat_t)dlsym(RTLD_NEXT, "renameat");
        renameat2_ptr = (real_renameat2_t)dlsym(RTLD_NEXT, "renameat2");

        truncate_ptr = (truncate_func_t)dlsym(RTLD_NEXT, "truncate");
        ftruncate_ptr = (ftruncate_func_t)dlsym(RTLD_NEXT, "ftruncate");

        lseek_ptr = (lseek_func_t)dlsym(RTLD_NEXT, "lseek");
        lseek64_ptr = (lseek64_func_t)dlsym(RTLD_NEXT, "lseek64");
        fseek_ptr = (fseek_func_t)dlsym(RTLD_NEXT, "fseek");
        fseeko_ptr = (fseeko_func_t)dlsym(RTLD_NEXT, "fseeko");

        pread_ptr = (real_pread_t)dlsym(RTLD_NEXT, "pread");
        pread64_ptr = (real_pread64_t)dlsym(RTLD_NEXT, "pread64");
        read_ptr = (real_read_t)dlsym(RTLD_NEXT, "read");
        fread_ptr = (real_fread_t)dlsym(RTLD_NEXT, "fread");

        write_ptr = ((real_write_t)dlsym(RTLD_NEXT, "write"));
        pwrite_ptr = (real_pwrite_t)dlsym(RTLD_NEXT, "pwrite");
        pwrite64_ptr = (real_pwrite64_t)dlsym(RTLD_NEXT, "pwrite64");
        fwrite_ptr = (real_fwrite_t)dlsym(RTLD_NEXT, "fwrite");
        fgets_ptr = (real_fgets_t)dlsym(RTLD_NEXT, "fgets");

        close_ptr = ((real_close_t)dlsym(RTLD_NEXT, "close"));
        fclose_ptr = ((real_fclose_t)dlsym(RTLD_NEXT, "fclose"));

        unlink_ptr = ((real_unlink_t)dlsym(RTLD_NEXT, "unlink"));
        unlinkat_ptr = ((real_unlinkat_t)dlsym(RTLD_NEXT, "unlinkat"));

        posix_fadvise_ptr = (real_posix_fadvise_t)dlsym(RTLD_NEXT, "posix_fadvise");
        posix_fadvise64_ptr = (real_posix_fadvise64_t)dlsym(RTLD_NEXT, "posix_fadvise64");
        fadvise_ptr = (real_fadvise_t)dlsym(RTLD_NEXT, "fadvise");
        fadvise64_ptr = (real_fadvise64_t)dlsym(RTLD_NEXT, "fadvise64");

        readahead_ptr = (real_readahead_t)dlsym(RTLD_NEXT, "readahead");
        madvise_ptr = (real_madvise_t)dlsym(RTLD_NEXT, "madvise");

        clone_ptr = (real_clone_t)dlsym(RTLD_NEXT, "clone");

        fcntl_ptr = reinterpret_cast<real_fcntl_t>(dlsym(RTLD_NEXT, "fcntl"));

        fsync_ptr = (real_fsync_t)dlsym(RTLD_NEXT, "fsync");
        fdatasync_ptr = (real_fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");

        mmap_ptr = (real_mmap_t)dlsym(RTLD_NEXT, "mmap");

        return;
}


/*Open functions*/

int real_openat(int dirfd, const char *pathname, int flags, mode_t mode){
        if(!openat_ptr)
                openat_ptr = ((real_openat_t)dlsym(RTLD_NEXT, "openat"));
        return ((real_openat_t)openat_ptr)(dirfd, pathname, flags, mode);
}

int real_open(const char *pathname, int flags, mode_t mode){
        if(!open_ptr)
                open_ptr = ((real_open_t)dlsym(RTLD_NEXT, "open"));
        return ((real_open_t)open_ptr)(pathname, flags, mode);
}

int real_creat(const char *pathname, mode_t mode){
        if(!creat_ptr)
                creat_ptr = ((real_creat_t)dlsym(RTLD_NEXT, "creat"));
        return ((real_creat_t)creat_ptr)(pathname, mode);
}

FILE *real_fopen(const char *filename, const char *mode){
        if(!fopen_ptr)
                fopen_ptr = (real_fopen_t)dlsym(RTLD_NEXT, "fopen");
        return ((real_fopen_t)fopen_ptr)(filename, mode);
}

/*Dup functions*/

int real_dup(int oldfd){
        if(!dup_ptr)
                dup_ptr = (real_dup_t)dlsym(RTLD_NEXT, "dup");
        return ((real_dup_t)dup_ptr)(oldfd);
}

int real_dup2(int oldfd, int newfd){
        if(!dup2_ptr)
                dup2_ptr = (real_dup2_t)dlsym(RTLD_NEXT, "dup2");
        return ((real_dup2_t)dup2_ptr)(oldfd, newfd);
}

int real_dup3(int oldfd, int newfd, int flags){
        if(!dup3_ptr)
                dup3_ptr = (real_dup3_t)dlsym(RTLD_NEXT, "dup3");
        return ((real_dup3_t)dup3_ptr)(oldfd, newfd, flags);
}

/*Link functions*/

int real_link(const char *oldpath, const char *newpath){
        if(!link_ptr)
                link_ptr = (real_link_t)dlsym(RTLD_NEXT, "link");
        return ((real_link_t)link_ptr)(oldpath, newpath);
}

int real_linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags){
        if(!linkat_ptr)
                linkat_ptr = (real_linkat_t)dlsym(RTLD_NEXT, "linkat");
        return ((real_linkat_t)linkat_ptr)(olddirfd, oldpath, newdirfd, newpath, flags);
}

int real_symlink(const char *target, const char *linkpath){
        if(!symlink_ptr)
                symlink_ptr = (real_symlink_t)dlsym(RTLD_NEXT, "symlink");
        return ((real_symlink_t)symlink_ptr)(target, linkpath);
}

int real_symlinkat(const char *target, int newdirfd, const char *linkpath){
        if(!symlinkat_ptr)
                symlinkat_ptr = (real_symlinkat_t)dlsym(RTLD_NEXT, "symlinkat");
        return ((real_symlinkat_t)symlinkat_ptr)(target, newdirfd, linkpath);
}

/*Rename functions*/

int real_rename(const char *oldpath, const char *newpath){
        if(!rename_ptr)
                rename_ptr = (real_rename_t)dlsym(RTLD_NEXT, "rename");
        return ((real_rename_t)rename_ptr)(oldpath, newpath);
}

int real_renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath){
        if(!renameat_ptr)
                renameat_ptr = (real_renameat_t)dlsym(RTLD_NEXT, "renameat");
        return ((real_renameat_t)renameat_ptr)(olddirfd, oldpath, newdirfd, newpath);
}

int real_renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags){
        if(!renameat2_ptr)
                renameat2_ptr = (real_renameat2_t)dlsym(RTLD_NEXT, "renameat2");
        return ((real_renameat2_t)renameat2_ptr)(olddirfd, oldpath, newdirfd, newpath, flags);
}

/*truncate functions*/

int real_truncate(const char *path, off_t length){
        if(!truncate_ptr)
                truncate_ptr = (truncate_func_t)dlsym(RTLD_NEXT, "truncate");
        return ((truncate_func_t)truncate_ptr)(path, length);
}

int real_ftruncate(int fd, off_t length){
        if(!ftruncate_ptr)
                ftruncate_ptr = (ftruncate_func_t)dlsym(RTLD_NEXT, "ftruncate");
        return ((ftruncate_func_t)ftruncate_ptr)(fd, length);
}

int real_fseek(FILE *stream, long offset, int whence){
        if(!fseek_ptr)
                fseek_ptr = (fseek_func_t)dlsym(RTLD_NEXT, "fseek");
        return ((fseek_func_t)fseek_ptr)(stream, offset, whence);
}

int real_fseeko(FILE *stream, off_t offset, int whence){
        if(!fseeko_ptr)
                fseeko_ptr = (fseeko_func_t)dlsym(RTLD_NEXT, "fseeko");
        return ((fseeko_func_t)fseeko_ptr)(stream, offset, whence);
}

/*seek functions*/

off_t real_lseek(int fd, off_t offset, int whence){
        if(!lseek_ptr)
                lseek_ptr = (lseek_func_t)dlsym(RTLD_NEXT, "lseek");
        return ((lseek_func_t)lseek_ptr)(fd, offset, whence);
}

off64_t real_lseek64(int fd, off64_t offset, int whence){
        if(!lseek64_ptr)
                lseek64_ptr = (lseek64_func_t)dlsym(RTLD_NEXT, "lseek64");
        return ((lseek64_func_t)lseek64_ptr)(fd, offset, whence);
}


/*Read functions*/

size_t real_fread(void *ptr, size_t size, size_t nmemb, FILE *stream){
        if(!fread_ptr)
                fread_ptr = (real_fread_t)dlsym(RTLD_NEXT, "fread");
        return ((real_fread_t)fread_ptr)(ptr, size, nmemb, stream);
}

ssize_t real_pread(int fd, void *data, size_t size, off_t offset){
        if(!pread_ptr)
                pread_ptr = (real_pread_t)dlsym(RTLD_NEXT, "pread");
        return ((real_pread_t)pread_ptr)(fd, data, size, offset);
}

ssize_t real_pread64(int fd, void *data, size_t size, off64_t offset){
        if(!pread64_ptr)
                pread64_ptr = (real_pread64_t)dlsym(RTLD_NEXT, "pread64");
        return ((real_pread64_t)pread64_ptr)(fd, data, size, offset);
}

ssize_t real_read(int fd, void *data, size_t size){
        if(!read_ptr)
                read_ptr = (real_read_t)dlsym(RTLD_NEXT, "read");
        return ((real_read_t)read_ptr)(fd, data, size);
}

/*Write functions*/

ssize_t real_write(int fd, const void *data, size_t size){
        if(!write_ptr)
                write_ptr = ((real_write_t)dlsym(RTLD_NEXT, "write"));
        return ((real_write_t)write_ptr)(fd, data, size);
}

ssize_t real_pwrite(int fd, const void *data, size_t size, off_t offset){
        if(!pwrite_ptr)
                pwrite_ptr = (real_pwrite_t)dlsym(RTLD_NEXT, "pwrite");
        return ((real_pwrite_t)pwrite_ptr)(fd, data, size, offset);
}

ssize_t real_pwrite64(int fd, const void *data, size_t size, off64_t offset){
        if(!pwrite64_ptr)
                pwrite64_ptr = (real_pwrite64_t)dlsym(RTLD_NEXT, "pwrite64");
        return ((real_pwrite64_t)pwrite64_ptr)(fd, data, size, offset);
}

char *real_fgets( char *str, int num, FILE *stream ){
        if(!fgets_ptr)
                fgets_ptr = (real_fgets_t)dlsym(RTLD_NEXT, "fgets");
        return ((real_fgets_t)fgets_ptr)(str, num, stream);
}

size_t real_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream){
        if(!fwrite_ptr)
                fwrite_ptr = (real_fwrite_t)dlsym(RTLD_NEXT, "fwrite");

        return ((real_fwrite_t)fwrite_ptr)(ptr, size, nmemb, stream);
}

/*Close functions*/

int real_fclose(FILE *stream){
        if(!fclose_ptr)
                fclose_ptr = ((real_fclose_t)dlsym(RTLD_NEXT, "fclose"));
        return ((real_fclose_t)fclose_ptr)(stream);
}

int real_close(int fd){
        if(!close_ptr)
                close_ptr = ((real_close_t)dlsym(RTLD_NEXT, "close"));
        return ((real_close_t)close_ptr)(fd);
}

/*Prefetch functions*/

int real_posix_fadvise(int fd, off_t offset, off_t len, int advice){
        if(!posix_fadvise_ptr)
                posix_fadvise_ptr = (real_posix_fadvise_t)dlsym(RTLD_NEXT, "posix_fadvise");
        return ((real_posix_fadvise_t)posix_fadvise_ptr)(fd, offset, len, advice);
}

int real_posix_fadvise64(int fd, off_t offset, off_t len, int advice){
        if(!posix_fadvise64_ptr)
                posix_fadvise64_ptr = (real_posix_fadvise64_t)dlsym(RTLD_NEXT, "posix_fadvise64");
        return ((real_posix_fadvise64_t)posix_fadvise64_ptr)(fd, offset, len, advice);
}

int real_fadvise(int fd, off_t offset, off_t len, int advice){
        if(!fadvise_ptr)
                fadvise_ptr = (real_fadvise_t)dlsym(RTLD_NEXT, "fadvise");
        return ((real_fadvise_t)fadvise_ptr)(fd, offset, len, advice);
}

int real_fadvise64(int fd, off_t offset, off_t len, int advice){
        if(!fadvise64_ptr)
                fadvise64_ptr = (real_fadvise64_t)dlsym(RTLD_NEXT, "fadvise64");
        return ((real_fadvise64_t)fadvise64_ptr)(fd, offset, len, advice);
}

ssize_t real_readahead(int fd, off_t offset, size_t count){
        if(!readahead_ptr)
                readahead_ptr = (real_readahead_t)dlsym(RTLD_NEXT, "readahead");

        return ((real_readahead_t)readahead_ptr)(fd, offset, count);
}

int real_madvise(void *addr, size_t length, int advice){
        if(!madvise_ptr)
                madvise_ptr = (real_madvise_t)dlsym(RTLD_NEXT, "madvise");
        return ((real_madvise_t)madvise_ptr)(addr, length, advice);
}

/*Clone functions*/

int real_unlink(const char *pathname){
        if(!unlink_ptr)
                unlink_ptr = (real_unlink_t)dlsym(RTLD_NEXT, "unlink");
        return ((real_unlink_t)unlink_ptr)(pathname);
}

int real_unlinkat(int dirfd, const char *pathname, int flags){
        if(!unlinkat_ptr)
                unlinkat_ptr = (real_unlinkat_t)dlsym(RTLD_NEXT, "unlinkat");
        return ((real_unlinkat_t)unlinkat_ptr)(dirfd, pathname, flags);
}

int real_clone(int (*fn)(void *), void *child_stack, int flags, void *arg,
                pid_t *ptid, void *newtls, pid_t *ctid){
        if(!clone_ptr)
                clone_ptr = (real_clone_t)dlsym(RTLD_NEXT, "clone");
        return ((real_clone_t)clone_ptr)(fn, child_stack, flags, arg, ptid, newtls, ctid);

}

int real_fcntl(int fd, int cmd, uintptr_t arg)
{
    switch (cmd) {
    case F_GETLK:
    case F_SETLK:
    case F_SETLKW:
        /* ptr‑based commands */
        return fcntl_ptr(fd, cmd,
                          reinterpret_cast<struct flock *>(arg));

    default:
        /* int‑based commands */
        return fcntl_ptr(fd, cmd, static_cast<int>(arg));
    }
}


int real_fsync(int fd){
        if(!fsync_ptr)
                fsync_ptr = ((real_fsync_t)dlsym(RTLD_NEXT, "fsync"));
        return ((real_fsync_t)fsync_ptr)(fd);
}

int real_fdatasync(int fd){
        if(!fdatasync_ptr)
                fdatasync_ptr = ((real_fdatasync_t)dlsym(RTLD_NEXT, "fdatasync"));
        return ((real_fdatasync_t)fdatasync_ptr)(fd);
}

void *real_mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset){
        if(!mmap_ptr)
                mmap_ptr = (real_mmap_t)dlsym(RTLD_NEXT, "mmap");
        return ((real_mmap_t)mmap_ptr)(addr, length, prot, flags, fd, offset);
}

uid_t real_getuid(){
        return ((real_getuid_t)dlsym(RTLD_NEXT, "getuid"))();
}
