#define FUSE_USE_VERSION 26
#define D_FILE_OFFSET_BITS = 64
#include <fuse.h>
#include "hiredis.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <net/if.h>
#include <stdarg.h>
#include <pthread.h>
#include <fuse/fuse.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>


redisReply *reply;

int time_check[18][2];

char* func_list[18] = {"init", "destroy", "readdir", "getattr", "mkdir", "rmdir", "write", "read", "symlink", "readlink", "open", "create", "chown", "chmod", "unlink", "utimens", "access", "truncate"};

clock_t start, end;
int time_test = 1;

int _g_redis_port = 6379;
char _g_redis_host[100] = { "127.0.0.1" };
int _g_debug = 1;
int _g_read_only = 0;
char _g_mount[200] = { "/mnt/redis" };
pthread_mutex_t _g_lock = PTHREAD_MUTEX_INITIALIZER;
int _g_fast = 0;
/*
 * To test
 */
int _g_test = 1;

// Connecting
redisContext *_g_redis = NULL;

void
redis_alive()
{
    struct timeval timeout = { 5, 0 };    // 5 seconds
    redisReply *reply = NULL;

    /**
     * If we have a handle see if it is alive.
     */
    if (_g_redis != NULL)
    {
	reply = redisCommand(_g_redis, "ping");

	if ((reply != NULL) &&
		(reply->str != NULL) && (strcmp(reply->str, "PONG") == 0)){
	    freeReplyObject(reply);
	    return;
	}
	else
	{
	    if (reply != NULL)
		freeReplyObject(reply);
	}
    }

    /**
     * OK we have no handle, create a connection to the server.
     */
    _g_redis = redisConnectWithTimeout(_g_redis_host, _g_redis_port, timeout);
    if (_g_redis == NULL)
    {
	fprintf(stderr, "Failed to connect to redis on [%s:%d].\n",
		_g_redis_host, _g_redis_port);
	exit(1);
    }
    else
    {
	if (_g_debug)
	    fprintf(stderr, "Reconnected to redis server on [%s:%d]\n",
		    _g_redis_host, _g_redis_port);
    }
}

char *
get_basename(const char *path)
{
    char *basename = NULL;
    char *p = NULL;
    int len = 0;

    /**
     * Test input is sane.
     */
    if (path == NULL)
        return NULL;

    /**
     * Allocate memory for a copy.
     */
    len = strlen(path) + 2;
    basename = (char *)malloc(len);
    if (basename == NULL)
        return NULL;

    /**
     * Look for right-most "/"
     */
    p = strrchr(path, '/');

    if (p == NULL)
        p = (char *)path;
    else
        p += 1;

    /**
     * Copy from after the char to the start.
     */
    strcpy(basename, p);

    /**
     * Return.
     */
    return (basename);
}

int
get_depth(const char *path)
{
    int i, count;
    count = 0; 
    if (strlen(path) == 1) return 0;
    for (i = 0; path[i]!='\0'; i++)
    {
	if (path[i] == '/') count++;
	else continue;
    }

    return count;
}

void *
fs_init()
{
    start = clock();
    if (_g_debug)
	fprintf(stderr, "fs_init()\n");

    pthread_mutex_init(&_g_lock, NULL);
    redis_alive();
    end = clock();
    if(!time_check[0][1])
    {
    time_check[0][1] = 1;
    time_check[0][0] = 0;
    }
    time_check[0][0] = (time_check[0][0]*time_check[0][1] + (end - start))/++time_check[0][1];
    return 0;
}

void
fs_destroy()
{
    start = clock();
    if (_g_debug)
	fprintf(stderr, "fs_destroy()\n");
    end = clock();
    if(!time_check[1][1])
    {
    time_check[1][1] = 1;
    time_check[1][0] = 0;
    }
    time_check[1][0] = (time_check[1][0]*time_check[1][1] + (end - start))/++time_check[1][1];
    pthread_mutex_destroy(&_g_lock);

    if(time_test)
    {
        for(int i = 0; i < 18; i++)
	{
	    printf("running time %s: %dms\n", func_list[i], time_check[i][0]);
	}

    }

    start = clock();
    end = clock();
    printf("%dms\n", end - start);
}

int
is_directory(const char *path)
{
    int ret = 0;
    redisReply *reply = NULL;

    if (_g_debug)
	fprintf(stderr, "is_directroy(%s)\n", path);

    redis_alive();

    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "EXISTS %d%s:data", depth, path);

    if (reply->integer == 0)
	ret = 1;

    freeReplyObject(reply);

    return (ret);
}

static int
fs_readdir(const char *path,
	   void *buf,
	   fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    redisReply *subreply = NULL;
    int i;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
	fprintf(stderr, "fs_readdir(%s)\n", path);

    redis_alive();

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    int depth = get_depth(path);
    reply = redisCommand(_g_redis, "KEYS %d%s*:meta", depth + 1, path);
    


    for(i = 0; i < reply -> elements; i++)
    {
	subreply = redisCommand(_g_redis, "HGET %s NAME", ((reply -> element[i])->str));
	filler(buf, strdup(subreply->str), NULL, 0);
    }
   
    freeReplyObject(reply);
    freeReplyObject(subreply);
    end = clock();
    if(!time_check[2][1])
    {
    time_check[2][1] = 1;
    time_check[2][0] = 0;
    }
    time_check[2][0] = (time_check[2][0]*time_check[2][1] + (end - start))/++time_check[2][1];
    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_getattr(const char *path, struct stat *stbuf)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
        fprintf(stderr, "fs_getattr(%s);\n", path);

    redis_alive();
    
    memset(stbuf, 0, sizeof(struct stat));

    if (strlen(path) == 1)
    {
        stbuf->st_atime = time(NULL);
        stbuf->st_mtime = stbuf->st_atime;
        stbuf->st_ctime = stbuf->st_atime;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();

        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 1;

        pthread_mutex_unlock(&_g_lock);

	return 0;
    }

    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "EXISTS %d%s:meta", depth, path);

    if (reply->integer == 0)
    {
	pthread_mutex_unlock(&_g_lock);
	freeReplyObject(reply);
	return -ENOENT;
    }

    freeReplyObject(reply);

    redisAppendCommand(_g_redis, "HMGET %d%s:meta CTIME ATIME MTIME GID UID LINK", depth, path);

    redisGetReply(_g_redis, (void **)&reply);
    if ((reply->element[0] != NULL)
        && (reply->element[0]->type == REDIS_REPLY_STRING))
        stbuf->st_ctime = atoi(reply->element[0]->str);
    if ((reply->element[1] != NULL)
        && (reply->element[1]->type == REDIS_REPLY_STRING))
        stbuf->st_atime = atoi(reply->element[1]->str);
    if ((reply->element[2] != NULL)
        && (reply->element[2]->type == REDIS_REPLY_STRING))
        stbuf->st_mtime = atoi(reply->element[2]->str);
    if ((reply->element[3] != NULL)
        && (reply->element[3]->type == REDIS_REPLY_STRING))
        stbuf->st_gid = atoi(reply->element[3]->str);
    if ((reply->element[4] != NULL)
        && (reply->element[4]->type == REDIS_REPLY_STRING))
        stbuf->st_uid = atoi(reply->element[4]->str);
    if ((reply->element[5] != NULL)
        && (reply->element[5]->type == REDIS_REPLY_STRING))
        stbuf->st_nlink = atoi(reply->element[5]->str);
   /* if ((reply->element[6] != NULL)
        && (reply->element[6]->type == REDIS_REPLY_STRING))
        stbuf->st_ctime = atoi(reply->element[5]->str);*/
    freeReplyObject(reply);

    reply = redisCommand(_g_redis, "HMGET %d%s:meta TYPE MODE SIZE", depth, path);

    if ((reply != NULL) && (reply->element[0] != NULL)
        && (reply->element[0]->type == REDIS_REPLY_STRING))
    {

        if ((reply->element[1] != NULL)
            && (reply->element[1]->type == REDIS_REPLY_STRING))
        {
            stbuf->st_mode = atoi(reply->element[1]->str);
        }

        if (strcmp(reply->element[0]->str, "DIR") == 0)
        {
            stbuf->st_mode |= S_IFDIR;
        }
        else if (strcmp(reply->element[0]->str, "LINK") == 0)
        {
            stbuf->st_mode |= S_IFLNK;
            stbuf->st_nlink = 1;
            stbuf->st_size = 0;
        }
        else if (strcmp(reply->element[0]->str, "FILE") == 0)
        {
            if ((reply->element[2] != NULL)
                && (reply->element[2]->type == REDIS_REPLY_STRING))
            {
                if (_g_debug)
                    fprintf(stderr, "found file\n");
                stbuf->st_size = atoi(reply->element[2]->str);
            }
        }
        else
        {
            if (_g_debug)
                fprintf(stderr, "UNKNOWN ENTRY TYPE: %s\n",
                        reply->element[0]->str);
        }
    }
    freeReplyObject(reply);
    end = clock();
    if(!time_check[3][1])
    {
    time_check[3][1] = 1;
    time_check[3][0] = 0;
    }
    time_check[3][0] = (time_check[3][0]*time_check[3][1] + (end - start))/++time_check[3][1];


    pthread_mutex_unlock(&_g_lock);

    return 0;
}

static int
fs_mkdir(const char *path, mode_t mode)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
	fprintf(stderr, "fs_mkdir(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();
    
    char *entry = get_basename(path);

    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HSET %d%s:meta NAME %s TYPE DIR MODE %d UID %d GID %d SIZE %d CTIME %d MTIME %d ATIME %d LINK 1", depth, path, entry, mode, fuse_get_context()->uid, fuse_get_context()->gid, 0, time(NULL), time(NULL), time(NULL));

    freeReplyObject(reply);
    
    free(entry);
    end = clock();
    if(!time_check[4][1])
    {
    time_check[4][1] = 1;
    time_check[4][0] = 0;
    }
    time_check[4][0] = (time_check[4][0]*time_check[4][1] + (end - start))/++time_check[4][1];
    pthread_mutex_unlock(&_g_lock);
    return 0;
}

int
count_dir_ent(const char *path)
{
    redisReply *reply = NULL;
    int depth = get_depth(path);
    reply = redisCommand(_g_redis, "KEYS %d%s/*", depth + 1, path);
    int res = reply->elements;
    printf("%d", res);
    freeReplyObject(reply);
    return res;
}

static int
fs_rmdir(const char *path)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
	fprintf(stderr, "fs_rmdir(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();

    if (!is_directory(path))
    {
	pthread_mutex_unlock(&_g_lock);
	return -ENOENT;
    }

    if (count_dir_ent(path))
    {
	pthread_mutex_unlock(&_g_lock);
	return -ENOTEMPTY;
    }
    
    int depth = get_depth(path);
    printf("\nredis code: UNLINK %d%s:meta\n", depth, path);
    reply = redisCommand(_g_redis, "UNLINK %d%s:meta", depth, path);

    freeReplyObject(reply);
    end = clock();
    if(!time_check[0][1])
    {
    time_check[5][1] = 1;
    time_check[5][0] = 0;
    }
    time_check[5][0] = (time_check[5][0]*time_check[5][1] + (end - start))/++time_check[5][1];
    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_write(const char *path,
	 const char *buf,
	 size_t size, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
	fprintf(stderr, "fs_write(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();

    int depth = get_depth(path);

    if (offset == 0)
    {
	char *mem = malloc(size + 1);
	memcpy(mem, buf, size);

	if (_g_debug)
            fprintf(stderr, "fs_write->simple(%s);\n", path);

	redisAppendCommand(_g_redis, "HSET %d%s:meta SIZE %d MTIME %d", depth, path, size, time(NULL));
        redisGetReply(_g_redis, (void **)&reply);
        freeReplyObject(reply);
	    
	redisAppendCommand(_g_redis, "SET %d%s:data %b", depth, path, mem, size);
	redisGetReply(_g_redis, (void **)&reply);
	freeReplyObject(reply);
	    
	free(mem);
    }
    else 
    {
	if (_g_debug)
            fprintf(stderr, "fs_write->offsetted(%s);\n", path);

	char *mem = malloc(size);
	memcpy(mem, buf, size);

	redisAppendCommand(_g_redis, "HINCRBY %d%s:meta SIZE %d", depth, path, size);
	redisAppendCommand(_g_redis, "APPEND %d%s:data %b", depth, path, mem, size);

	if (!_g_fast)
	{
	    redisAppendCommand(_g_redis, "HSET %d%s:meta MTIME %d", depth, path, time(NULL));
	    redisGetReply(_g_redis, (void **)&reply);
	    freeReplyObject(reply);
	}

	redisGetReply(_g_redis, (void **)&reply);
	freeReplyObject(reply);
	redisGetReply(_g_redis, (void **)&reply);
	freeReplyObject(reply);

	free(mem);
    }
    end = clock();
    if(!time_check[6][1])
    {
    time_check[6][1] = 1;
    time_check[6][0] = 0;
    }
    time_check[6][0] = (time_check[6][0]*time_check[6][1] + (end - start))/++time_check[6][1];
    pthread_mutex_unlock(&_g_lock);
    return size;
}

static int
fs_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    size_t sz;

    pthread_mutex_lock(&_g_lock);
    start = clock();
    if (_g_debug)
        fprintf(stderr, "fs_read(%s);\n", path);

    redis_alive();
    
    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HGET %d%s:meta SIZE", depth, path);

    sz = atoi(reply->str);
    freeReplyObject(reply);

    if (sz < size)
	size = sz;
    if (offset + size > sz)
	size = sz - offset;

    reply = redisCommand(_g_redis, "GETRANGE %d%s:data %lu %lu", depth, path, offset, size + offset);

    if  ((reply != NULL) && (reply->type == REDIS_REPLY_ERROR))
	{
	    freeReplyObject(reply);

	    reply = redisCommand(_g_redis, "SUBSTR %d%s:data %lu %lu", depth, path, offset, size + offset);
	}

    if (size > 0)
	memcpy(buf, reply->str, size);

    freeReplyObject(reply);
    end = clock();
    if(!time_check[7][1])
    {
    time_check[7][1] = 1;
    time_check[7][0] = 0;
    }
    time_check[7][0] = (time_check[7][0]*time_check[7][1] + (end - start))/++time_check[7][1];
    pthread_mutex_unlock(&_g_lock);
    return size;
}

static int
fs_symlink(const char *target, const char *path)
{
    redisReply *reply =NULL;
    int depth =get_depth(path);
    char *entry =get_basename(path);

    pthread_mutex_lock(&_g_lock);
    start = clock();
    if (_g_debug)
	    fprintf(stderr,"fs_symlink(target:%s -> %s);\n", target, path);

    if(_g_read_only)
    {
	    pthread_mutex_unlock(&_g_lock);
	    return -EPERM;
    }
    redis_alive();

    redisAppendCommand(_g_redis, "HSET %d%s:meta NAME %s",depth,path, entry);
    redisAppendCommand(_g_redis, "HSET %d%s:meta TYPE LINK",depth,path,entry);
    redisAppendCommand(_g_redis, "HSET %d%s:meta TARGET %s",depth,path,target);
    redisAppendCommand(_g_redis, "HSET %d%s:meta  MODE %d",depth,path, 0444);
    redisAppendCommand(_g_redis, "HSET %d%s:meta UID %d",
                       depth,path,fuse_get_context()->uid);
    redisAppendCommand(_g_redis, "HSET %d%s:meta GID %d",depth,path, fuse_get_context()->gid);
    redisAppendCommand(_g_redis, "HSET %d%s:meta SIZE %d",depth,path, 0);
    redisAppendCommand(_g_redis, "HSET %d%s:meta CTIME %d",depth,path, time(NULL));
    redisAppendCommand(_g_redis, "HSET %d%s:meta MTIME %d",depth,path, time(NULL));
    redisAppendCommand(_g_redis, "HSET %d%s:meta ATIME %d",depth,path,time(NULL));
    redisAppendCommand(_g_redis, "HSET %d%s:meta LINK 1",depth,path);   

int i = 0;
for (i = 0; i < 11; i++)
{
    redisGetReply(_g_redis,(void**)&reply);
    freeReplyObject(reply);
}
    free(entry);
    end = clock();
    if(!time_check[8][1])
    {
    time_check[8][1] = 1;
    time_check[8][0] = 0;
    }
    time_check[8][0] = (time_check[8][0]*time_check[8][1] + (end - start))/++time_check[8][1];
    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_readlink(const char *path, char *buf, size_t size)
{
    redisReply *reply =NULL;
    pthread_mutex_lock(&_g_lock);
    start = clock();
    int depth=get_depth(path);

    if (_g_debug)
	fprintf(stderr, "fs_readlink(%s);\n",path);

    redis_alive();
    reply = redisCommand(_g_redis, "HGET %d%s:meta TARGET",depth,path);
    
    if((reply != NULL) &&(reply->type == REDIS_REPLY_STRING)&&(reply->str != NULL))
   {
	   strcpy(buf, (char *)reply->str);
	   freeReplyObject(reply);
	   pthread_mutex_unlock(&_g_lock);
	   return 0;
   }
    freeReplyObject(reply);
    end = clock();
    if(!time_check[9][1])
    {
    time_check[9][1] = 1;
    time_check[9][0] = 0;
    }
    time_check[9][0] = (time_check[9][0]*time_check[9][1] + (end - start))/++time_check[9][1];
    pthread_mutex_unlock(&_g_lock);

    return(-ENOENT);
}

static int
fs_open(const char *path, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;

    if (_g_debug)
	fprintf(stderr, "fs_open(%s);\n", path);

    pthread_mutex_lock(&_g_lock);
    start = clock();
    int depth = get_depth(path);
    
    reply = redisCommand(_g_redis, "HSET %d%s:meta ATIME %d", depth, path, time(NULL));

    freeReplyObject(reply);
    end = clock();
    if(!time_check[10][1])
    {
    time_check[10][1] = 1;
    time_check[10][0] = 0;
    }
    time_check[10][0] = (time_check[10][0]*time_check[10][1] + (end - start))/++time_check[10][1];
    pthread_mutex_unlock(&_g_lock);

    return 0;
}

static int
fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if (_g_debug)
	fprintf(stderr, "fs_create(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }
    
    redis_alive();

    char *entry = get_basename(path);
    int depth = get_depth(path);

    redisAppendCommand(_g_redis, "HSET %d%s:meta NAME %s TYPE FILE MODE %d UID %d GID %d SIZE %d CTIME %d MTIME %d ATIME %d LINK 1", depth, path, entry, mode, fuse_get_context()->uid, fuse_get_context()->gid, 0, time(NULL), time(NULL), time(NULL));

    redisGetReply(_g_redis, (void **)&reply);
    freeReplyObject(reply);
    end = clock();
    if(!time_check[11][1])
    {
    time_check[11][1] = 1;
    time_check[11][0] = 0;
    }
    time_check[11][0] = (time_check[11][0]*time_check[11][1] + (end - start))/++time_check[11][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_chown(const char *path, uid_t uid, gid_t gid)
{
    redisReply *reply = NULL;
    pthread_mutex_lock(&_g_lock);
    start = clock();
    if (_g_debug)
	fprintf(stderr, "fs_chown(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }
    
    redis_alive();
    
    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HSET %d%s:meta UID %d GID %d MTIME %d", depth, path, uid, gid, time(NULL));

    freeReplyObject(reply);
    end = clock();
    if(!time_check[12][1])
    {
    time_check[12][1] = 1;
    time_check[12][0] = 0;
    }
    time_check[12][0] = (time_check[12][0]*time_check[12][1] + (end - start))/++time_check[12][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_chmod(const char *path, mode_t mode)
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();
    if (_g_debug)
	fprintf(stderr, "fs_chmod(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();

    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HSET %d%s:meta MODE %d MTIME %d", depth, path, mode, time(NULL));
    freeReplyObject(reply);
    end = clock();
    if(!time_check[13][1])
    {
    time_check[13][1] = 1;
    time_check[13][0] = 0;
    }
    time_check[13][0] = (time_check[13][0]*time_check[13][1] + (end - start))/++time_check[13][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_unlink(const char *path)
{
    redisReply *reply =NULL;

    int depth=get_depth(path);
    pthread_mutex_lock(&_g_lock);
    start = clock();
    if(_g_debug) fprintf(stderr, "fs_unlink(%s);\n", path);
    if(_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
     }

    redis_alive();

    reply = redisCommand(_g_redis, "UNLINK %d%s:meta",depth,path);
    freeReplyObject(reply);
    
    reply = redisCommand(_g_redis, "UNLINK %d%s:data",depth,path);
    freeReplyObject(reply);
    end = clock();
    if(!time_check[14][1])
    {
    time_check[14][1] = 1;
    time_check[14][0] = 0;
    }
    time_check[14][0] = (time_check[14][0]*time_check[14][1] + (end - start))/++time_check[14][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;

}

static int
fs_utimens(const char *path, const struct timespec tv[2])
{
    redisReply *reply = NULL;

    pthread_mutex_lock(&_g_lock);
    start = clock();
    if (_g_debug)
	fprintf(stderr, "fs_utimens(%s);\n", path);

    if (_g_read_only)
    {
	pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();

    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HSET %d%s:meta ATIME %d MTIME %d", depth, path, tv[0].tv_sec, tv[1].tv_sec);

    freeReplyObject(reply);
    end = clock();
    if(!time_check[15][1])
    {
    time_check[15][1] = 1;
    time_check[15][0] = 0;
    }
    time_check[15][0] = (time_check[15][0]*time_check[15][1] + (end - start))/++time_check[15][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_access(const char *path, int mode)
{
    redisReply *reply = NULL;

    if (_g_debug)
	fprintf(stderr, "fs_access(%s);\n", path);

    pthread_mutex_lock(&_g_lock);
    start = clock();
    int depth = get_depth(path);

    reply = redisCommand(_g_redis, "HSET %d%s:meta ATIME %d", depth, path, time(NULL));

    freeReplyObject(reply);
    end = clock();
    if(!time_check[16][1])
    {
    time_check[16][1] = 1;
    time_check[16][0] = 0;
    }
    time_check[16][0] = (time_check[16][0]*time_check[16][1] + (end - start))/++time_check[16][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}
	


int
fs_rename(const char *old, const char *path)
{
   /* redisReply *reply =NULL;
    int depth=get_depth(path);

    pthread_mutex_lock(&_g_lock);

    if(_g_debug)
      fprintf(stderr,"fs_rename(%s,%s);\n",old, path);

    if(_g_read_only)
    {pthread_mutex_unlock(&_g_lock);
     return -EPERM;
    }


    redis_alive();

    char *basename =get_basename(path);
    redisReply *subReply =NULL;
    subReply = redisCommand(_g_redis, "HSET %d%s:meta NAME %s",depth,path, basename);

    freeReplyObject(subReply);
    free(basename);

    



*/
    return 0;
}

static int
fs_truncate(const char *path, off_t size)
{
    redisReply *reply =NULL;
    int depth=get_depth(path);

    pthread_mutex_lock(&_g_lock);
    start = clock();

    if(_g_debug)
      fprintf(stderr,"fs_truncate(%s);\n",path);

    if(_g_read_only)
    {
      pthread_mutex_unlock(&_g_lock);
      return -EPERM;
    }

    if(is_directory(path))
   {
      pthread_mutex_unlock(&_g_lock);
      return -ENOENT;
    }
    reply = redisCommand(_g_redis,"DEL %d%s:data",depth,path);
    freeReplyObject(reply);

    reply=redisCommand(_g_redis, "HSET %d%s:meta SIZE 0 MTIME %d",depth,path, time(NULL));
    end = clock();
    if(!time_check[17][1])
    {
    time_check[17][1] = 1;
    time_check[17][0] = 0;
    }
    time_check[17][0] = (time_check[17][0]*time_check[17][1] + (end - start))/++time_check[17][1];

    pthread_mutex_unlock(&_g_lock);
    return 0;
}

long
writePID(const char *filename)
{
    char buf[20];
    int fd;
    long pid;

    if ((fd = open(filename, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1)
        return -1;

    pid = getpid();
    snprintf(buf, sizeof(buf), "%ld", (long)pid);
    if (write(fd, buf, strlen(buf)) != strlen(buf))
    {
        close(fd);
        return -1;
    }

    return pid;
}


int
usage(int argc, char *argv[])
{
    printf("%s - version %s - Filesystem based upon FUSE\n", argv[0], __VERSION__);
    printf("\nOptions:\n\n");
    printf("\t--debug      - Launch with debugging information.\n");
    printf("\t--help       - Show this minimal help information.\n");
    printf("\t--host       - The hostname of the redis server [localhost]\n");
    printf
        ("\t--mount      - The directory to mount our filesystem under [/mnt/redis].\n");
    printf("\t--port       - The port of the redis server [6389].\n");
    printf("\t--read-only  - Mount the filesystem read-only.\n");
    printf("\n");
    return 1;
}


static struct fuse_operations redisfs_operations = {
    .chmod = fs_chmod,
    .chown = fs_chown,
    .create = fs_create,
    .getattr = fs_getattr,
    .mkdir = fs_mkdir,
    .read = fs_read,
    .readdir = fs_readdir,
    .readlink = fs_readlink,
    //.rename = fs_rename,
    .rmdir = fs_rmdir,
    .symlink = fs_symlink,
    .truncate = fs_truncate,
    .unlink = fs_unlink,
    .utimens = fs_utimens,
    .write = fs_write,


     /*
     *  FAKE: Only update access-time.
     */
    .access = fs_access,
    .open = fs_open,


     /*
     * Mutex setup/cleanup.
     */
    .init = fs_init,
    .destroy = fs_destroy,
};



int main(int argc, char **argv) {
    if (_g_test) printf("\n----------------------------\nREDifs\ndesigned by jw&sy\n----------------------------\n\n");
    int c;
    struct stat statbuf;

    char *args[] = {
	"fuse-redisfs", _g_mount,
	"-o", "allow_other",
	"-o", "nonempty",
	"-f",
	"-o", "debug",
	NULL
    };

    int args_c = 7;

    while(1)
    {
	static struct option long_options[] = {
	    {"debug", no_argument, 0, 'd'},
	    {"help", no_argument, 0, 'h'},
	    {"host", required_argument, 0, 's'},
	    {"mount", required_argument, 0, 'm'},
	    {"port", required_argument, 0, 'P'},
	    {"read-only", no_argument, 0, 'r'},
	    {"version", no_argument, 0, 'v'},
	    {0, 0, 0, 0}
	};
	int option_index = 0;

	c = getopt_long(argc, argv, "s:P:m:drhv", long_options, &option_index);

	if (c == -1)
	    break;

	switch (c)
	{
	    case 'v':
		fprintf(stderr, "redisfs - version %d - <http://www.steve.org.uk/Software/redisfs>\n", _SC_VERSION);
		exit(0);
	    case 'P':
		_g_redis_port = atoi(optarg);
		break;
	    case 'r':
		_g_read_only = 1;
		break;
	    case 's':
		snprintf(_g_redis_host, sizeof(_g_redis_host) - 1, "%s", optarg);
		break;
	    case 'h':
		return (usage(argc, argv));
		break;
	    case 'm':
		snprintf(_g_mount, sizeof(_g_mount) - 1, "%s", optarg);
		break;
	    case 'd':
		args_c = 7;
		_g_debug += 1;
		break;
	    default:
		abort();
	}
    }

    if (getuid() != 0)
    {
	fprintf(stderr, "You must start this program as root.\n");
	return -1;
    }

    if ((stat(_g_mount, &statbuf) != 0) || ((statbuf.st_mode & S_IFMT) != S_IFDIR))
    {
	fprintf(stderr, "%s doesn't exist or isn't a directory!\n", _g_mount);
	return -1;
    }

    if (!writePID("/var/run/redisfs.pid"))
    {
	fprintf(stderr, "Writing PID file failed\n");
	return -1;
    }

    printf("Connecting to redis-server %s:%d and mounting at %s.\n", _g_redis_host, _g_redis_port, _g_mount);
    
    if (_g_read_only)
	printf("Filesystem is read-only.\n");

    return (fuse_main(args_c, args, &redisfs_operations, NULL));
}




    
    //const char* path = "2/c/b";
    //printf(get_basename(path));
    //if (is_directory(path)) printf("it is dir");
    //printf("depth:%d\n", is_directory(path));
