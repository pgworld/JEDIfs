#define FUSE_USE_VERSION 27
#define D_FILE_OFFSET_BITS = 64
#include <fuse.h>
#include <hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <net/if.h>
#include <stdarg.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>
#include <semaphore.h>
#include "rbtree.h"
#include "queue.h"

redisReply *reply;
Queue *queue;
pthread_t p_thread[1];
int _g_redis_port = 6379;
char _g_redis_host[100] = { "127.0.0.1" };
int _g_debug = 1;
int _g_read_only = 0;
char _g_mount[200] = { "/mnt/redis" };
pthread_mutex_t _g_lock = PTHREAD_MUTEX_INITIALIZER;
int _g_fast = 0;
pthread_mutex_t mutex_lock;
//sem_t space;
int this_space =0;
int queue_limit = 50;


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
		char rediscom[1000];
		sprintf(rediscom, "ping");
		reply = redisCommand(_g_redis, "%s", rediscom);

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


void *consumer(){
    struct redis_comm_req *req;
    char *comm;
    while(1)
	{
    	req = Dequeue(queue);
    	if (!req) continue;
    	if (req->reply != NULL) break;
    	comm = req->comm;
		redis_alive();
    	req->reply = redisCommand(_g_redis, comm);
    	if (req->cond && req->mutex)
		{
			pthread_mutex_lock(req->mutex);
            pthread_cond_signal(req->cond);
			pthread_mutex_unlock(req->mutex);
        }
    }
}

/*
void
redis_alive()
{
    struct timeval timeout = { 5, 0 };    // 5 seconds
    redisReply *reply = NULL;

    if (_g_redis != NULL)
    {
		char rediscom[1000];
		sprintf(rediscom, "ping");
		reply = redisCommand(_g_redis, "%s", rediscom);

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
*/
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
     * RetuUUrn.
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


void 
fs_init()
{
    if (_g_debug)
	fprintf(stderr, "fs_init()\n");

    pthread_mutex_init(&_g_lock, NULL);
    redis_alive();
	char* base = "/";
	insertion(base, _g_lock); 

    // init & run consumer
	queue=(Queue*)malloc(sizeof(Queue));
	InitQueue(queue);
	s_init();
	pthread_mutex_init(&mutex_lock, NULL);
	pthread_create(&p_thread[0],NULL,consumer,NULL);
    return 0;
}

void
fs_destroy()
{
    if (_g_debug)
	fprintf(stderr, "fs_destroy()\n");
    pthread_mutex_destroy(&_g_lock);

    // stop & destroy consumer
	pthread_join(p_thread[0], NULL);
}

int
is_directory(const char *path)
{
    int ret = 0;
	struct redis_comm_req redis_req;

    redisReply *reply = NULL;

	/* define lock and init*/
    pthread_cond_t cond;
    pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
    pthread_mutex_lock(&mutex);

    if (_g_debug)
	fprintf(stderr, "is_directroy(%s)\n", path);

    int depth = get_depth(path);

	char rediscom[1000];
	sprintf(rediscom, "EXISTS %d%s:data", depth, path);

	redis_req.comm = rediscom;
	redis_req.cond = &cond;
	redis_req.mutex = &mutex;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);
    if (redis_req.reply->integer == 0) ret = 1;
	freeReplyObject(redis_req.reply);
	pthread_mutex_unlock(&mutex);
    return (ret);
}

static int
fs_readdir(const char *path,
	   void *buf,
	   fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    int i;

	/* define redis_comm_req */
    struct redis_comm_req redis_req;
	struct redis_comm_req sub_redis_req;

	/* define lock and init*/
    pthread_cond_t cond;
    pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
    path_lock(path, _g_lock);
    pthread_mutex_lock(&mutex);

    if (_g_debug)
	fprintf(stderr, "fs_readdir(%s)\n", path);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    int depth = get_depth(path);

	char rediscom[1000];
    sprintf(rediscom, "KEYS %d%s*:meta", depth + 1, path);

    /* need wait */
    redis_req.comm = rediscom;
    redis_req.cond = &cond;
    redis_req.mutex = &mutex;
    redis_req.reply = reply;
	sub_redis_req.cond = &cond;
	sub_redis_req.mutex = &mutex;

    Enqueue(queue, &redis_req);
    pthread_cond_wait(&cond, &mutex);

	for(i = 0; i < redis_req.reply -> elements; i++)
	{
		sprintf(rediscom, "HGET %s NAME", ((redis_req.reply -> element[i])->str));
		redisReply *subreply = NULL;
		sub_redis_req.reply = subreply; 
		sub_redis_req.comm = rediscom;
		Enqueue(queue, &sub_redis_req);
		pthread_cond_wait(&cond, &mutex);
		filler(buf, strdup(sub_redis_req.reply->str), NULL, 0);
		freeReplyObject(sub_redis_req.reply);
	}

	freeReplyObject(redis_req.reply);
    pthread_mutex_unlock(&mutex);
    path_unlock(path, _g_lock);
    return 0;
}

static int
fs_getattr(const char *path, struct stat *stbuf)
{
    redisReply *reply = NULL;

	/* define redis_comm_req */
	struct redis_comm_req redis_req;
	struct redis_comm_req sub_redis_req;
	/* define lock and init*/
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	path_lock(path, _g_lock);
	pthread_mutex_lock(&mutex);

    if (_g_debug)
        fprintf(stderr, "fs_getattr(%s);\n", path);
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
		pthread_mutex_unlock(&mutex);
        path_unlock(path, _g_lock);

		return 0;
    }
	
    int depth = get_depth(path);
	char rediscom[1000];
	sprintf(rediscom, "EXISTS %d%s:meta", depth, path);

	redis_req.comm = rediscom;
	redis_req.cond = &cond;
	redis_req.mutex = &mutex;
	redis_req.reply = reply;
	
	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);
    if (redis_req.reply->integer == 0)
    {
		pthread_mutex_unlock(&mutex);
		path_unlock(path, _g_lock);
		return -ENOENT;
    }
	freeReplyObject(redis_req.reply);
	sprintf(rediscom, "HMGET %d%s:meta CTIME ATIME MTIME GID UID LINK", depth, path);
	
	redis_req.comm = rediscom;

/*
   현재 내가 고민하고 있는 것.
   Enqueue에 redis_req의 주소를 넣어주었다 이거야
   근데 나중에 consumer에서 reply free를 해주잖아?
   그러면 ㅅㅂ 나중에 이걸 받아올 수 있을까?
   여기에 하나하나 free를 다시 해주어야하지 않을까 이거야
   이거는 나중에 디버깅할 때 한번 돌아가는지 보고 확인해보자
   만약 메모리 에러가 난다면 하나의 이유는 이거지 않을까 싶다.
   나중에 디버깅을 할때 해당 메모가 도움이 되었으면 좋겠구만.
   */
	redis_req.reply = NULL;
	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);

    if ((redis_req.reply->element[0] != NULL)
        && (redis_req.reply->element[0]->type == REDIS_REPLY_STRING))
        stbuf->st_ctime = atoi(redis_req.reply->element[0]->str);
    if ((redis_req.reply->element[1] != NULL)
        && (redis_req.reply->element[1]->type == REDIS_REPLY_STRING))
        stbuf->st_atime = atoi(redis_req.reply->element[1]->str);
    if ((redis_req.reply->element[2] != NULL)
        && (redis_req.reply->element[2]->type == REDIS_REPLY_STRING))
        stbuf->st_mtime = atoi(redis_req.reply->element[2]->str);
    if ((redis_req.reply->element[3] != NULL)
        && (redis_req.reply->element[3]->type == REDIS_REPLY_STRING))
        stbuf->st_gid = atoi(redis_req.reply->element[3]->str);
    if ((redis_req.reply->element[4] != NULL)
        && (redis_req.reply->element[4]->type == REDIS_REPLY_STRING))
        stbuf->st_uid = atoi(redis_req.reply->element[4]->str);
    if ((redis_req.reply->element[5] != NULL)
        && (redis_req.reply->element[5]->type == REDIS_REPLY_STRING))
        stbuf->st_nlink = atoi(redis_req.reply->element[5]->str);
	
	sprintf(rediscom, "HMGET %d%s:meta TYPE MODE SIZE", depth, path);
	redis_req.comm = rediscom;
	redis_req.reply = NULL;
	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);

    if ((redis_req.reply != NULL) && (redis_req.reply->element[0] != NULL)
        && (redis_req.reply->element[0]->type == REDIS_REPLY_STRING))
    {

        if ((redis_req.reply->element[1] != NULL)
            && (redis_req.reply->element[1]->type == REDIS_REPLY_STRING))
        {
            stbuf->st_mode = atoi(redis_req.reply->element[1]->str);
        }

        if (strcmp(redis_req.reply->element[0]->str, "DIR") == 0)
        {
            stbuf->st_mode |= S_IFDIR;
        }
        else if (strcmp(redis_req.reply->element[0]->str, "LINK") == 0)
        {
            stbuf->st_mode |= S_IFLNK;
            stbuf->st_nlink = 1;
            stbuf->st_size = 0;
        }
        else if (strcmp(redis_req.reply->element[0]->str, "FILE") == 0)
        {
            if ((redis_req.reply->element[2] != NULL)
                && (redis_req.reply->element[2]->type == REDIS_REPLY_STRING))
            {
                if (_g_debug)
                    fprintf(stderr, "found file\n");
                stbuf->st_size = atoi(redis_req.reply->element[2]->str);
            }
        }
        else
        {
            if (_g_debug)
                fprintf(stderr, "UNKNOWN ENTRY TYPE: %s\n",
                        redis_req.reply->element[0]->str);
        }
    }
	pthread_mutex_unlock(&mutex);
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);
    return 0;
}

static int
fs_mkdir(const char *path, mode_t mode)
{
    redisReply *reply = NULL;

	struct redis_comm_req redis_req;

    insertion(path, _g_lock);
    path_lock(path, _g_lock);

    if (_g_debug)
		fprintf(stderr, "fs_mkdir(%s);\n", path);

    if (_g_read_only)
    {
		path_unlock(path, _g_lock);
		return -EPERM;
    }

	
    char *entry = get_basename(path);

    int depth = get_depth(path);

	char rediscom[1000];
	sprintf(rediscom, "HMSET %d%s:meta NAME %s TYPE DIR MODE %d UID %d GID %d SIZE %d CTIME %d MTIME %d ATIME %d LINK 1", depth, path, entry, mode, fuse_get_context()->uid, fuse_get_context()->gid, 0, time(NULL), time(NULL), time(NULL));
	
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;
	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}

int
count_dir_ent(const char *path)
{
    redisReply *reply = NULL;

	struct redis_comm_req redis_req;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);

    int depth = get_depth(path);

	char rediscom[1000];
	sprintf(rediscom, "KEYS %d%s/*", depth + 1, path);
	
	redis_req.comm = rediscom;
	redis_req.cond = &cond;
	redis_req.mutex = &mutex;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);

    int res = redis_req.reply->elements;
	freeReplyObject(redis_req.reply);
	pthread_mutex_unlock(&mutex);

    return res;
}

static int
fs_rmdir(const char *path)
{
    redisReply *reply = NULL;
	struct redis_comm_req redis_req;

    path_lock(path, _g_lock);

    if (_g_debug)
		fprintf(stderr, "fs_rmdir(%s);\n", path);

    if (_g_read_only)
    {
		path_unlock(path, _g_lock);
		return -EPERM;
    }

    if (!is_directory(path))
    {
		path_unlock(path, _g_lock);
		return -ENOENT;
    }


    if (count_dir_ent(path))
    {
		path_unlock(path, _g_lock);
		return -ENOTEMPTY;
    }
    
    int depth = get_depth(path);
	char rediscom[1000];
    sprintf(rediscom, "DEL %d%s:meta", depth, path);

	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;
	
	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_write(const char *path,
	 const char *buf,
	 size_t size, off_t offset, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
	
	struct redis_comm_req redis_req;

    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock);

    if (_g_debug)
		fprintf(stderr, "fs_write(%s);\n", path);

    if (_g_read_only)
    {
		path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
		return -EPERM;
    }
	
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	char rediscom[1000];


    int depth = get_depth(path);

    if (offset == 0)
    {
		char *mem = malloc(size + 1);
		memcpy(mem, buf, size);

		if (_g_debug)
            fprintf(stderr, "fs_write->simple(%s);\n", path);

		sprintf(rediscom, "HSET %d%s:meta SIZE %d MTIME %d", depth, path, size, time(NULL));
        
		redis_req.comm = rediscom;
		Enqueue(queue, &redis_req);
		freeReplyObject(redis_req.reply);
		redis_req.reply = NULL;
		sprintf(rediscom, "SET %d%s:data %b", depth, path, mem, size);
		redis_req.comm = rediscom;
		Enqueue(queue, &redis_req);
	   	freeReplyObject(redis_req.reply); 
		free(mem);
    }
    else 
    {
		if (_g_debug)
            fprintf(stderr, "fs_write->offsetted(%s);\n", path);

		char *mem = malloc(size);
		memcpy(mem, buf, size);

		sprintf(rediscom, "HINCRBY %d%s:meta SIZE %d", depth, path, size);

		redis_req.comm = rediscom;
		Enqueue(queue, &redis_req);
		freeReplyObject(redis_req.reply);
		redis_req.reply = NULL;
		sprintf(rediscom, "APPEND %d%s:data %b", depth, path, mem, size);
		redis_req.comm = rediscom;
		Enqueue(queue, &redis_req);
		freeReplyObject(redis_req.reply);
		redis_req.reply = NULL;
		if (!_g_fast)
		{
	    	sprintf(rediscom, "HSET %d%s:meta MTIME %d", depth, path, time(NULL));
	    	redis_req.comm = rediscom;
	    	Enqueue(queue, &redis_req);
			freeReplyObject(redis_req.reply);
		}

		free(mem);
    }
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return size;
}

static int
fs_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
    size_t sz;
	
	struct redis_comm_req redis_req;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
    path_lock(path, _g_lock);
	pthread_mutex_lock(&mutex);

    if (_g_debug)
        fprintf(stderr, "fs_read(%s);\n", path);

    redis_alive();
    
    int depth = get_depth(path);
	char rediscom[1000];

    sprintf(rediscom, "HGET %d%s:meta SIZE", depth, path);
	redis_req.comm = rediscom;
	redis_req.cond = &cond;
	redis_req.mutex = &mutex;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);

    sz = atoi(redis_req.reply->str);
	freeReplyObject(redis_req.reply);
	redis_req.reply = NULL;
    if (sz < size)
	size = sz;
    if (offset + size > sz)
	size = sz - offset;

    sprintf(rediscom, "GETRANGE %d%s:data %lu %lu", depth, path, offset, size + offset);
	redis_req.comm = rediscom;

	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);

    if  ((redis_req.reply != NULL) && (redis_req.reply->type == REDIS_REPLY_ERROR))
	{
		freeReplyObject(redis_req.reply);
		redis_req.reply = NULL;
	    sprintf(rediscom, "SUBSTR %d%s:data %lu %lu", depth, path, offset, size + offset);
		redis_req.comm = rediscom;
		redis_req.reply = reply;

		Enqueue(queue, &redis_req);
		pthread_cond_wait(&cond, &mutex);
	}

    if (size > 0)
	memcpy(buf, redis_req.reply->str, size);

	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);
	pthread_mutex_unlock(&mutex);
    return size;
}

static int
fs_symlink(const char *target, const char *path)
{
	insertion(path, _g_lock);
    redisReply *reply =NULL;

    int depth =get_depth(path);
    char *entry =get_basename(path);

	struct redis_comm_req redis_req;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
    path_lock(path, _g_lock);
	pthread_mutex_lock(&mutex);//////

    if (_g_debug)
	    fprintf(stderr,"fs_symlink(target:%s -> %s);\n", target, path);

    if(_g_read_only)
    {
	    path_unlock(path, _g_lock);
		pthread_mutex_unlock(&mutex);
	    //deletion(path, _g_lock);
	    return -EPERM;
    }

    redis_alive();
	
	char rediscom[1000];
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

    sprintf(rediscom, "HSET %d%s:meta NAME %s",depth,path, entry);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta TYPE LINK",depth,path,entry);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta TARGET %s",depth,path,target);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta  MODE %d",depth,path, 0444);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta UID %d", depth,entry,fuse_get_context()->uid);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta GID %d",depth,path, fuse_get_context()->gid);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta SIZE %d",depth,path, 0);
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta CTIME %d",depth,path, time(NULL));
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta MTIME %d",depth,path, time(NULL));
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta ATIME %d",depth,path,time(NULL));
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);
	reply = NULL;
	redis_req.reply = reply;//
    sprintf(rediscom, "HSET %d%s:meta LINK 1",depth,path);  
	redis_req.comm = rediscom;
	Enqueue(queue, &redis_req);

    free(entry);
    
    path_unlock(path, _g_lock);
	pthread_mutex_unlock(&mutex);
    return 0;
}

static int
fs_readlink(const char *path, char *buf, size_t size)
{
    redisReply *reply =NULL;
    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock);

    int depth=get_depth(path);

	struct redis_comm_req redis_req;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
    path_lock(path, _g_lock);
	pthread_mutex_lock(&mutex);

    if (_g_debug)
	fprintf(stderr, "fs_readlink(%s);\n",path);

    redis_alive();

	char rediscom[1000];
    sprintf(rediscom, "HGET %d%s:meta TARGET",depth,path);

	redis_req.comm = rediscom;
	redis_req.cond = &cond;
	redis_req.mutex = &mutex;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	pthread_cond_wait(&cond, &mutex);
    
    if((redis_req.reply != NULL) &&(redis_req.reply->type == REDIS_REPLY_STRING)&&(reply->str != NULL))
	{
		strcpy(buf, (char *)redis_req.reply->str);
	   	path_unlock(path, _g_lock);
		pthread_mutex_unlock(&mutex);
	   	return 0;
   	}
    path_unlock(path, _g_lock);
	pthread_mutex_unlock(&mutex);

    return(-ENOENT);
}

static int
fs_open(const char *path, struct fuse_file_info *fi)
{
    redisReply *reply = NULL;
	struct redis_comm_req redis_req;

    if (_g_debug)
	fprintf(stderr, "fs_open(%s);\n", path);

    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock);

    int depth = get_depth(path);
    char rediscom[1000];

    sprintf(rediscom, "HSET %d%s:meta ATIME %d", depth, path, time(NULL));

	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);

    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);

    return 0;
}

static int
fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    insertion(path, _g_lock);
    redisReply *reply = NULL;

	struct redis_comm_req redis_req;
	
	path_lock(path, _g_lock);

    if (_g_debug)
		fprintf(stderr, "fs_create(%s);\n", path);

    if (_g_read_only)
    {
		path_unlock(path, _g_lock);
		//deletion(path, _g_lock);
		return -EPERM;
    }

    char *entry = get_basename(path);
    int depth = get_depth(path);

	char rediscom[1000];
    sprintf(rediscom, "HMSET %d%s:meta NAME %s TYPE FILE MODE %d UID %d GID %d SIZE %d CTIME %d MTIME %d ATIME %d LINK 1", depth, path, entry, mode, fuse_get_context()->uid, fuse_get_context()->gid, 0, time(NULL), time(NULL), time(NULL));
	
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);
    return 0;
}

static int
fs_chown(const char *path, uid_t uid, gid_t gid)
{
    redisReply *reply = NULL;
	
	struct redis_comm_req redis_req;
	
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
	path_lock(path, _g_lock);//////////


    if (_g_debug)
	fprintf(stderr, "fs_chown(%s);\n", path);

    if (_g_read_only)
    {
		pthread_mutex_unlock(&mutex);
		path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
		return -EPERM;
    }
    
    redis_alive();
    
    int depth = get_depth(path);

	char rediscom[1000];

    sprintf(rediscom, "HSET %d%s:meta UID %d GID %d MTIME %d", depth, path, uid, gid, time(NULL));
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);

	pthread_mutex_unlock(&mutex);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_chmod(const char *path, mode_t mode)
{
    redisReply *reply = NULL;
	
	struct redis_comm_req redis_req;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
    path_lock(path, _g_lock);

    if (_g_debug)
	fprintf(stderr, "fs_chmod(%s);\n", path);

    if (_g_read_only)
    {
		path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
	return -EPERM;
    }

    redis_alive();

    int depth = get_depth(path);
	char rediscom[1000];
    sprintf(rediscom, "HSET %d%s:meta MODE %d MTIME %d", depth, path, mode, time(NULL));
    redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);

	pthread_mutex_unlock(&mutex);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_unlink(const char *path)
{
    redisReply *reply =NULL;

	struct redis_comm_req redis_req;

    path_lock(path, _g_lock);

    int depth=get_depth(path);
    if(_g_debug)
		fprintf(stderr, "fs_unlink(%s);\n", path);

    if(_g_read_only)
	{
		path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
		return -EPERM;
    }


	char rediscom[1000];
    sprintf(rediscom, "DEL %d%s:meta", depth, path);
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
	redis_req.reply = NULL;
    
    sprintf(rediscom, "DEL %d%s:data", depth, path);
	redis_req.comm = rediscom;


	Enqueue(queue, &redis_req);
	
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    //deletion(path, _g_lock);
    return 0;

}

static int
fs_utimens(const char *path, const struct timespec tv[2])
{
    redisReply *reply = NULL;
	
	struct redis_comm_req redis_req;	

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock);

    if (_g_debug)
		fprintf(stderr, "fs_utimens(%s);\n", path);

    if (_g_read_only)
    {
		pthread_mutex_unlock(&mutex);
		path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
		return -EPERM;
    }

    redis_alive();

    int depth = get_depth(path);
	
	char rediscom[1000];

    sprintf(rediscom, "HSET %d%s:meta ATIME %d MTIME %d", depth, path, tv[0].tv_sec, tv[1].tv_sec);
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;
	
	Enqueue(queue, &redis_req);
	
	pthread_mutex_unlock(&mutex);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}

static int
fs_access(const char *path, int mode)
{
    redisReply *reply = NULL;

	struct redis_comm_req redis_req;
    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock); 	

    if (_g_debug)
	fprintf(stderr, "fs_access(%s);\n", path);


    int depth = get_depth(path);

	char rediscom[1000];

    sprintf(rediscom, "HSET %d%s:meta ATIME %d", depth, path, time(NULL));
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
   	freeReplyObject(redis_req.reply);	
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
    return 0;
}
	


int
fs_rename(const char *old, const char *path)
{
   /* redisReply *reply =NULL;
    int depth=get_depth(path);
    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock);
    if(_g_debug)
      fprintf(stderr,"fs_rename(%s,%s);\n",old, path);
    if(_g_read_only)
    {path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
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

	struct redis_comm_req redis_req;
    path_lock(path, _g_lock);//pthread_mutex_lock(&_g_lock); 	

    if(_g_debug)
      fprintf(stderr,"fs_truncate(%s);\n",path);

    if(_g_read_only)
    {
      	path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
      	return -EPERM;
    }

    if(is_directory(path))
    {
      	path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
      	return -ENOENT;
    }

	char rediscom[1000];
    sprintf(rediscom, "DEL %d%s:data", depth, path);

	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;
	redis_req.reply = reply;

	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
	redis_req.reply = NULL;

    sprintf(rediscom, "HSET %d%s:meta SIZE 0 MTIME %d",depth,path, time(NULL));
	redis_req.comm = rediscom;
	redis_req.cond = NULL;
	redis_req.mutex = NULL;

	Enqueue(queue, &redis_req);
	freeReplyObject(redis_req.reply);
    path_unlock(path, _g_lock);//pthread_mutex_unlock(&_g_lock);
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

