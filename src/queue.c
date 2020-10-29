#include <stdlib.h>
#include <hiredis.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include "queue.h"

pthread_mutex_t mutex_lock;
sem_t space;

void InitQueue(Queue *queue)
{
	queue->front = NULL;
	queue->rear = NULL;
	queue->count = 0;
}

int IsEmpty(Queue *queue)
{
	return queue->count == 0;
}

int IsFull(Queue *queue)
{
	return queue->count == 50;
}

void Enqueue(Queue *queue, struct redis_comm_req *data)
{		
	down(&space);
	printf("in enqueue, redis_req(%s) address :%d\n", data->comm, data);
    Node *now=(Node *)malloc(sizeof(Node));
    now->data = data;
    now->next = NULL;

    if(IsEmpty(queue))
    {
    	queue-> front = now;
    }
    else
    {
    queue->rear->next=now;
    }
    queue->rear = now;
    queue->count++;
}

struct redis_comm_req *Dequeue(Queue *queue)
{
    if(IsEmpty(queue))
    {
		return NULL;
    }
    struct redis_comm_req *re;
    Node *now;

    pthread_mutex_lock(&mutex_lock);

    now = queue->front;
    printf("%s\n", now->data->comm);
	re = now->data;
	printf("in dequeue up, redis_req(%s) address: %d\n", re->comm,re);
    queue->front = now->next;
    //free(now);
    queue->count--;

    pthread_mutex_unlock(&mutex_lock);
    up(&space);
	printf("in dequeue down, redis_req(%s) address: %d\n", re->comm,re);
	return re;
}

void down(sem_t *sem)
{
    if(sem_wait(sem)<0)
    {
        printf("DOWN ERROR");
    }
}

void up(sem_t *sem)
{
    if(sem_post(sem)<0)
    {
        printf("UP ERROR");
    }

}
/*
void *consumer(){
    printf("start consumer\n");
    struct redis_comm_req req;
    char *comm;
    while(1)
	{
    	req = Dequeue(queue);
    	if (!req.comm) continue;
    	printf("consumer(%s)\n", req.comm);
    	if (req.reply != NULL) break;

    	comm = req.comm;
    	req.reply = redisCommand(_g_redis, comm);
    	if (req.cond)
		{
            pthread_mutex_lock(req.mutex);
            pthread_cond_signal(req.cond);
            pthread_mutex_unlock(req.mutex);
        }
    }
    up(req.space);
}
*/
void s_init(void)
{
    sem_init(&space,0,50);
}

