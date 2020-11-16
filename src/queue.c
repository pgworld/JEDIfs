#include <stdlib.h>
#include <hiredis.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include "queue.h"


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

void Enqueue(Queue *queue, struct redis_comm_req *data, sem_t *space, sem_t *item)
{
	sem_wait(space);
    Node *now=(Node *)malloc(sizeof(Node));
    now->data = data;
    now->next = NULL;

    if(IsEmpty(queue))
    {
    	queue->front = now;
    }
    else
    {
    	queue->rear->next=now;
    }
    queue->rear = now;
    queue->count++;
	sem_post(item);
}

struct redis_comm_req *Dequeue(Queue *queue, sem_t *space, sem_t *item, pthread_mutex_t *dequeue_lock)
{
    if(IsEmpty(queue))
    {
		return NULL;
    }
	sem_wait(item);
    struct redis_comm_req *re;
    Node *now;

    pthread_mutex_lock(dequeue_lock);

    now = queue->front;
	re = queue->front->data;
    queue->front = queue->front->next;
    queue->count--;

    pthread_mutex_unlock(dequeue_lock);
	sem_post(space);

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
void s_init(sem_t *space, sem_t *item)
{
    sem_init(space,0,50);
	sem_init(item,0,0);
}


