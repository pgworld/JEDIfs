#include <stdlib.h>
#include <hiredis.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

struct redis_comm_req
{
	char *comm;
	redisReply *reply;
	pthread_cond_t *cond;
	pthread_mutex_t *mutex;
	sem_t *space;
};

typedef struct Node
{
	struct redis_comm_req *data;
	struct Node *next;
}Node;

typedef struct Queue
{
	Node *front;
	Node *rear;
	int count;
}Queue;

void InitQueue(Queue *queue);
int IsEmpty(Queue *queue);
int IsFull(Queue *queue);
void Enqueue(Queue *queue, struct redis_comm_req *data);
struct redis_comm_req *Dequeue(Queue *queue);
void down(sem_t *sem);
void up(sem_t *sem);
//void *consumer(void);
void s_init(void);
