#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
//#include "queue.h"

enum node_color{ RED, BLACK };
struct node_t
{
    const char *data;
    int color;
    sem_t file_lock;
    struct node_t *link[2];
};

struct node_t *create_node(char *inp);
void insertion(const char *inp_t, pthread_mutex_t _t_lock);
void deletion(char *data, pthread_mutex_t _t_lock);
struct node_t *tree_search(const char *data_t);
void path_lock(const char *data_t, pthread_mutex_t _t_lock);
void path_unlock(const char *data_t, pthread_mutex_t _t_lock);
