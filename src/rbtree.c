#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include "rbtree.h"
//#include "queue.h"

struct node_t *create_node(char *inp)
{
	    struct node_t *new_node;
		new_node = (struct node_t *)malloc(sizeof(struct node_t));
		(new_node->data) = inp;
		new_node -> color = RED;
//		pthread_mutex_init(&(new_node->file_mutex), NULL);
		new_node -> link[0] = new_node -> link[1] = NULL;
		return new_node;
}

struct node_t *root = NULL;
void insertion(const char *inp_t, pthread_mutex_t _t_lock)
{
    char *inp = (char *) inp_t;
    pthread_mutex_lock(&_t_lock);
    struct node_t *stack[98], *ptr, *newnode, *xPtr, *yPtr;
    int dir[98], ht = 0, index;
    ptr = root;
    if(!root)
    {
        root = create_node(inp);
        pthread_mutex_unlock(&_t_lock);
        return;
    }

    stack[ht] = root;
    dir[ht++] = 0;

    while (ptr != NULL)
    {
        if(strcmp(inp, ptr->data) == 0)
        {
            pthread_mutex_unlock(&_t_lock);
            return;
        }
        index = (strcmp(inp, ptr->data)) > 0 ? 1 : 0;
        stack[ht] = ptr;
        ptr = ptr -> link[index];
        dir[ht++] = index;
    }

    stack[ht - 1]->link[index] = newnode = create_node(inp);

    while ((ht >= 3) && (stack[ht - 1]->color == RED))
    {
        if(dir[ht - 2] == 0)
        {
            yPtr = stack[ht - 2] -> link[1];
            if (yPtr != NULL && yPtr->color == RED)
            {
                stack[ht - 2]->color = RED;
                stack[ht - 1]->color = yPtr->color = BLACK;
                ht = ht - 2;
            }
            else
            {
                if (dir[ht - 1] == 0)
                {
                    yPtr = stack[ht - 1];
                }
                else
                {
                    xPtr = stack[ht - 1];
                    yPtr = xPtr -> link[1];
                    xPtr->link[1] = yPtr->link[0];
                    yPtr->link[0] = xPtr;
                    stack[ht - 2]->link[0] = yPtr;
                }
                xPtr = stack[ht - 2];
                xPtr->color = RED;
                yPtr->color = BLACK;
                xPtr->link[0] = yPtr->link[1];
                yPtr->link[1] = xPtr;
                if (xPtr == root)
                {
                    root = yPtr;
                }
                else
                {
                    stack[ht - 3]->link[dir[ht - 3]] = yPtr;
                }
                break;
            }
        }
        else
        {
            yPtr = stack[ht - 2]->link[0];
            if ((yPtr != NULL) && (yPtr->color == RED))
            {
                stack[ht - 2]->color = RED;
                stack[ht - 1]->color = yPtr->color = BLACK;
                ht = ht - 2;
            }
            else
            {
                if (dir[ht - 1] == 1)
                {
                    yPtr = stack[ht - 1];
                }
                else
                {
                    xPtr = stack[ht - 1];
                    yPtr = xPtr->link[0];
                    xPtr->link[0] = yPtr->link[1];
                    yPtr->link[1] = xPtr;
                    stack[ht - 2]->link[1] = yPtr;
                }
                xPtr = stack[ht - 2];
                yPtr->color = BLACK;
                xPtr->color = RED;
                xPtr->link[1] = yPtr->link[0];
                yPtr->link[0] = xPtr;
                if (xPtr == root)
                {
                    root = yPtr;
                }
                else
                {
                    stack[ht - 3]->link[dir[ht - 3]] = yPtr;
                }
                break;
            }
        }
    }
    root -> color = BLACK;
    pthread_mutex_unlock(&_t_lock);
}

void deletion(char *data, pthread_mutex_t _t_lock)
{
    pthread_mutex_lock(&_t_lock);
    struct node_t *stack[98], *ptr, *xPtr, *yPtr;
    struct node_t *pPtr, *qPtr, *rPtr;
    int dir[98], ht = 0, diff, i;
    enum node_color color;

    if (!root)
    {
        pthread_mutex_unlock(&_t_lock);
        return;
    }

    ptr = root;
    while (ptr != NULL)
    {
        if(strcmp(data, ptr->data) == 0)
            break;
        diff = strcmp(data, ptr->data) > 0 ? 1 : 0;
        stack[ht] = ptr;
        dir[ht++] = diff;
        ptr = ptr->link[diff];
    }
    if (ptr->link[1] == NULL)
    {
        if ((ptr == root) && (ptr->link[0] == NULL))
        {
            free(ptr);
            root = NULL;
        }
        else if (ptr == root)
        {
            root = ptr->link[0];
            free(ptr);
        }
        else
        {
            stack[ht - 1]->link[dir[ht - 1]] = ptr->link[0];
        }
    }
    else
    {
        xPtr = ptr->link[1];
        if (xPtr->link[0] == NULL)
        {
            xPtr->link[0] = ptr->link[0];
            color = xPtr->color;
            xPtr->color = ptr->color;
            ptr->color = color;

            if(ptr == root)
            {
                root = xPtr;
            }
            else
            {
                stack[ht - 1]->link[dir[ht - 1]] = xPtr;
            }

            dir[ht] = 1;
            stack[ht++] = xPtr;
        }
        else
        {
            i = ht++;
            while(1)
            {
                dir[ht] = 0;
                stack[ht++] = xPtr;
                yPtr = xPtr->link[0];
                if(!yPtr->link[0])
                    break;
                xPtr = yPtr;
            }

            dir[i] = 1;
            stack[i] = yPtr;
            if (i > 0)
                stack[i - 1]->link[dir[i - 1]] = yPtr;

            yPtr->link[0] = ptr->link[0];

            xPtr->link[0] = yPtr->link[1];
            yPtr->link[1] = ptr->link[1];

            if (ptr == root)
            {
                root = yPtr;
            }

            color = yPtr->color;
            yPtr->color = ptr->color;
            ptr->color = color;
        }
    }

    if (ht < 1)
    {
        pthread_mutex_unlock(&_t_lock);
        return;
    }

    if (ptr->color == BLACK)
    {
        while(1)
        {
            pPtr = stack[ht - 1]->link[dir[ht - 1]];
            if (pPtr && pPtr->color == RED)
            {
                pPtr->color = BLACK;
                break;
            }

            if (ht < 2)
                break;

            if (dir[ht - 2] == 0)
            {
                rPtr = stack[ht - 1]->link[1];

                if(!rPtr)
                    break;

                if(rPtr->color == RED)
                {
                    stack[ht - 1]->color = RED;
                    rPtr->color = BLACK;
                    stack[ht - 1]->link[1] = rPtr->link[0];
                    rPtr->link[0] = stack[ht - 1];

                    if(stack[ht - 1] == root)
                    {
                        root = rPtr;
                    }
                    else
                    {
                        stack[ht - 2]->link[dir[ht - 2]] = rPtr;
                    }
                    dir[ht] = 0;
                    stack[ht] = stack[ht - 1];
                    stack[ht - 1] = rPtr;
                    ht++;

                    rPtr = stack[ht - 1]->link[1];
                }

                if ((!rPtr->link[0] || rPtr->link[0]->color == BLACK) &&
                        (!rPtr->link[1] || rPtr->link[1]->color == BLACK))
                {
                    rPtr->color = RED;
                }
                else
                {
                    if(!rPtr->link[1] || rPtr->link[1]->color == BLACK)
                    {
                        qPtr = rPtr->link[0];
                        rPtr->color = RED;
                        qPtr->color = BLACK;
                        rPtr->link[0] = qPtr->link[1];
                        qPtr->link[1] = rPtr;
                        rPtr = stack[ht - 1]->link[1] = qPtr;
                    }
                    rPtr->color = stack[ht - 1]->color;
                    stack[ht - 1]->color = BLACK;
                    rPtr->link[1]->color = BLACK;
                    stack[ht - 1]->link[1] = rPtr->link[0];
                    rPtr->link[0] = stack[ht - 1];

                    if (stack[ht - 1] == root)
                    {
                        root = rPtr;
                    }
                    else
                    {
                        stack[ht - 2]->link[dir[ht - 2]] = rPtr;
                    }
                    break;
                }
            }
            else
            {
                rPtr = stack[ht - 1]->link[0];
                if (!rPtr)
                    break;

                if (rPtr->color == RED)
                {
                    stack[ht - 1]->color = RED;
                    rPtr->color = BLACK;
                    stack[ht - 1]->link[0] = rPtr->link[1];
                    rPtr->link[1] = stack[ht - 1];

                    if (stack[ht - 1] == root)
                    {
                        root = rPtr;
                    }
                    else
                    {
                        stack[ht - 2]->link[dir[ht - 2]] = rPtr;
                    }
                    dir[ht] = 1;
                    stack[ht] = stack[ht - 1];
                    stack[ht - 1] = rPtr;
                    ht++;

                    rPtr = stack[ht - 1]->link[0];
                }
                if ((!rPtr->link[0] || rPtr->link[0]->color == BLACK) &&
                        (!rPtr->link[1] || rPtr->link[1]->color == BLACK))
                {
                    rPtr->color = RED;
                }
                else
                {
                    if(!rPtr->link[0] || rPtr->link[0]->color == BLACK)
                    {
                        qPtr = rPtr->link[1];
                        rPtr->color = RED;
                        rPtr->color = BLACK;
                        rPtr->link[1] = qPtr->link[0];
                        qPtr->link[0] = rPtr;
                        rPtr = stack[ht - 1]->link[0] = qPtr;
                    }
                    rPtr->color = stack[ht - 1]->color;
                    stack[ht - 1]->color = BLACK;
                    rPtr->link[0]->color = BLACK;
                    stack[ht - 1]->link[0] = rPtr->link[1];
                    rPtr->link[1] = stack[ht - 1];
                    if(stack[ht - 1] == root)
                    {
                        root = rPtr;
                    }
                    else
                    {
                        stack[ht - 2]->link[dir[ht - 2]] = rPtr;
                    }
                    break;
                }
            }
            ht--;
            pthread_mutex_unlock(&_t_lock);
        }
    }
}

struct node_t *tree_search(const char *data_t)
{
    char *data = (char *) data_t;
    struct node_t *stack[98], *ptr, *xPtr, *yPtr;
    struct node_t *pPtr, *qPtr, *rPtr;
    int dir[98], ht = 0, diff, i;
//    enum node_color color;

    if (!root)
    {
        return NULL;
    }

    ptr = root;
    while (ptr != NULL)
    {
        if(strcmp(data, ptr->data) == 0)
        {
            return ptr;
        }
        diff = strcmp(data, ptr->data) > 0 ? 1 : 0;
        stack[ht] = ptr;
        dir[ht++] = diff;
        ptr = ptr->link[diff];
    }
    return NULL;
}

void path_lock(const char *data_t, pthread_mutex_t _t_lock)
{

//    char *data = (char *) data_t;
    pthread_mutex_lock(&_t_lock);
/*    if(!tree_search(data))
    {
        pthread_mutex_unlock(&_t_lock);
        insertion(data, _t_lock);
        pthread_mutex_lock(&_t_lock);
    }
    pthread_mutex_lock(&(tree_search(data)->file_mutex));
*/	printf("path_lock(%s)\n", data_t);
    pthread_mutex_unlock(&_t_lock);
}

void path_unlock(const char *data_t, pthread_mutex_t _t_lock)
{
//    char *data = (char *) data_t;
    pthread_mutex_lock(&_t_lock);
//    pthread_mutex_unlock(&(tree_search(data)->file_mutex));
	printf("path_unlock(%s)\n", data_t);
    pthread_mutex_unlock(&_t_lock);
}

