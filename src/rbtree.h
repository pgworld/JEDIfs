#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
/*
enum node_color
{
	RED,
	BLACK
};

enum lock_states
{
	LOCKED,
	UNLOCKED
};
*/
struct node_t;
/*{
	char *data;
	int color, lock_state;
	struct node_t *link[2];
};*/

//struct node_t *root = NULL;

/**
 * Create a red-black tree
 *
 */
struct node_t *create_node(char *inp);

/**
 * Insert a node
 *
 */
void insertion(char *inp, pthread_mutex_t _t_lock);

/**
 * Delete a node
 *
 */
void deletion(char *data, pthread_mutex_t _t_lock);

/**
 * Print the inorder traversal of the tree
 *
 */
struct node_t *tree_search(char *data);

void path_lock(char *data, pthread_mutex_t _t_lock);

void path_unlock(char *data, pthread_mutex_t _t_lock);

int check_lock(char *data, pthread_mutex_t _t_lock);

