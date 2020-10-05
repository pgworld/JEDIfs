#include <stdio.h>
#include <stdlib.h>
#ifndef __LOCKUTIL_H
#define __LOCKUTIL_H

struct Node
{
    const char *data;
    struct Node *prev;
    struct Node *next;
};

struct List
{
    struct Node* head;
    struct Node* tail;
    int count;
};

void InitList(struct List **list);

struct Node* CreateNode(const char* data);

void AddNode(struct List **list, struct Node* node);

struct Node* FindNode(struct List **list, const char* data);

void DelNode(struct List **list, struct Node *n);

void PrintAllNode(struct List **list);

void file_lock(const char* path, struct List **list);

void file_unlock(const char* path, struct List **list);

#endif /* __LOCKUTIL_H */
