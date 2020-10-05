#include <stdio.h>
#include <stdlib.h>

struct Node
{
    const char *data;
    struct Node* prev;
    struct Node* next;
};

struct List
{
    struct Node* head;
    struct Node* tail;
    unsigned short count;
};

void InitList(struct List **list)
{
    printf("InitList\n");
    (*list) = (struct List*)malloc(sizeof(list));
    (*list)->head = NULL;
    (*list)->tail = NULL;
    (*list)->count = 1;
}

struct Node* CreateNode(const char *data)
{
    struct Node* node = (struct Node*)malloc(sizeof(struct Node));
    node->data = data;
    node->prev = NULL;
    node->next = NULL;
    return node;
}

void AddNode(struct List **list, struct Node* node)
{   
    printf("Addnode start\n");
    if ((*list)->count == 0)
    {
   printf("addnode start if\n");
        (*list)->head = (*list)->tail = node;
        node->next = node->prev = node;
   printf("addnode end if\n");
    }
    else
    {
        node->prev = (*list)->tail;
        node->next = (*list)->head;

        (*list)->tail->next = node;
        (*list)->head->prev = node;
        (*list)->tail = node;
    }
    printf("count1 = %d\n", (*list)->count);
    (*list)->count++;
    printf("count2 = %d\n", (*list)->count);
}

struct Node* FindNode(struct List ** list, const char *data)
{
    if (!(*list)) return NULL;

    int idx = 1;
    int cnt = (*list)->count;
    struct Node* n = (*list)->head;

    while (idx <= cnt)
    {
        if (n->data == data)
        {
            return n;
        }
        n = n->next;
        idx++;
    }
    return NULL;
}

void DelNode(struct List **list, struct Node * n)
{
    if (!(*list) || !n)
    {
       free(n);
       return;
    }

    if ((*list)->count==1)
    {
        (*list)->head = (*list)->tail = NULL;
        free(n);
    }
    else
    {
        if ((*list)->head == n)
        {
            (*list)->head = n->next;
        }
        else if ((*list)->tail == n)
        {
            (*list)->tail = n->prev;
        }
        n->prev->next = n->next;
        n->next->prev = n->prev;

        free(n);
    }

    (*list)->count--;
}

void PrintAllNode(struct List**list)
{
    if (!(*list)) return;
    if ((*list)->count == 0)
    {
   printf(" ### Thread is empty ### \n");
        return;
    }
    printf(" ### Print All Thread ### \n"); 
    int idx = 1;
    int cnt = (*list)->count;
    struct Node* n = (*list)->head;
    while (idx <= cnt)
    {
   printf("[%d]%s\n", idx, n->data);
        n = n->next;
        idx++;
    }
}


void file_lock(const char *path, struct List ** list)
{
    PrintAllNode(&list);
    while(1)
    {
        if(!FindNode(&list, path))
   {
       printf("노드가 없어!\n");
       if((*list)->count < 20)
       {
      printf("20개 아래야! 그러니까 %d개라는거지!\n", (*list)->count);
      break;
       }
   }

    }
    printf("before addnode\n");
    AddNode(&list, CreateNode(path));
    PrintAllNode(&list);
    printf("락을 거니까 %d개의 노드가 있어!\n", (*list)->count);
}

void file_unlock(const char *path, struct List ** list)
{
    if(FindNode(&list, path))
    {
   printf("언락하기 전에는 %d개의 노드가 있어!\n", (*list)->count);
   printf("%s를 언락할게!\n", path);
   DelNode(&list, FindNode(&list, path));
   printf("언락하고나니까 %d개의 노드가 있어!\n", (*list)->count);
    }
}
