#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

ts_entry_t* initEntry(int key, int value){
  ts_entry_t* entry = malloc(sizeof(ts_entry_t));
  entry->key = key;
  entry->value = value;
  entry->next = NULL;
  return entry;
}

void updateFields(ts_hashmap_t* map, int deltaSize){
  pthread_mutex_t* lock = (map->locks)[map->capacity];
  pthread_mutex_lock(lock);
  map->size += deltaSize;
  map->numOps ++;
  pthread_mutex_unlock(lock);  
}
/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  //this method is threadsafe because it is only initializing new data and returning the location of this data
  ts_hashmap_t* map = malloc(sizeof (ts_hashmap_t));
  map->capacity = capacity;
  map->size = 0;
  map->numOps = 0;
  map->table = malloc(capacity * sizeof(ts_entry_t*));
  map->locks = malloc((1+capacity)*sizeof(pthread_mutex_t*));
  //set all the values to null, so we can tell which tables do and don't have entries.
  for(int i = 0; i < capacity; i++){
    (map->table)[i] = NULL;
    (map->locks)[i] = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init((map->locks)[i], NULL);
  }
  //locks[capacity] is what i am calling the "opLock". It protects mutual exclusion in the numOps and size fields, as those cannot be protected on a per_thread basis.
  (map->locks)[capacity] = malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init((map->locks)[capacity], NULL);
  return map;
}
/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  //printf("starting get\n");
  int location = key%(map->capacity); //non CS
  pthread_mutex_t* lock = (map->locks)[location];
  pthread_mutex_lock(lock);
  ts_entry_t* entry = (map->table)[location]; //CS (stays in CS until entry has finished being written to)
  if(entry == NULL){
    pthread_mutex_unlock(lock);
    updateFields(map, 0);
    return INT_MAX;
  }
  while(entry->next != NULL){
    entry = entry->next;
  }
  pthread_mutex_unlock(lock);
  updateFields(map, 0);
  //printf("get complete\n");
  return entry->value; //CS (kinda)

}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  //printf("starting put\n");
  int location = key%(map->capacity);
  pthread_mutex_t* lock = (map->locks)[location];
  pthread_mutex_lock(lock);
  ts_entry_t* entry = (map->table)[location];
  if(entry == NULL){
    (map->table)[location] = initEntry(key, value);
    map->size ++;
    pthread_mutex_unlock(lock);
    updateFields(map, 1);
    //printf("put complete\n");
    return INT_MAX;
  }
  while(1){
    if(entry->key == key){
      int oldVal = entry->value;
      entry->value = value;
      map->size ++;
      pthread_mutex_unlock(lock);
      updateFields(map, 1);
      //printf("put complete\n");
      return oldVal;
    }
    if(entry->next == NULL){
      entry->next = initEntry(key, value);
      map->size ++;
      pthread_mutex_unlock(lock);
      updateFields(map, 1);
      //printf("put complete\n");
      return INT_MAX;
    }
    entry = entry->next;
  } 
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  //printf("starting del\n");
  int location = key%(map->capacity);
  pthread_mutex_t* lock = (map->locks)[location];
  pthread_mutex_lock(lock);
  ts_entry_t* entry = (map->table)[location];
  int oldVal;
  if(entry == NULL){
    pthread_mutex_unlock(lock);
    updateFields(map, 0);
    //printf("del complete\n");
    return INT_MAX;
  }
  if(entry -> key == key){
    oldVal = entry->value;
    (map->table)[location] = entry->next;
    free(entry);
    pthread_mutex_unlock(lock);
    updateFields(map, -1);
    //printf("del complete\n");
    return oldVal;
  }
  ts_entry_t* previous;
  while(entry->next != NULL){
    previous = entry;
    entry = entry -> next;
    if(entry->key == key){
      oldVal = entry->value;
      previous->next = entry->next;
      free(entry);
      pthread_mutex_unlock(lock);
      updateFields(map, -1);
      //printf("del complete\n");
      return oldVal;
    }
  }
  pthread_mutex_unlock(lock);
  updateFields(map, 0);
  //printf("del complete\n");
  return INT_MAX;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  ts_entry_t* entry;
  ts_entry_t* old;
  for(int i = 0; i < map->capacity; i++){
    entry = (map->table)[i];
    while(entry != NULL){
      old = entry;
      entry = entry->next;
      free(old);
    }
    pthread_mutex_destroy((map->locks)[i]);
  }
  free((map->locks)[map->capacity]);
  free(map->locks);
  free(map->table);
  free(map);
  // TODO: iterate through each list, free up all nodes
  // TODO: free the hash table
  // TODO: destroy locks
}