#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

ts_entry_t* detatchHead(ts_entry_t* entry){
  //printf("entering head datatch fn\n");
  if(entry == NULL){
    printf("BAD\n");
  }
  ts_entry_t* head = entry;
  entry = head->next;
  head->next = NULL;
  //printf("leaving\n");
  return head;

}

void rehash(ts_hashmap_t* map, int newCapacity){
  printf("rehashing\n");
  map->isRehashing = 1;
  for(int i = 0; i < map->capacity; i++){
    pthread_mutex_lock((map->locks)[i]);
  }
  printf("acquired all locks\n");
  ts_entry_t** oldTable = map->table;
  map->table = malloc(newCapacity * sizeof(ts_entry_t*));
  for(int i = 0; i < newCapacity; i++){
    (map->table)[i] = malloc(sizeof(ts_entry_t));
    (map->table)[i] = NULL;
  }
  printf("created new table\n");
  for(int i = 0; i < map->capacity; i++){
    ts_entry_t* current = oldTable[i];
    //printf("acquired head %d\n", i);
    while(current != NULL){
      ts_entry_t* temp = current->next;
      ts_entry_t* head = detatchHead(current);
      current = temp;
      int newLocation = (head->key) % newCapacity;
      //printf("finding location\n");
      ts_entry_t** table = (map->table);
      //printf("new location: %d. Array size: %d, key: %d\n", newLocation, newCapacity, head->key);
      ts_entry_t* target = table[newLocation];
      //printf("found location\n");
      if(target == NULL){
        target = head;
      }else{
        while(target->next != NULL){
          target = target->next;
        }
        target->next = head;
      }
    }
  }
  printf("all entries placed\n");
  pthread_mutex_t** oldLocks = map->locks;
  map->locks = malloc(newCapacity * sizeof(pthread_mutex_t*));
  for(int i = 0; i < map->capacity; i++){
    //if(i < newCapacity){
      (map->locks)[i] = oldLocks[i];
      pthread_mutex_unlock((map->locks)[i]);
    //}else{
    //  pthread_mutex_destroy(oldLocks[i]); //destroy extra locks if the capacity shrunk
    //}
  }
  printf("existing locks done\n");
  //printf("i = %d, i ends at: %d\n", map->capacity, newCapacity);
  //starting from where the locks left off
  for(int i = map->capacity; i < newCapacity; i++){
    //printf("%d\n", i);
    (map->locks)[i] = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init((map->locks)[i], NULL);
  }
  printf("new locks done\n");
  map->capacity = newCapacity;
  free(oldLocks);
  free(oldTable);
  map->isRehashing = 0;
  printf("done rehashing\n");
}
ts_entry_t* initEntry(int key, int value){
  printf("initEntry\n");
  ts_entry_t* entry = malloc(sizeof(ts_entry_t));
  printf("malloc good\n");
  entry->key = key;
  entry->value = value;
  entry->next = NULL;
  printf("done\n");
  return entry;
}

void updateFields(ts_hashmap_t* map, int deltaSize){
  pthread_spin_lock(map->lock);
  map->size += deltaSize;
  map->numOps ++;
  double load = ((double)map->size)/((double)map->capacity);
  if(load >= 0.75){
    //printf("load is %.3f. Size is %d, cap is %d\n", load, map->size, map->capacity);
    //rehash(map, map->capacity * 2);
  //}if(load <= 0.1 && deltaSize < 0){
    rehash(map, map->capacity / 2);
  }

  pthread_spin_unlock(map->lock);
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
  map->locks = malloc((capacity)*sizeof(pthread_mutex_t*));
  map->isRehashing = 0; //0 if not rehasing, 1 if rehashing
  map->lock = malloc(sizeof(pthread_spinlock_t));
  pthread_spin_init(map->lock, PTHREAD_PROCESS_PRIVATE);
  //set all the values to null, so we can tell which tables do and don't have entries.
  for(int i = 0; i < capacity; i++){
    (map->table)[i] = NULL;
    (map->locks)[i] = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init((map->locks)[i], NULL);
  }
  return map;
}
/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  while(map->isRehashing){
    ;
  }
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
  while(map->isRehashing){
    ;
  }
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
  while(map->isRehashing){
    ;
  }
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