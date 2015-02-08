// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

lock_server::lock_server() :
        nacquire(0) {
}

lock_protocol::status lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    pthread_mutex_lock(&table_lock);
    lock_iterator lr = locklist.find(lid);
    if (lr == locklist.end()) {
        lck l;
        l.clt = clt;
        l.st = true;
        locklist[lid] = l;
        pthread_mutex_unlock(&table_lock);
        return lock_protocol::OK;
    }
    else {
        lck l = locklist[lid];
        if (l.st == false) {
            l.st = true;
            l.clt = clt;
            locklist[lid] = l;
            pthread_mutex_unlock(&table_lock);
            return lock_protocol::OK;
        }
        else {
            pthread_mutex_unlock(&table_lock);
            return lock_protocol::IOERR;
        }
    }
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    pthread_mutex_lock(&table_lock);
    lock_iterator lr = locklist.find(lid);
    if (lr == locklist.end()) {
        pthread_mutex_unlock(&table_lock);
        return lock_protocol::OK;
    }
    else {
        lck l = locklist[lid];
        if (l.st == false) {
            pthread_mutex_unlock(&table_lock);
            return lock_protocol::OK;
        }
        else {
            if (l.clt == clt) {
                locklist[lid].st = false;
                pthread_mutex_unlock(&table_lock);
                return lock_protocol::OK;
            }
            else {
                pthread_mutex_unlock(&table_lock);
                return lock_protocol::RETRY;
            }
        }
    }
}




