// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>

#define MAX_THREADS 256

static void *
releasethread(void *x) {
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->releaser();
    return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
        class lock_release_user *_lu)
        : lock_client(xdst), lu(_lu) {
    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char *hname;
    // assert(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    /* register RPC handlers with rlsrpc */
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
    sequence_num = 0;
    pthread_t th;
    //int r = pthread_create(&th, NULL, &releasethread, (void *) this);
    //assert (r == 0);
}

lock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, unsigned int &num) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&lock_table[lid].entry_mutex);
    if (lock_table[lid].status == lock_info::FREE) {
        lock_table[lid].status = lock_info::RELEASED;
        pthread_mutex_unlock(&lock_table[lid].entry_mutex);
        //lu->dorelease(lid);
        ret = lock_client::release(id, lid, sequence_num);
    }
    else if (lock_table[lid].status == lock_info::RELEASED){
        pthread_mutex_unlock(&lock_table[lid].entry_mutex);
        ret = lock_client::release(id, lid, sequence_num);
    }
    else {
        lock_table[lid].status = lock_info::REVOKED;
        pthread_mutex_unlock(&lock_table[lid].entry_mutex);
    }
    return ret;
}

void
lock_client_cache::releaser() {


}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    lock_protocol::status ret = lock_protocol::IOERR;
    struct timeval now;
    while (1) {
        pthread_mutex_lock(&table_mutex);
        std::map<lock_protocol::lockid_t, struct lock_info>::iterator it = lock_table.find(lid);
        if (it != lock_table.end()) {
            /*Lock Exists*/
            pthread_mutex_unlock(&table_mutex);
            while (1) {
                pthread_mutex_lock(&(it->second).entry_mutex);
                switch ((it->second).status) {
                    case lock_info::FREE:
                        (it->second).status = lock_info::LOCKED;
                        (it->second).owner = pthread_self();
                        ret = lock_protocol::OK;
                        sequence_num++;
                        break;
                    case lock_info::RELEASED:
                        if (lock_client::acquire(id, lid, sequence_num) == lock_protocol::RETRY) {
                            pthread_mutex_lock(&(it->second).retry_mutex);
                            while (1) {
                                gettimeofday(&now, NULL);
                                struct timespec next_timeout;
                                next_timeout.tv_sec = now.tv_sec + 3;
                                next_timeout.tv_nsec = 0;
                                pthread_cond_timedwait(&(it->second).retry_cond, &(it->second).retry_mutex, &next_timeout);
                                if (id.length() == 0)
                                    printf("NULL ID\n");
                                printf("RETRYING AGAIN... : %d id:%s \n", lid, id.c_str());
                                ret = lock_client::acquire(id, lid, sequence_num);
                                if (ret == lock_protocol::OK)
                                    break;
                            }
                            pthread_mutex_unlock(&(it->second).retry_mutex);
                        }
                        else {
                        }
                        if ((it->second).status != lock_info::REVOKED)
                            (it->second).status = lock_info::LOCKED;
                        (it->second).owner = pthread_self();
                        (it->second).is_retry = false;
                        ret = lock_protocol::OK;
                        break;
                    default:
                        gettimeofday(&now, NULL);
                        struct timespec next_timeout2;
                        next_timeout2.tv_sec = now.tv_sec + 3;
                        next_timeout2.tv_nsec = 0;
                        pthread_cond_timedwait(&(it->second).default_cond, &(it->second).entry_mutex, &next_timeout2);
                }
                if (ret == lock_protocol::OK) {
                    pthread_mutex_unlock(&(it->second).entry_mutex);
                    return ret;
                }
                pthread_mutex_unlock(&(it->second).entry_mutex);
            }
        }
        else {
            /*New Lock*/
            lock_info newlock;
            newlock.status = lock_info::RELEASED;
            lock_table[lid] = newlock;
            pthread_mutex_unlock(&table_mutex);
        }
    }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&(lock_table[lid].entry_mutex));
    if (lock_table[lid].status == lock_info::REVOKED) {
        lock_table[lid].status = lock_info::RELEASED;
        pthread_mutex_unlock(&(lock_table[lid].entry_mutex));
        //if (lu != 0)
        //    lu->dorelease(lid);
        ret = lock_client::release(id, lid, sequence_num);
    }
    else {
        lock_table[lid].status = lock_info::FREE;
        pthread_mutex_unlock(&lock_table[lid].entry_mutex);
    }
    pthread_cond_signal(&lock_table[lid].default_cond);
    return ret;
}

lock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, unsigned int &num) {
    pthread_mutex_lock(&(lock_table[lid].retry_mutex));
    lock_table[lid].is_retry = true;
    pthread_cond_signal(&(lock_table[lid].retry_cond));
    pthread_mutex_unlock(&(lock_table[lid].retry_mutex));
    return lock_protocol::OK;
}

