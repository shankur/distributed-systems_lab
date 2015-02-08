// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <pthread.h>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

typedef struct lock_status {
    int clt;
    bool st;
} lck;

typedef std::map <lock_protocol::lockid_t, lck> lock;
typedef std::map<lock_protocol::lockid_t, lck>::iterator lock_iterator;

class lock_server {
private:
    lock locklist;
    pthread_mutex_t table_lock;

protected:
    int nacquire;

public:
    lock_server();

    ~lock_server() {
    };

    lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);

    lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);

    lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







