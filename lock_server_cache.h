#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "lock_server.h"
#include "fifo.h"
#include <stdio.h>
#include "rpc/fifo.h"
#include <map>
#include "rsm.h"

typedef enum {
    LOCKED, REVOKING, RETRYING
} lock_status_;

typedef struct {
    std::string client_address;
    lock_protocol::lockid_t lockid;
    int seq_number;
} client_info;

struct lock_t {
    lock_status_ status;
    client_info current;
    client_info next;
    pthread_mutex_t entry_lock;
    std::vector<client_info> waiting_queue;

    lock_t() {
        pthread_mutex_init(&entry_lock, NULL);
    }
};

class lock_server_cache : public rsm_state_transfer {
private:
    class rsm *rsm;

public:
    lock_server_cache(class rsm *rsm = 0);

    lock_protocol::status stat(lock_protocol::lockid_t, int &);

    lock_protocol::status acquire(std::string clt, lock_protocol::lockid_t lid, int &);

    lock_protocol::status release(std::string clt, lock_protocol::lockid_t lid, int &);

    void revoker();

    void retryer();

    std::string marshal_state();

    void unmarshal_state(std::string);
};

inline marshall &operator<<(marshall &m, client_info c) {
    m << c.client_address;
    m << c.lockid;
    m << c.seq_number;
    return m;
}

inline unmarshall &operator>>(unmarshall &u, client_info &c) {
    u >> c.client_address;
    u >> c.lockid;
    u >> c.seq_number;
    return u;
}
/*
inline marshall &operator<<(marshall &m, fifo<client_info> q) {
    client_info c;
    m << q.size();
    printf("MARSHALL FIFO S=%d\n", q.size());
    for (; q.size() > 0;) {
        q.deq(&c);
        m << c;
    }
    return m;
}

inline unmarshall &operator>>(unmarshall &m, fifo<client_info> &q) {
    int size;
    client_info c;
    m >> size;
    printf("UNMARSHALL FIFO S=%d\n", size);
    for (int i = 0; i < size; ++i) {
        m >> c;
        printf("UNMARSHALL FIFO LID=%lld\n", c.lockid);
        printf("UNMARSHALL FIFO CLID=%s\n", c.client_address.c_str());
        printf("UNMARSHALL FIFO SEQ=%d\n", c.seq_number);
        q.enq(c);
    }
    return m;
}*/

#endif
