// lock client interface.

#ifndef lock_client_h
#define lock_client_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_client.h"
#include <vector>

// Client interface to the lock server
class lock_client {
protected:
    rsm_client *cl;
public:
    lock_client(std::string d);

    virtual ~lock_client() {
    };

    virtual lock_protocol::status acquire(std::string id, lock_protocol::lockid_t, int &s);

    virtual lock_protocol::status release(std::string id, lock_protocol::lockid_t, int &s);

    virtual lock_protocol::status stat(lock_protocol::lockid_t);
};


#endif 
