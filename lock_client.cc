// RPC stubs for clients to talk to lock_server

#include "rsm_client.h"
#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

lock_client::lock_client(std::string dst) {
    cl = new rsm_client(dst);
}

int lock_client::stat(lock_protocol::lockid_t lid) {
    int r;
    int ret = lock_protocol::OK;
    //int ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
    assert(ret == lock_protocol::OK);
    return r;
}

lock_protocol::status lock_client::acquire(std::string id, lock_protocol::lockid_t lid,
        int &s) {
    int ret = cl->call(lock_protocol::acquire, id, lid, s);
    return ret;
}

lock_protocol::status lock_client::release(std::string id, lock_protocol::lockid_t lid,
        int &s) {
    int ret = cl->call(lock_protocol::release, id, lid, s);
    return ret;
}
