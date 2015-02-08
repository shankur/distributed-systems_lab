#ifndef CONNECTION_HANDLER
#define CONNECTION_HANDLER

#include "rpc.h"
#include "lock_protocol.h"
#include <arpa/inet.h>
#include "lock_server_cache.h"
#include "paxos_protocol.h"
#include "rpc/rpc.h"

#include <sstream>
#include <iostream>
#include <stdio.h>
#include <string>
//#include <IMServicePlugIn/IMServicePlugIn.h>

class connection_handler {
protected:
    rpcc *client;
    std::string hostid;
    std::string portid;
public:
    connection_handler(std::string handle) {
        sockaddr_in dstsock;
        //std::string host, port;
        //split(handle, &host, &port);
        make_sockaddr(handle.c_str(), &dstsock);
        client = new rpcc(dstsock);
        if (client->bind() < 0) {
            printf("lock_client_cache: call bind\n");
        }
    }

    void split(std::string addr, std::string *host, std::string *port) {
        size_t index;
        index = addr.find(":");
        *host = addr.substr(0, index);
        *port = addr.substr(index + 1, addr.length() - index + 1);
    }

    void retry(lock_protocol::lockid_t lid, int r) {
        while (client->call(rlock_protocol::retry, lid, r) != lock_protocol::OK);
    }

    void revoke(lock_protocol::lockid_t lid, int r) {
        while (client->call(rlock_protocol::revoke, lid, r) != lock_protocol::OK);
    }

    paxos_protocol::status preparereq(std::string src, paxos_protocol::preparearg a, paxos_protocol::prepareres &r) {
        return client->call(paxos_protocol::preparereq, src, a, r, rpcc::to(1000));
    }

    paxos_protocol::status acceptreq(std::string src, paxos_protocol::acceptarg a, int &r) {
        return client->call(paxos_protocol::acceptreq, src, a, r, rpcc::to(1000));
    }

    paxos_protocol::status decidereq(std::string src, paxos_protocol::decidearg a, int &r) {
        return client->call(paxos_protocol::decidereq, src, a, r, rpcc::to(1000));
    }

    rsm_client_protocol::status invoke(int procno, viewstamp vs, std::string req, int &r) {
        return client->call(rsm_protocol::invoke, procno, vs, req, r, rpcc::to(1000));
    }

    void heartbeat() {
        return;
    }
};

#endif
