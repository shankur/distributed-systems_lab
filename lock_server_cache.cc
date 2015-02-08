// the caching lock server implementation
/*
 This is an implementation of two phase locking protocol.
 First acquire rpc call is always a prepare, even if the 
 lock was actually free. This reduces the amount of veri-
 -fication we need to do in order to grant the locking pe- 
 -ermissions. The client resends the acquire message after
 it actually receives the retry rpc call from the server.
 */

#include "lock_server_cache.h"
#include "lock_client_cache.h"
#include "connection_handler.cc"
#include "marshall.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <cstdlib>
#include <stdlib.h>
#include <arpa/inet.h>
#include <map>
#include <sys/stat.h>
//#include <PubSub/PubSub.h>
//#include <Foundation/Foundation.h>

pthread_mutex_t retryer_lock;
pthread_mutex_t revoker_lock;
std::vector<client_info> retry_queue;
std::vector<client_info> revoke_queue;
pthread_mutex_t table_lock;
pthread_mutex_t connections_lock;
std::map<lock_protocol::lockid_t, lock_t> lock_catalog;
std::map<int, connection_handler *> connections;

std::string itoa(int i) {
    std::string s = "";
    if (i == 0)
        return "0";
    while (i > 0) {
        s = (char)(i%10 + 48) + s;
        i = i / 10;
    }
    return s;
}

int portaddr(std::string addr) {
    int p;
    addr = addr.substr(addr.find(":") + 1, addr.length() - addr.find(":") + 1);
    p = atoi(addr.c_str());
    return p;
}

static void *revokethread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->revoker();
    return 0;
}

static void *retrythread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->retryer();
    return 0;
}

lock_t ParseLock(std::vector<std::string> &v, lock_protocol::lockid_t &lid);
client_info ParseClientInfo(std::vector<std::string> &v);
std::vector<client_info> ParseFifo(std::vector<std::string> &v);
void PackLock(std::vector<std::string> &v,lock_protocol::lockid_t lid, lock_t l);
void PackClientInfo(std::vector<std::string> &v, client_info cl);
void PackFifo(std::vector<std::string> &v, std::vector <client_info> f);

lock_server_cache::lock_server_cache(class rsm *_rsm)
        : rsm(_rsm) {
    pthread_mutex_init(&table_lock, NULL);
    pthread_mutex_init(&retryer_lock, NULL);
    pthread_mutex_init(&revoker_lock, NULL);
    pthread_t th;
    rsm->set_state_transfer(this);
    int r = pthread_create(&th, NULL, &revokethread, (void *) this);
    assert (r == 0);
    r = pthread_create(&th, NULL, &retrythread, (void *) this);
    assert (r == 0);
}

void
lock_server_cache::revoker() {
    /* when woke up create a new thread to handle the revoke so that multiple revokes can be invoked at the same time */
    client_info revoke_client;
    connection_handler *cl_handle;
    std::map<int, connection_handler *>::iterator it;
    int ret;

    while (1) {
        pthread_mutex_lock(&revoker_lock);
		if(revoke_queue.size() ==  0) {
        	pthread_mutex_unlock(&revoker_lock);
			continue;
		}
        revoke_client = revoke_queue.front();
        revoke_queue.erase(revoke_queue.begin());
        pthread_mutex_unlock(&revoker_lock);
        ret = portaddr(revoke_client.client_address);
        pthread_mutex_lock(&connections_lock);
        it = connections.find(ret);
        if (it != connections.end()) {
            cl_handle = connections[ret];
            pthread_mutex_unlock(&connections_lock);
        }
        else {
            cl_handle = new connection_handler(revoke_client.client_address);
            connections[ret] = cl_handle;
            pthread_mutex_unlock(&connections_lock);
        }
        if (rsm->amiprimary()) {
            bool t = true;
            pthread_mutex_lock(&lock_catalog[revoke_client.lockid].entry_lock);
            if (lock_catalog[revoke_client.lockid].current.client_address != revoke_client.client_address)
                t = false;
            pthread_mutex_unlock(&lock_catalog[revoke_client.lockid].entry_lock);
            if (t)
                cl_handle->revoke(revoke_client.lockid, ret);
        }
    }
}

void
lock_server_cache::retryer() {
    /* when wake up, create a new thread to handle the retry so that multiple retries at the same time */
    client_info retry_client;
    connection_handler *cl_handle;
    std::map<int, connection_handler *>::iterator it;
    int ret;

    while (1) {
        pthread_mutex_lock(&retryer_lock);
		if(retry_queue.size() ==  0) {
        	pthread_mutex_unlock(&retryer_lock);
			continue;
		}
        retry_client = retry_queue.front();
        retry_queue.erase(retry_queue.begin());
        pthread_mutex_unlock(&retryer_lock);
        ret = portaddr(retry_client.client_address);
        pthread_mutex_lock(&connections_lock);
        it = connections.find(ret);
        if (it != connections.end()) {
            cl_handle = connections[ret];
            pthread_mutex_unlock(&connections_lock);
        }
        else {
            cl_handle = new connection_handler(retry_client.client_address);
            connections[ret] = cl_handle;
            pthread_mutex_unlock(&connections_lock);
        }
        if (rsm->amiprimary())
            cl_handle->retry(retry_client.lockid, ret);
    }
}

lock_protocol::status lock_server_cache::acquire(std::string clt, lock_protocol::lockid_t lid, int &r) {
    client_info lock_request;
    lock_request.client_address = clt;
    lock_request.lockid = lid;
    lock_request.seq_number = 0;
    pthread_mutex_lock(&table_lock);
    std::map<lock_protocol::lockid_t, lock_t>::iterator it = lock_catalog.find(lid);
    if (it != lock_catalog.end()) {
        /*Existing Lock*/
        pthread_mutex_lock(&(it->second).entry_lock);
        pthread_mutex_unlock(&table_lock);
        std::vector<client_info>::iterator itv;
        switch ((it->second).status) {
            case LOCKED:
                if ((it->second).current.client_address == clt) {
                    pthread_mutex_unlock(&(it->second).entry_lock);
                    return lock_protocol::OK;
                }
                itv = (it->second).waiting_queue.begin();
                for (; itv != (it->second).waiting_queue.end(); ++itv) {
                    if ((*itv).client_address == clt || clt == (it->second).next.client_address) {
                        revoke_queue.push_back((it->second).current);
                        pthread_mutex_unlock(&(it->second).entry_lock);
                        return lock_protocol::RETRY;
                    }
                }
                (it->second).status = REVOKING;
                (it->second).waiting_queue.push_back(lock_request);
                revoke_queue.push_back((it->second).current);
                pthread_mutex_unlock(&(it->second).entry_lock);
                return lock_protocol::RETRY;
            case REVOKING:
                if ((it->second).current.client_address == clt) {
                    pthread_mutex_unlock(&(it->second).entry_lock);
                    return lock_protocol::OK;
                }
                itv = (it->second).waiting_queue.begin();
                for (; itv != (it->second).waiting_queue.end(); ++itv) {
                    if ((*itv).client_address == clt || clt == (it->second).next.client_address) {
                        revoke_queue.push_back((it->second).current);
                        pthread_mutex_unlock(&(it->second).entry_lock);
                        return lock_protocol::RETRY;
                    }
                }
                (it->second).waiting_queue.push_back(lock_request);
                pthread_mutex_unlock(&(it->second).entry_lock);
                return lock_protocol::RETRY;
            case RETRYING:
                if (clt == (it->second).next.client_address) {
                    /*GRANT*/
                    if ((it->second).waiting_queue.size()) {
                        (it->second).current = (it->second).next;
                        revoke_queue.push_back((it->second).current);
                        (it->second).status = REVOKING;
                        pthread_mutex_unlock(&(it->second).entry_lock);
                        return lock_protocol::OK;
                    }
                    else {
                        (it->second).current = (it->second).next;
                        (it->second).status = LOCKED;
                        pthread_mutex_unlock(&(it->second).entry_lock);
                        return lock_protocol::OK;
                    }
                }
                else {
                    itv = (it->second).waiting_queue.begin();
                    for (; itv != (it->second).waiting_queue.end(); ++itv) {
                        if ((*itv).client_address == clt || clt == (it->second).next.client_address) {
                            pthread_mutex_unlock(&(it->second).entry_lock);
                            return lock_protocol::RETRY;
                        }
                    }
                    (it->second).waiting_queue.push_back(lock_request);
                    pthread_mutex_unlock(&(it->second).entry_lock);
                    return lock_protocol::RETRY;
                }
            default:
                pthread_mutex_unlock(&(it->second).entry_lock);
                return lock_protocol::RETRY;
        }
    }
    else {
        /*New Lock*/
        lock_t newlock;
        newlock.status = LOCKED;
        newlock.current = lock_request;
        pthread_mutex_init(&newlock.entry_lock, NULL);
        client_info next;
        next.client_address = "";
        next.lockid = lid;
        next.seq_number = 0;
        newlock.next = next;
        lock_catalog[lid] = newlock;
        pthread_mutex_unlock(&table_lock);
        return lock_protocol::OK;
    }

}

lock_protocol::status lock_server_cache::release(std::string clt, lock_protocol::lockid_t lid, int &r) {
    pthread_mutex_lock(&table_lock);
    std::map<lock_protocol::lockid_t, lock_t>::iterator it = lock_catalog.find(lid);
    pthread_mutex_lock(&(it->second).entry_lock);
    pthread_mutex_unlock(&table_lock);
    if (clt == (it->second).current.client_address) {
        (it->second).status = RETRYING;
        (it->second).next = (it->second).waiting_queue.front();
        (it->second).waiting_queue.erase((it->second).waiting_queue.begin());
        retry_queue.push_back((it->second).next);
        pthread_mutex_unlock(&(it->second).entry_lock);
        return lock_protocol::OK;
    }
    else {
        pthread_mutex_unlock(&(it->second).entry_lock);
        return lock_protocol::OK;
    }
}

lock_protocol::status lock_server_cache::stat(lock_protocol::lockid_t lid, int &r) {
    return lock_protocol::OK;
}

std::string lock_server_cache::marshal_state() {
    std::vector<std::string> v;
    v.clear();
    pthread_mutex_lock(&table_lock);
    v.push_back(itoa(lock_catalog.size()));
    for(std::map<lock_protocol::lockid_t, lock_t>::iterator it = lock_catalog.begin(); it != lock_catalog.end(); ++it)
    {
        PackLock(v, it->first, it->second);
    }
    pthread_mutex_unlock(&table_lock);

    pthread_mutex_lock(&retryer_lock);
    PackFifo(v, retry_queue);
    pthread_mutex_unlock(&retryer_lock);

    pthread_mutex_lock(&revoker_lock);
    PackFifo(v, revoke_queue);
    pthread_mutex_unlock(&revoker_lock);

    marshall m;
    m << v;
    return m.str();
}

void lock_server_cache::unmarshal_state(std::string st) {
    unmarshall u(st);
    std::vector<std::string> v;
    v.clear();
    u >> v;
    int n = atoi(v.front().c_str());
    v.erase(v.begin());
    pthread_mutex_lock(&table_lock);
    for (int i = 0; i < n; ++i)
    {
        lock_protocol::lockid_t lid;
        lock_t l = ParseLock(v, lid);
        lock_catalog[lid] = l;
    }
    pthread_mutex_unlock(&table_lock);
    pthread_mutex_lock(&retryer_lock);
    retry_queue = ParseFifo(v);
    pthread_mutex_unlock(&retryer_lock);

    pthread_mutex_lock(&revoker_lock);
    revoke_queue = ParseFifo(v);
    pthread_mutex_unlock(&revoker_lock);
}


lock_t ParseLock(std::vector<std::string> &v,lock_protocol::lockid_t &lid) {
    lid = atoi(v.front().c_str());
    v.erase(v.begin());
    int stat = atoi(v.front().c_str());
    v.erase(v.begin());
    client_info current = ParseClientInfo(v);
    client_info next = ParseClientInfo(v);
    std::vector <client_info> wq = ParseFifo(v);
    lock_t l;
    l.status = (lock_status_)stat;
    l.current = current;
    l.next = next;
    pthread_mutex_init(&l.entry_lock, NULL);
    l.waiting_queue = wq;
    return l;
}

client_info ParseClientInfo(std::vector<std::string> &v) {
    client_info cl;
    cl.lockid = atoi(v.front().c_str());
    v.erase(v.begin());
    cl.client_address = v.front();
    v.erase(v.begin());
    cl.seq_number = atoi(v.front().c_str());
    v.erase(v.begin());
    return cl;
}

std::vector <client_info> ParseFifo(std::vector<std::string> &v) {
    std::vector <client_info> q;
    int n = atoi(v.front().c_str());
    v.erase(v.begin());
    for (int i = 0; i < n; ++i)
    {
        q.push_back(ParseClientInfo(v));
    }
    return q;
}

void PackLock(std::vector<std::string> &v,lock_protocol::lockid_t lid, lock_t l) {
    int l_id = lid;
    v.push_back(itoa(l_id));
    v.push_back(itoa((int)l.status));
    PackClientInfo(v, l.current);
    PackClientInfo(v, l.next);
    PackFifo(v, l.waiting_queue);
}

void PackClientInfo(std::vector<std::string> &v, client_info cl) {
    int l_id = cl.lockid;
    v.push_back(itoa(l_id));
    v.push_back(cl.client_address);
    std::string k(itoa(cl.seq_number));
    v.push_back(k);
}

void PackFifo(std::vector<std::string> &v, std::vector<client_info> f) {
    v.push_back(itoa(f.size()));
    for (std::vector<client_info>::iterator it = f.begin(); it != f.end(); ++it) {
        PackClientInfo(v, *it);
    }
}



