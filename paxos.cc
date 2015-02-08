#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include <map>
//#include <SpriteKit/SpriteKit.h>
#include "connection_handler.cc"
#include <pthread.h>
#include <string>
//#include <AVFoundation/AVFoundation.h>

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.

std::map<int, connection_handler *> conns;
std::map<int, paxos_protocol::preparearg> instance_value;
pthread_mutex_t connect_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;

int portaddr_s(std::string addr) {
    int p;
    addr = addr.substr(addr.find(":") + 1, addr.length() - addr.find(":") + 1);
    p = atoi(addr.c_str());
    return p;
}

bool
operator>(const prop_t &a, const prop_t &b) {
    return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>=(const prop_t &a, const prop_t &b) {
    return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes) {
    std::string s;
    s.clear();
    for (unsigned i = 0; i < nodes.size(); i++) {
        s += nodes[i];
        if (i < (nodes.size() - 1))
            s += ",";
    }
    return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes) {
    for (unsigned i = 0; i < nodes.size(); i++) {
        if (nodes[i] == m) return 1;
    }
    return 0;
}

bool
proposer::isrunning() {
    bool r;
    assert(pthread_mutex_lock(&pxs_mutex) == 0);
    r = !stable;
    assert(pthread_mutex_unlock(&pxs_mutex) == 0);
    return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1,
        const std::vector<std::string> &l2) {
    unsigned n = 0;

    for (unsigned i = 0; i < l1.size(); i++) {
        if (isamember(l1[i], l2))
            n++;
    }
    return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor,
        std::string _me)
        : cfg(_cfg), acc(_acceptor), me(_me), break1(false), break2(false),
          stable(true) {
    assert(pthread_mutex_init(&pxs_mutex, NULL) == 0);

}

void
proposer::setn() {
    my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> newnodes, std::string newv) {
    std::vector<std::string> accepts;
    std::vector<std::string> nodes;
    std::vector<std::string> nodes1;
    std::string v;
    bool r = false;

    pthread_mutex_lock(&pxs_mutex);
    if (!stable) {  // already running proposer?
        printf("proposer::run: already running\n");
        pthread_mutex_unlock(&pxs_mutex);
        return false;
    }
    my_n.m = me;
    setn();
    accepts.clear();
    nodes.clear();
    v.clear();
    if (newnodes.size() == 0)
        newnodes.push_back(me);
    nodes = newnodes;
    //printf("start: before: me=%s initiate paxos for %s w. i=%d v=%s stable=%d\n",me.c_str(), print_members(nodes).c_str(), instance, newv.c_str(), stable);
    for (std::vector<std::string>::iterator it = nodes.begin(); it != nodes.end();) {
        if (newv.find((*it).c_str()) == std::string::npos) {
            int port_s = portaddr_s(*it);
            nodes.erase(it);
            pthread_mutex_lock(&connect_lock);
            std::map<int, connection_handler *>::iterator citer;
            citer = conns.find(port_s);
            if (citer != conns.end()) {
                connection_handler *c = citer->second;
                conns.erase(port_s);
                delete(c);
            }
            pthread_mutex_unlock(&connect_lock);
            if (it == nodes.end())
                break;
            continue;
        }
        it++;
    }
    printf("paxos::man: me=%s initiate paxos for %s w. i=%d v=%s stable=%d\n", me.c_str(), print_members(nodes).c_str(), instance, newv.c_str(), stable);
    if (prepare(instance, accepts, nodes, v)) {
        //printf("Prepare accept list: %s", print_members(accepts).c_str());
        if (majority(newnodes, accepts)) {
            printf("paxos::manager:me %s: received a majority of prepare responses\n", me.c_str());

            if (v.size() == 0) {
                v = newv;
            }

            breakpoint1();

            nodes1 = accepts;
            accepts.clear();
            printf("paxos::manager:me %s: sent accept %s\n", me.c_str(), print_members(nodes1).c_str());
            accept(instance, accepts, nodes1, v);

            if (majority(newnodes, accepts)) {
                printf("paxos::manager:me %s: received a majority of accept responses from, %s\n", me.c_str(), print_members(accepts).c_str());

                breakpoint2();
                decide(instance, accepts, v);
                r = true;
            } else {
                printf("paxos::manager:me %s: no majority of accept responses\n", me.c_str());
            }
        } else {
            printf("paxos::manager:me %s: no majority of prepare responses: accepts only from: %s\n", me.c_str(), print_members(accepts).c_str());
        }
    } else {
        printf("paxos::man:me %s: prepare is rejected %d  v=%s\n", me.c_str(), stable, v.c_str());
    }
    stable = true;
    pthread_mutex_unlock(&pxs_mutex);
    return r;
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes,
        std::string &v) {
    paxos_protocol::preparearg preparg;
    preparg.instance = instance;
    preparg.n = my_n;
    preparg.v = v;
    paxos_protocol::prepareres presult;
    printf("paxos::prep: i=%d v=%s me=%s\n", instance, print_members(nodes).c_str(), me.c_str());
    for (std::vector<std::string>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        pthread_mutex_lock(&connect_lock);
        std::map<int, connection_handler *>::iterator citer;
        citer = conns.find(portaddr_s(*it));
        connection_handler *c = citer->second;
        if (citer == conns.end()) {
            c = new connection_handler(*it);
            conns[portaddr_s(*it)] = c;
        }
        pthread_mutex_unlock(&connect_lock);
        if (c->preparereq(me, preparg, presult) == paxos_protocol::OK) {
            if (presult.accept == 1) {
                v = presult.v_a;
                preparg.v = v;
                accepts.push_back(*it);
                printf("paxos::prep: i=%d me %s: accept from %s\n", instance, me.c_str(), (*it).c_str());
            }
            else if (presult.oldinstance == 1) {
                v = presult.v_a;
                acc->commit(instance, presult.v_a);
                printf("paxos::prep: i=%d :me %s: old instance from %s\n", instance, me.c_str(), (*it).c_str());
                return false;
            }
            else {
                return false;
            }
        }
    }
    return true;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v) {
    paxos_protocol::acceptarg accarg;
    accarg.instance = instance;
    accarg.n = my_n;
    accarg.v = v;
    for (std::vector<std::string>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        pthread_mutex_lock(&connect_lock);
        std::map<int, connection_handler *>::iterator citer;
        citer = conns.find(portaddr_s(*it));
        connection_handler *c = citer->second;
        if (citer == conns.end()) {
            c = new connection_handler(*it);
            conns[portaddr_s(*it)] = c;
        }
        pthread_mutex_unlock(&connect_lock);
        int r = 0;
        if (c->acceptreq(me, accarg, r) == paxos_protocol::OK)
            accepts.push_back(*it);
    }
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts,
        std::string v) {
    paxos_protocol::decidearg d;
    d.instance = instance;
    d.v = v;
    for (std::vector<std::string>::iterator it = accepts.begin(); it != accepts.end(); it++) {
        pthread_mutex_lock(&connect_lock);
        std::map<int, connection_handler *>::iterator citer;
        citer = conns.find(portaddr_s(*it));
        connection_handler *c = citer->second;
        if (citer == conns.end()) {
            c = new connection_handler(*it);
            conns[portaddr_s(*it)] = c;
        }
        pthread_mutex_unlock(&connect_lock);
        int r = 0;
        c->decidereq(me, d, r);
    }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me,
        std::string _value)
        : cfg(_cfg), me(_me), instance_h(0) {
    assert(pthread_mutex_init(&pxs_mutex, NULL) == 0);

    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();

    l = new log(this, me);

    if (instance_h == 0 && _first) {
        values[1] = _value;
        l->loginstance(1, _value);
        instance_h = 1;
    }

    pxs = new rpcs(atoi(_me.c_str()));
    pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
    pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
    pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
        paxos_protocol::prepareres &r) {
    // handle a preparereq message from proposer
    printf("paxos:man: (Acceptor) me=%s n=%d n_h=%d i=%d instance_h=%d\n", me.c_str(), a.n.n, n_h.n, a.instance, instance_h);
    if (a.instance <= instance_h) {
        printf("paxos::man: (Acceptor) me:%s old instance : i=%d, v=%s\n", me.c_str(), a.instance, values[a.instance].c_str());
        r.oldinstance = 1;
        r.accept = 0;
        r.v_a = values[a.instance];
        return paxos_protocol::OK;
    }
    else if (a.n > n_h) {
        r.accept = 1;
        r.oldinstance = 0;
        n_h = a.n;
        if (v_a.size() != 0)
            r.v_a = v_a;
        else
            r.v_a = a.v;
        l->loghigh(n_h);
        return paxos_protocol::OK;
    }
    r.accept = 0;
    return paxos_protocol::OK;

}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r) {

    // handle an acceptreq message from proposer
    std::cout << "paxos::man: values of acceptor: ";
    for (std::map<unsigned, std::string>::iterator it = values.begin(); it != values.end(); it++) {
        std::cout << it->first << "->" << it->second << " : ";
    }
    std::cout << std::endl;

    if (a.instance <= instance_h) {
        return paxos_protocol::ERR;
    }
    else if (a.n >= n_h) {
        n_a = a.n;
        v_a = a.v;
        l->logprop(n_a, v_a);
        return paxos_protocol::OK;
    }

    return paxos_protocol::ERR;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r) {

    // handle an decide message from proposer
    if (a.instance < instance_h) {
        return paxos_protocol::ERR;
    }
    else {
        commit(a.instance, a.v);
        return paxos_protocol::OK;
    }
}

void
acceptor::commit_wo(unsigned instance, std::string value) {
    //assume pxs_mutex is held
    if (instance > instance_h) {
        values[instance] = value;
        l->loginstance(instance, value);
        instance_h = instance;
        n_h.n = 0;
        n_h.m = me;
        n_a.n = 0;
        n_a.m = me;
        v_a.clear();
        std::cout << "paxos::man: me: " << me << " commit: i=" << instance << " v=" << value << " :BEFORE CFG " << std::endl;
        if (cfg) {
            pthread_mutex_unlock(&pxs_mutex);
            std::cout << "paxos::man: me: " << me << " commit: i=" << instance << " v=" << value << " :CALLING PAXOS_COMMIT" << std::endl;
            cfg->paxos_commit(instance, value);
            std::cout << "paxos::man: me: " << me << " commit: i=" << instance << " v=" << value << " :PAXOS_COMMIT SUCCESS" << std::endl;
            pthread_mutex_lock(&pxs_mutex);
        }
    }
    else
        std::cout << "paxos::man: me: " << me << " commit: i=" << instance << " v=" << value << " :NOT LOGGED " << std::endl;
}

void
acceptor::commit(unsigned instance, std::string value) {
    pthread_mutex_lock(&pxs_mutex);
    std::cout << "paxos::man: me: " << me << " commit: i=" << instance << " v=" << value << std::endl;
    commit_wo(instance, value);
    pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump() {
    return l->dump();
}

void
acceptor::restore(std::string s) {
    l->restore(s);
    l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1() {
    if (break1) {
        printf("Dying at breakpoint 1!\n");
        exit(1);
    }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2() {
    if (break2) {
        printf("Dying at breakpoint 2!\n");
        exit(1);
    }
}

void
proposer::breakpoint(int b) {
    if (b == 3) {
        printf("Proposer: breakpoint 1\n");
        break1 = true;
    } else if (b == 4) {
        printf("Proposer: breakpoint 2\n");
        break2 = true;
    }
}
