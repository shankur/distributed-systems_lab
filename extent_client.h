// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"
#include <map>
#include <pthread.h>

class extent_client {
private:
    rpcc *cl;
    typedef struct {
        extent_protocol::attr ext_attr;
        std::string data;
        bool is_dirty;
    } extent_info;

    std::map<extent_protocol::extentid_t, extent_info *> local_extent_cache;
    typedef std::map<extent_protocol::extentid_t, extent_info *>::iterator cache_iter;
    pthread_mutex_t cache_lock;

public:
    extent_client(std::string dst);

    extent_protocol::status load_extent(extent_protocol::extentid_t eid);

    extent_protocol::status flush_extent(extent_protocol::extentid_t eid);

    extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);

    extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);

    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);

    extent_protocol::status remove(extent_protocol::extentid_t eid);
};

#endif 

