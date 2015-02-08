// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "extent_protocol.h"

typedef struct finfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
    std::string content;
};

typedef std::map<unsigned long long, finfo> metatable;
typedef std::map<unsigned long long, finfo>::iterator metatable_iterator;

class extent_server {
public:
    extent_server();

    metatable metadata;

    int put(extent_protocol::extentid_t id, std::string, int &);

    int get(extent_protocol::extentid_t id, std::string &);

    int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);

    int remove(extent_protocol::extentid_t id, int &);
};

#endif 







