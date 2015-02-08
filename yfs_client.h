#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>
#include "lock_client_cache.h"

class lock_release_impl : public lock_release_user {
private:
    class extent_client *ec;

public:
    lock_release_impl(extent_client *ec);

    virtual ~lock_release_impl() {
    };

    void dorelease(lock_protocol::lockid_t lid);
};

class yfs_client {
    extent_client *ec;
    lock_client_cache *lc;
    lock_release_impl *lu;

public:

    typedef unsigned long long inum;

    enum xxstatus {
        OK, RPCERR, NOENT, IOERR, FBIG
    };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };

    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };

    struct dirent {
        std::string name;
        unsigned long long inum;
    };

private:
public:

    static std::string filename(inum);

    static inum n2i(std::string);

    yfs_client(std::string, std::string);

    bool isfile(inum);

    bool isdir(inum);

    inum file_lookup(std::string, std::string name);

    int lookup(inum, std::string, inum &);

    int getfile(inum, fileinfo &);

    int getdir(inum, dirinfo &);

    int getdata(inum, std::string &);

    int createfile(inum, std::string, inum &);

    int createdir(inum, std::string, inum &);

    int remove(inum, std::string);

    int putdata(inum, std::string);

};

typedef struct X {
    unsigned long long inum;
    std::string name;
};

class string_iter {
private:
    std::string namelist;
    std::string entry_sep;
    std::string value_sep;
public:
    string_iter(std::string s);

    X next();
};

#endif 
