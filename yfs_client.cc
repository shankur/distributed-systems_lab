#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <time.h>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "lock_client.h"

lock_release_impl::lock_release_impl(extent_client *ec) {
    this->ec = ec;
}

void
lock_release_impl::dorelease(lock_protocol::lockid_t lid) {
    ec->flush_extent(lid);
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lu = new lock_release_impl(ec);
    lc = new lock_client_cache(lock_dst, lu);
}

yfs_client::inum yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum) {
    if (inum & 0x80000000)
        return true;
    return false;
}

bool yfs_client::isdir(inum inum) {
    return !isfile(inum);
}

int yfs_client::getfile(inum id, fileinfo &fin) {
    int r = OK;


    printf("getfile %016llx\n", id);
    extent_protocol::attr a;
    lc->acquire(id);
    if (ec->getattr(id, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", id, fin.size);

    release:
    lc->release(id);
    return r;
}

int yfs_client::getdir(inum id, dirinfo &din) {
    int r = OK;


    printf("getdir %016llx\n", id);
    extent_protocol::attr a;
    lc->acquire(id);
    if (ec->getattr(id, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    lc->release(id);
    return r;
}

yfs_client::inum yfs_client::file_lookup(std::string content, std::string name) {
    name = '#' + name + '%';
    if (content.find(name) == -1)
        return 0;
    else {
        size_t pos1 = content.find(name);
        size_t pos2 = content.find_last_of((char) 37, pos1);
        return yfs_client::n2i(content.substr(pos2 + 1, pos1 - pos2 - 1));
    }
}

int yfs_client::getdata(yfs_client::inum in, std::string &s) {
    int r = OK;

    std::string buf;
    if (ec->get(in, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    s = buf;

    release:
    return r;
}

int yfs_client::lookup(yfs_client::inum parent, std::string name, yfs_client::inum &id) {
    lc->acquire(parent);
    std::string content;
    if (getdata(parent, content) == yfs_client::OK) {
        id = file_lookup(content, name);
        if (id == 0) {
            lc->release(parent);
            return IOERR;
        } else {
            lc->release(parent);
            return OK;
        }
    }
    lc->release(parent);
}

int yfs_client::createfile(yfs_client::inum parent, std::string name, inum &id) {
    int r;
    lc->acquire(parent);
    //std::cerr << "Create:" << name << std::endl;
    std::string dummybuf;
    unsigned long x = rand();
    x = x | 0x80000000;
    while (ec->get(x, dummybuf) != extent_protocol::IOERR) {
        x = rand();
        x = x | 0x80000000;
    }
    lc->acquire(x);
    std::string c;
    if (getdata(parent, c) == extent_protocol::OK);
    {
        if (file_lookup(c, name) > 0) {
            id = file_lookup(c, name);
            lc->release(parent);
            lc->release(x);
            return OK;
        }
        c = c + filename(x);
        c = c + "#";
        c = c + name;
        c = c + "%";
        if (ec->put(parent, c) != extent_protocol::OK) {
            r = IOERR;
        } else {
            if (ec->put(x, "") != extent_protocol::OK) {
                r = IOERR;
            } else {
                id = x;
                std::cerr << "Create:" << name << " : " << x << std::endl;
                lc->release(parent);
                lc->release(x);
                return OK;
            }
        }

    }
    lc->release(parent);
    lc->release(x);
    return IOERR;
}

int yfs_client::createdir(yfs_client::inum parent, std::string name, yfs_client::inum &id) {
    int r;
    lc->acquire(parent);
    std::string dummybuf;
    unsigned long x = rand();
    x = x | 0x70000000;
    while (ec->get(x, dummybuf) != extent_protocol::IOERR) {
        x = rand();
        x = x | 0x70000000;
    }
    lc->acquire(x);
    std::string c;
    if (getdata(parent, c) == extent_protocol::OK);
    {
        c = c + filename(x);
        c = c + "#";
        c = c + name;
        c = c + "%";
        if (ec->put(parent, c) != extent_protocol::OK) {
            r = IOERR;
        } else {
            if (ec->put(x, "") != extent_protocol::OK) {
                r = IOERR;
            } else {
                id = x;
                lc->release(parent);
                lc->release(x);
                return OK;
            }
        }

    }
    lc->release(parent);
    lc->release(x);
    return IOERR;
}

int yfs_client::remove(yfs_client::inum parent, std::string name) {
    int r;
    std::string c;
    lc->acquire(parent);
    if (getdata(parent, c) == extent_protocol::OK);
    {
        // If file with same name exists in the parent directory, return error
        if (file_lookup(c, name) < 0) {
            lc->release(parent);
            return IOERR;
        }
        inum fiden = file_lookup(c, name);
        std::string id = filename(fiden);
        id = id + '#' + name + '%';
        std::string s1 = c.substr(0, c.find(id));
        std::string s2 = c.substr(c.find(id) + id.length(), c.length() - c.find(id) - id.length());
        c = s1 + s2;
        if (ec->put(parent, c) != extent_protocol::OK) {
            r = IOERR;
        } else {
            lc->acquire(fiden);
            if (ec->remove(fiden) != extent_protocol::OK) {
                lc->release(fiden);
                r = IOERR;
            } else {
                lc->release(parent);
                lc->release(fiden);
                return OK;
            }
        }
    }
    lc->release(parent);
    return IOERR;
}

string_iter::string_iter(std::string s) {
    namelist = s;
    entry_sep = "%";
    value_sep = "#";
}

X string_iter::next() {
    X temp;
    if (namelist.length() > 0) {
        unsigned int next_pos = namelist.find_first_of(entry_sep, 0);
        std::string new_entry = namelist.substr(0, next_pos);
        namelist = namelist.substr(next_pos + 1, namelist.length() - next_pos - 1);
        int vpos = new_entry.find_first_of(value_sep, 0);
        std::string number = new_entry.substr(0, vpos);
        std::string name = new_entry.substr(vpos + 1, new_entry.length() - vpos - 1);
        temp.inum = yfs_client::n2i(number);
        temp.name = name;
    } else
        temp.name = "";
    return temp;
}

int yfs_client::putdata(yfs_client::inum id, std::string buff) {
    lc->acquire(id);
    if (ec->put(id, buff) == extent_protocol::OK) {
        lc->release(id);
        return OK;
    } else {
        lc->release(id);
        return IOERR;
    }
}

