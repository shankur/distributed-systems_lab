// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
    metatable_iterator miter = metadata.find(0x00000001);
    if (miter == metadata.end()) {
        finfo root;
        root.size = 0;
        root.atime = 0;
        root.mtime = 0;
        root.ctime = 0;
        root.content = "";
        metadata[0x00000001] = root;
    }

    miter = metadata.find(id);
    if (miter != metadata.end()) {
        metadata[id].content = buf;
        metadata[id].size = buf.length();
        metadata[id].mtime = time(NULL);
        metadata[id].ctime = time(NULL);
    } else {
        finfo f;
        f.content = buf;
        time_t t;
        time(&t);
        f.atime = t;
        f.mtime = t;
        f.ctime = t;
        f.size = buf.length();
        metadata[id] = f;
    }

    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf) {
    metatable_iterator miter = metadata.find(id);
    if (miter != metadata.end()) {
        metadata[id].atime = time(NULL);
        buf = metadata[id].content;
        return extent_protocol::OK;
    } else {
        return extent_protocol::IOERR;
    }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {

    metatable_iterator miter = metadata.find(id);
    if (miter != metadata.end()) {
        a.atime = metadata[id].atime;
        a.mtime = metadata[id].mtime;
        a.ctime = metadata[id].ctime;
        a.size = metadata[id].size;
        return extent_protocol::OK;
    } else {
        return extent_protocol::IOERR;
    }
}

int extent_server::remove(extent_protocol::extentid_t id, int &) {
    if (metadata.erase(id)) {
        return extent_protocol::OK;
    } else {
        return extent_protocol::IOERR;
    }
}

