// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst) {
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

extent_protocol::status extent_client::load_extent(extent_protocol::extentid_t eid) {

    extent_protocol::status ret = extent_protocol::OK;
    extent_protocol::attr attr;

    ret = cl->call(extent_protocol::getattr, eid, attr);
    if (ret == extent_protocol::OK) {
        extent_info *ex = new extent_info();
        ex->ext_attr.size = attr.size;
        ex->ext_attr.atime = attr.atime;
        ex->ext_attr.mtime = attr.mtime;
        ex->ext_attr.ctime = attr.ctime;
        ex->is_dirty = false;
        ret = cl->call(extent_protocol::get, eid, ex->data);
        if (ret != extent_protocol::OK) {
            delete(ex);
            return ret;
        }
        local_extent_cache[eid] = ex;
    }
    return ret;
}

extent_protocol::status
extent_client::flush_extent(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    int r;
    /*lock table*/
    pthread_mutex_lock(&cache_lock);
    extent_info *ex;
    if (local_extent_cache.count(eid) > 0) {
        ex = local_extent_cache[eid];
        if (ex->is_dirty) {
            ret = cl->call(extent_protocol::put, eid, ex->data, r);
        }
        delete(local_extent_cache[eid]);
        local_extent_cache.erase(eid);
    } else {
        ret = cl->call(extent_protocol::remove, eid, r);
    }
    /*release extent table*/
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    int r = 0;
    extent_protocol::status ret = extent_protocol::OK;
    pthread_mutex_lock(&cache_lock);
    if (local_extent_cache.count(eid) == 0) {
        ret = load_extent(eid);
        if (ret != extent_protocol::OK) {
            pthread_mutex_unlock(&cache_lock);
            return ret;
        }
    }
    buf = local_extent_cache[eid]->data;
    local_extent_cache[eid]->ext_attr.atime = time(NULL);
    /*release extent table*/
    pthread_mutex_unlock(&cache_lock);
    return extent_protocol::OK;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
        extent_protocol::attr &attr) {
    int r = 0;
    extent_protocol::status ret = extent_protocol::OK;
    pthread_mutex_lock(&cache_lock);
    if (local_extent_cache.count(eid) == 0) {
        ret = load_extent(eid);
        if (ret != extent_protocol::OK) {
            pthread_mutex_unlock(&cache_lock);
            return ret;
        }
    }
    attr = local_extent_cache[eid]->ext_attr;
    /*release extent table*/
    pthread_mutex_unlock(&cache_lock);
    return extent_protocol::OK;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    int r = 0;
    extent_protocol::status ret = extent_protocol::OK;
    pthread_mutex_lock(&cache_lock);
    extent_info *ex = new extent_info();
    if (local_extent_cache.count(eid) == 0) {
        ex->data = buf;
        ex->is_dirty = true;
        ex->ext_attr.size = buf.size();
        time_t t = time(NULL);
        ex->ext_attr.atime = t;
        ex->ext_attr.ctime = t;
        ex->ext_attr.mtime = t;
        local_extent_cache[eid] = ex;
    } else {
        local_extent_cache[eid]->data = buf;
        local_extent_cache[eid]->is_dirty = true;
        local_extent_cache[eid]->ext_attr.size = buf.size();
        local_extent_cache[eid]->ext_attr.mtime = time(NULL);
        local_extent_cache[eid]->ext_attr.ctime = time(NULL);
    }
    /*release extent table*/
    pthread_mutex_unlock(&cache_lock);
    return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    pthread_mutex_lock(&cache_lock);
    if (local_extent_cache.count(eid) == 0) {
        ret = extent_protocol::IOERR;
    } else {
        local_extent_cache.erase(eid);
    }
    /*release extent table*/
    pthread_mutex_unlock(&cache_lock);
    return ret;
}


