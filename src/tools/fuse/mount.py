#!/usr/bin/env python

#    Copyright (C) 2006  Andrew Straw  <strawman@astraw.com>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import logging
import sys, os, stat, errno
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse


LOG = logging.getLogger()

global SNAPSHOT
global CACHE_DIR


if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

def download(path):
    cmd = "hindsight checkout %s:%s %s" % (SNAPSHOT, path.strip("/"), cache(""))
    LOG.debug(cmd)
    os.system(cmd)

def cache(path):
    return CACHE_DIR + path

class MyStat(fuse.Stat):
    def __init__(self, st_mode=0, st_ino=0, st_dev=0, st_nlink=0, st_uid=0, st_gid=0,
                 st_size=0, st_atime=0, st_mtime=0, st_ctime=0):
        self.st_mode = st_mode
        self.st_ino = st_ino
        self.st_dev = st_dev
        self.st_nlink = st_nlink
        self.st_uid = st_uid
        self.st_gid = st_gid
        self.st_size = st_size
        self.st_atime = st_atime
        self.st_mtime = st_mtime
        self.st_ctime = st_ctime

    @staticmethod
    def fromStat(st):
        return MyStat(st.st_mode, st.st_ino, st.st_dev, st.st_nlink, st.st_uid, st.st_gid,
                      st.st_size, st.st_atime, st.st_mtime, st.st_ctime)


class HelloFS(Fuse):

    def getattr(self, path):
        LOG.debug("getattr: " + path)
        st = MyStat()
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2
        elif os.path.exists(cache(path)):
            return MyStat.fromStat(os.stat(cache(path)))
        return -errno.ENOENT

    def readdir(self, path, offset):
        LOG.debug("readdir: " + path)
        lst = os.listdir(cache(path))
        LOG.debug(lst)
        if not lst:
            download(path)
            lst = os.listdir(cache(path))

        for r in  (['.', '..'] + lst):
            yield fuse.Direntry(r)

    def open(self, path, flags):
        LOG.debug("open: " + path)
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        LOG.debug("read: " + path)
        f = file(cache(path))
        f.seek(offset)
        return f.read(size)

def main():
    handler = logging.FileHandler("logfile")
    handler.setLevel(logging.DEBUG)
    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)

    usage="""
Userspace hello example

""" + Fuse.fusage
    server = HelloFS(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    LOG.debug("running")
    server.main()

if __name__ == '__main__':
    SNAPSHOT = sys.argv[1]
    mount = sys.argv[2]

    dir = os.path.dirname(os.getcwd() + "/" + sys.argv[0])
    CACHE_DIR = dir + "/" + mount + ".hindsight-cache"

    if not (os.path.exists(mount)):
        os.mkdir(mount)
    if not os.path.exists(CACHE_DIR):
        os.mkdir(CACHE_DIR)

    main()
