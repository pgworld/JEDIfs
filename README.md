# Introduction
------------

Redis is a high-performance key/value store which allows storing keys, and their values, in a fast memory-backed store.

Intuitively it might seem that basing a filesystem around a simple key=value store isn't such a practical endeavour, but happily redis also has several useful abilities built-in, including native support for other data-structures including:

* sets.
* hashes.
* lists.

# Getting Started
---------------

To build the code, assuming you have the required build dependencies
installed, you should merely need to run:

     make

Once built the software can be installed via:

     make install

It is possible to run the filesystem without having installed the
software, via:

     #jedifs

# TODO
---
- Develop fs_rename
- Multithreading(mutex lock from file lock)

# time check
---
is_directory - 1 reply(exists)
fs_readdir - 2 reply(keys, hget)
fs_getattr - 3 reply(exists, hmget, hmget)
fs_mkdir - 1 reply(hset)
count_dir_ent - 1 reply(keys)
fs_rmdir - 1 reply(unlink)
fs_write - 5 reply(hset, set, hincrby, append, hset)
fs_read - 3 reply(hget, getrange, substr)
fs_symlink - 1 reply(hmset)
fs_readlink - 1 reply(hget)
fs_open - 1 reply(hset)
fs_create - 1 reply(hset)
fs_chown - 1 reply(hset)
fs_chmod - 1 reply(hset)
fs_unlink - 2 reply(unlink, unlink)
fs_utimens - 1 reply(hset)

