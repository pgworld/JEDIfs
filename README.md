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

     #redisfs

# TODO
---
- develop fs_rename
- revise fs_rmdir
