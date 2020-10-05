
#
#  Flags and stuffs.
#
CFLAGS=-Wall -O3  `pkg-config fuse --cflags` -I. -DVERSION=$(VERSION) -D_DEFAULT_SOURCE -g
LDFLAGS=`pkg-config fuse --libs`
LDLIBS := $(shell pkg-config fuse --libs) libhiredis.a

#
#  Version is only set if building from the top-level directory
#
ifndef VERSION
VERSION=\"unknown\"
endif

#
#  By default make our filesystem.
#
all: link jedifs jedifs-snapshot


#
#  Clean.
#
clean:
	rm -f jedifs jedifs-snapshot *.o || true
	rm -f fmacros.h || true
	rm -f hiredis.c || true
	rm -f hiredis.h || true
	rm -f sds.c || true
	rm -f sds.h  || true
	rm -f net.c || true
	rm -f net.h  || true
	rm -f read.h || true
	rm -f read.c || true
	rm -f alloc.h || true
	rm -f alloc.c || true
	rm -f async.h || true
	rm -f async.c || true
	rm -f win32.h || true
	rm -f sockcompat.h || true
	rm -f sdsalloc.h || true
	rm -f sockcompat.c || true
	rm -f dict.c || true
	rm -f dict.h || true
	rm -f async_private.h || true
	rm -f jedifs-snapshot || true
	rm -f jedifs-snapshot.o || true


#
#  Install the binary
#
install:
	redis-server --daemonize yes
	mkdir -p $(prefix)/sbin
	cp ./jedifs $(prefix)/sbin/

#
#  Indent & tidy.
#
tidy:
	indent --no-space-after-function-call-names --no-space-after-casts --braces-after-if-line --no-tabs --indent-level 4 -bli0 -cdb -sc jedifs.c
	indent --no-space-after-function-call-names --no-space-after-casts --braces-after-if-line --no-tabs --indent-level 4 -bli0 -cdb -sc jedifs-snapshot.c


#
#  The filesystem
#
#jedifs: pathutil.o jedifs.o hiredis.o sds.o net.o read.o alloc.o async.o sockcompat.o dict.o
jedifs: jedifs.o


#
#  The filesystem snapshot utility
#
#jedifs-snapshot: pathutil.o jedifs-snapshot.o hiredis.o sds.o net.o read.o alloc.o async.o sockcompat.o dict.o
jedifs-snapshot: jedifs-snapshot.o


#
#  Link our C-client library into place
#
link:
	ln -sf ../hiredis/fmacros.h .
	ln -sf ../hiredis/hiredis.c .
	ln -sf ../hiredis/hiredis.h .
	ln -sf ../hiredis/sds.c .
	ln -sf ../hiredis/sds.h .
	ln -sf ../hiredis/net.c .
	ln -sf ../hiredis/net.h .
	ln -sf ../hiredis/read.h .
	ln -sf ../hiredis/read.c .
	ln -sf ../hiredis/alloc.h .
	ln -sf ../hiredis/alloc.c .
	ln -sf ../hiredis/async.h .
	ln -sf ../hiredis/async.c .
	ln -sf ../hiredis/win32.h .
	ln -sf ../hiredis/sockcompat.h .
	ln -sf ../hiredis/sdsalloc.h .
	ln -sf ../hiredis/sockcompat.c .
	ln -sf ../hiredis/dict.c .
	ln -sf ../hiredis/dict.h .
	ln -sf ../hiredis/async_private.h .






