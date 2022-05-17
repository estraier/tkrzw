# Makefile for sample programs of Tkrzw

#================================================================
# Setting Variables
#================================================================

# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = helloworld accesscounter transfer ulogrestore \
  hashdbm_ex1 hashdbm_ex2 hashdbm_ex3 \
  treedbm_ex1 treedbm_ex2 treedbm_ex3 \
  skipdbm_ex1 skipdbm_ex2 skipdbm_ex3 \
  tinydbm_ex1 tinydbm_ex2 \
  babydbm_ex1 babydbm_ex2 \
  cachedbm_ex1 \
  stdtreedbm_ex1 stdtreedbm_ex2 \
  polydbm_ex1 sharddbm_ex1 asyncdbm_ex1 \
  fileindex_ex1 memindex_ex1 stdindex_ex1 \
  invindex_ex1 \
  langc_ex1 langc_ex2

# Building binaries
CC = gcc
CXX = g++
CPPFLAGS = -I. -I.. -I/usr/local/include -D_GNU_SOURCE=1
CFLAGS = -std=c11 -Wall -pedantic -fsigned-char -O2
CXXFLAGS = -std=c++17 -Wall -fsigned-char -O2
LDFLAGS =
LIBS = -L. -L.. -L/usr/local/lib -ltkrzw -lstdc++ -lpthread
LDENV = LD_RUN_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..
RUNENV = LD_LIBRARY_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..

#================================================================
# Suffix rules
#================================================================

.SUFFIXES :
.SUFFIXES : .c .cc .o

.c.o :
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $<

.cc.o :
	$(CXX) -c $(CPPFLAGS) $(CXXFLAGS) $<

#================================================================
# Actions
#================================================================

all : $(MYBINS)

clean :
	rm -rf $(MYBINS) *.exe *.o a.out check.out gmon.out leak.log casket* *.tkh *.tkt *.tks *~

static :
	make LDFLAGS="$(LDFLAGS) -static"

single :
	g++ -std=c++17 -I/usr/local/include -Wall -fsigned-char -O2 \
	  -o helloworld helloworld.cc -L/usr/local/lib -ltkrzw -lstdc++ -lpthread

.PHONY : all clean static single

#================================================================
# Building binaries
#================================================================

helloworld : helloworld.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

accesscounter : accesscounter.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

transfer : transfer.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

ulogrestore : ulogrestore.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

hashdbm_ex1 : hashdbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

hashdbm_ex2 : hashdbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

hashdbm_ex3 : hashdbm_ex3.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

treedbm_ex1 : treedbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

treedbm_ex2 : treedbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

treedbm_ex3 : treedbm_ex3.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

skipdbm_ex1 : skipdbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

skipdbm_ex2 : skipdbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

skipdbm_ex3 : skipdbm_ex3.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

tinydbm_ex1 : tinydbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

tinydbm_ex2 : tinydbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

babydbm_ex1 : babydbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

babydbm_ex2 : babydbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

cachedbm_ex1 : cachedbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

stdtreedbm_ex1 : stdtreedbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

stdtreedbm_ex2 : stdtreedbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

polydbm_ex1 : polydbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

sharddbm_ex1 : sharddbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

asyncdbm_ex1 : asyncdbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

fileindex_ex1 : fileindex_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

memindex_ex1 : memindex_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

stdindex_ex1 : stdindex_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

invindex_ex1 : invindex_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

langc_ex1 : langc_ex1.o
	$(LDENV) $(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

langc_ex2 : langc_ex2.o
	$(LDENV) $(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

# END OF FILE
