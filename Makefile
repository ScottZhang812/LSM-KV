
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g

all: correctness persistence

correctness: skiplist.o bloom.o kvstore.o correctness.o

persistence: skiplist.o bloom.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o
