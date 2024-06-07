
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g

all: correctness persistence speed

correctness: skiplist.o kvstore.o correctness.o

persistence: skiplist.o kvstore.o persistence.o

./tst/speed.o: ./tst/speed.cc
	g++ -c $< -o $@

speed: skiplist.o kvstore.o ./tst/speed.o
	g++ $^ -o $@

clean:
	-rm -f correctness persistence speed *.o ./tst/speed.o