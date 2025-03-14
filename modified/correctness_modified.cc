#include <assert.h>

#include <cstdint>
#include <iostream>
#include <string>

#include "test.h"
// #define DET_TEST
// #define SIMPLE_CASES

class CorrectnessTest : public Test {
   private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1024 * 64;
    const uint64_t GC_TEST_MAX = 1024 * 48;
    // const uint64_t GC_TEST_MAX = 1024 * 5;

    void regular_test(uint64_t max) {
        uint64_t i;

        // Test a single key
        EXPECT(not_found, store.get(1));
        store.put(1, "SE");
        EXPECT("SE", store.get(1));
        EXPECT(true, store.del(1));
        EXPECT(not_found, store.get(1));
        EXPECT(false, store.del(1));

        phase();

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i) {
            store.put(i, std::string(i + 1, 's'));
            EXPECT(std::string(i + 1, 's'), store.get(i));
        }
        phase();

        // Test after all insertions
        for (i = 0; i < max; ++i) EXPECT(std::string(i + 1, 's'), store.get(i));
        phase();

// // Test scan
// std::list<std::pair<uint64_t, std::string>> list_ans;
// std::list<std::pair<uint64_t, std::string>> list_stu;

// for (i = 0; i < max / 2; ++i) {
//     list_ans.emplace_back(std::make_pair(i, std::string(i + 1,
//     's')));
// }

// store.scan(0, max / 2 - 1, list_stu);
// EXPECT(list_ans.size(), list_stu.size());

// auto ap = list_ans.begin();
// auto sp = list_stu.begin();
// while (ap != list_ans.end()) {
//     if (sp == list_stu.end()) {
//         EXPECT((*ap).first, -1);
//         EXPECT((*ap).second, not_found);
//         ap++;
//     } else {
//         EXPECT((*ap).first, (*sp).first);
//         EXPECT((*ap).second, (*sp).second);
//         ap++;
//         sp++;
//     }
// }

// phase();

// Test deletions
#ifdef DET_TEST
        std::cout << "TEST: Deleting even\n";
#endif
        for (i = 0; i < max; i += 2) {
#ifdef DET_TEST
            // std::cout << "TEST: Deleting " << i << " ";
#endif
            EXPECTI(true, store.del(i), i);
        }
#ifdef DET_TEST
        std::cout << "TEST: Getting all\n";
#endif
        // for (i = 0; i < 1000; ++i)
        //     EXPECTI((i & 1) ? std::string(i + 1, 's') : not_found,
        //     store.get(i),
        //             i);
        for (i = 0; i < max; ++i)
            EXPECTI((i & 1) ? std::string(i + 1, 's') : not_found, store.get(i),
                    i);

            // store.printUidContainsWatchedKey();

#ifdef DET_TEST
        std::cout << "TEST: Deleting all\n";
#endif
        // for (i = 1; i < 5000; ++i) {
        //     EXPECTI(i & 1, store.del(i), i);
        // }
        for (i = 1; i < max; ++i) {
            if (i == 65485) store.printSST(7, 3371);
            EXPECTI(i & 1, store.del(i), i);
        }

        phase();

        report();
    }

    void gc_test(uint64_t max) {
        uint64_t i;
        uint64_t gc_trigger = 1024;
#ifdef DET_TEST
        std::cout << "TEST: Putting all\n";
#endif
        for (i = 0; i < max; ++i) {
            store.put(i, std::string(i + 1, 's'));
        }

#ifdef DET_TEST
        std::cout << "TEST: Gettting and Updating differently\n";
#endif
        for (i = 0; i < max; ++i) {
            EXPECTI(std::string(i + 1, 's'), store.get(i), i);
            switch (i % 3) {
                case 0:
                    store.put(i, std::string(i + 1, 'e'));
                    break;
                case 1:
                    store.put(i, std::string(i + 1, '2'));
                    break;
                case 2:
                    store.put(i, std::string(i + 1, '3'));
                    break;
                default:
                    assert(0);
            }

            if (i % gc_trigger == 0) [[unlikely]] {
#ifdef DET_TEST
                // std::cout << "curHead: " << store.getHead() << "\n";
                // std::cout << "curTail: " << store.getTail() << "\n";
                // std::cout << "TEST: Checking new round of GC\n ";
#endif
                check_gc(16 * MB);
                // check_gc(16 * 1024);
            }
            fflush(stdout);
            fflush(stderr);
        }

        phase();
#ifdef DET_TEST
        std::cout << "TEST: Gettting differently\n";
#endif

        for (i = 0; i < max; ++i) {
            switch (i % 3) {
                case 0:
                    EXPECT(std::string(i + 1, 'e'), store.get(i));
                    break;
                case 1:
                    EXPECT(std::string(i + 1, '2'), store.get(i));
                    break;
                case 2:
                    EXPECT(std::string(i + 1, '3'), store.get(i));
                    break;
                default:
                    assert(0);
            }
        }

        phase();

#ifdef DET_TEST
        std::cout << "TEST: Del/Get/Del/Get differently\n";
#endif
        for (i = 1; i < max; i += 2) {
            EXPECT(true, store.del(i));

            if ((i - 1) % gc_trigger == 0) [[unlikely]] {
                check_gc(8 * MB);
                // check_gc(8 * 1024);
            }
        }

        for (i = 0; i < max; i += 2) {
            if (i == 4590) {
                store.lookInMemtable(i);
                store.printUidContainsWatchedKey();
                // SS_OFFSET_TL testOffset;
                // store.get(i, &testOffset);
                // std::cout << "test_get_offset: " << testOffset << "\n";
            }
            switch (i % 3) {
                case 0:
                    EXPECTI(std::string(i + 1, 'e'), store.get(i), i);
                    break;
                case 1:
                    EXPECTI(std::string(i + 1, '2'), store.get(i), i);
                    break;
                case 2:
                    EXPECTI(std::string(i + 1, '3'), store.get(i), i);
                    break;
                default:
                    assert(0);
            }

            store.del(i);

            if (((i - 1) / 2) % gc_trigger == 0) [[unlikely]] {
                check_gc(32 * MB);
                // check_gc(32 * 1024);
            }
            fflush(stdout);
            fflush(stderr);
        }

        for (i = 0; i < max; ++i) {
            EXPECT(not_found, store.get(i));
        }

        phase();

        report();
    }

   public:
    CorrectnessTest(const std::string &dir, const std::string &vlog,
                    bool v = true)
        : Test(dir, vlog, v) {}

    void start_test(void *args = NULL) override {
        std::cout << "KVStore Correctness Test" << std::endl;

        // store.reset();

        // std::cout << "[Simple Test]" << std::endl;
        // regular_test(SIMPLE_TEST_MAX);

        // store.reset();

        // std::cout << "[Large Test]" << std::endl;
        // regular_test(LARGE_TEST_MAX);

        store.reset();

        std::cout << "[GC Test]" << std::endl;
        gc_test(GC_TEST_MAX);
    }
};

int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", "./data/vlog", verbose);

    test.start_test();

    return 0;
}
