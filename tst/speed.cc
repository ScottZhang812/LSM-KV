#include <assert.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "../test.h"

class SpeedTest : public Test {
   private:
    const uint64_t TEST_MAX = 10000;
    const uint64_t TEST_TINY = 1024;

    void prepare_data(uint64_t max) {
        for (uint64_t i = 0; i < max; ++i) {
            store.put(i, std::string(i + 1, 's'));
        }
    }

    void measure_put(uint64_t max) {
        prepare_data(max);

        auto start = std::chrono::high_resolution_clock::now();
        for (uint64_t i = max; i < 2 * max; ++i) {
            store.put(max + i, std::string(i + 1, 's'));
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = max / duration.count();
        double avg_latency = duration.count() / max;

        std::cout << "PUT: Throughput = " << throughput << " ops/sec, "
                  << "Average Latency = " << avg_latency << " sec/op"
                  << std::endl;
    }

    void measure_get(uint64_t max) {
        prepare_data(max);

        auto start = std::chrono::high_resolution_clock::now();
        for (uint64_t i = max; i < 2 * max; ++i) {
            store.get(i);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = max / duration.count();
        double avg_latency = duration.count() / max;

        std::cout << "GET: Throughput = " << throughput << " ops/sec, "
                  << "Average Latency = " << avg_latency << " sec/op"
                  << std::endl;
    }

    void measure_del(uint64_t max) {
        prepare_data(max);

        auto start = std::chrono::high_resolution_clock::now();
        for (uint64_t i = 0; i < max; ++i) {
            if (i % 2 == 0) {
                store.del(i);  // Should return true
            } else {
                store.del(max + i);  // Should return false
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = max / duration.count();
        double avg_latency = duration.count() / max;

        std::cout << "DEL: Throughput = " << throughput << " ops/sec, "
                  << "Average Latency = " << avg_latency << " sec/op"
                  << std::endl;
    }

    void measure_scan(uint64_t max) {
        prepare_data(max);

        auto start = std::chrono::high_resolution_clock::now();
        int cnt = 0;
        for (uint64_t i = 0; i < max; i += (max / 10)) {
            std::list<std::pair<uint64_t, std::string>> result;
            store.scan(i, i + (max / 10) - 1, result);
            cnt++;
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput = cnt / duration.count();
        double avg_latency = duration.count() / cnt;

        std::cout << "SCAN: Throughput = " << throughput << " ops/sec, "
                  << "Average Latency = " << avg_latency << " sec/op"
                  << std::endl;
    }

    void measure_get_no_cache(uint64_t max) {
        uint64_t interval = 100;
        // Disable all caching
        store.disableCache();
        prepare_data(max);
        auto start = std::chrono::high_resolution_clock::now();
        int cnt = 0;
        for (uint64_t i = max / 2; i < max + max / 2; ++i) {
            if (i % interval == 0) store.get(i), cnt++;
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double avg_latency = duration.count() / cnt;

        std::cout << "GET (No Cache): Average Latency = " << avg_latency
                  << " sec/op" << std::endl;
    }

    void measure_get_index_cache(uint64_t max) {
        // Enable only index caching
        store.enableIndexCache();
        prepare_data(max);
        auto start = std::chrono::high_resolution_clock::now();
        for (uint64_t i = max; i < 2 * max; ++i) {
            store.get(i);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double avg_latency = duration.count() / max;

        std::cout << "GET (Index Cache): Average Latency = " << avg_latency
                  << " sec/op" << std::endl;
    }

    void measure_get_bloom_filter_cache(uint64_t max) {
        // Enable both index and Bloom Filter caching
        store.enableBloomFilterCache();
        prepare_data(max);
        auto start = std::chrono::high_resolution_clock::now();
        for (uint64_t i = max; i < 2 * max; ++i) {
            store.get(i);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double avg_latency = duration.count() / max;

        std::cout << "GET (Bloom Filter + Index Cache): Average Latency = "
                  << avg_latency << " sec/op" << std::endl;
    }

    void measure_put_with_compaction(uint64_t max) {
        prepare_data(max);

        std::ofstream latency_file("put_latency.txt");
        std::ofstream throughput_file("put_throughput.txt");
        if (!latency_file.is_open() || !throughput_file.is_open()) {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }

        uint64_t put_count = 0;
        auto start_time = std::chrono::high_resolution_clock::now();
        auto last_time = start_time;

        for (uint64_t i = 0; i < max; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            store.put(max + i, std::string(i + 1, 's'));
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::micro> latency = end - start;
            latency_file << latency.count() << std::endl;

            put_count++;
            auto current_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_time =
                current_time - last_time;

            if (elapsed_time.count() >= 1.0) {
                throughput_file << put_count << std::endl;
                put_count = 0;
                last_time = current_time;
            }
        }

        // Write the remaining count if the loop ends before a full second
        throughput_file << put_count << std::endl;

        latency_file.close();
        throughput_file.close();
    }

   public:
    SpeedTest(const std::string &dir, const std::string &vlog, bool v = true)
        : Test(dir, vlog, v) {}

    void start_test(void *args = NULL) override {
        std::cout << "KVStore Speed Test (1)" << std::endl;

        store.reset();

        std::cout << "[PUT Test]" << std::endl;
        measure_put(TEST_MAX);

        store.reset();

        std::cout << "[GET Test]" << std::endl;
        measure_get(TEST_MAX);

        // store.reset();

        // std::cout << "[DEL Test]" << std::endl;
        // measure_del(TEST_MAX);

        // store.reset();

        // std::cout << "[SCAN Test]" << std::endl;
        // measure_scan(TEST_MAX);
        // std::cout << "KVStore Speed Test (2)" << std::endl;

        // store.reset();

        // std::cout << "[GET Test - No Cache]" << std::endl;
        // measure_get_no_cache(TEST_MAX);

        // store.reset();

        // std::cout << "[GET Test - Index Cache]" << std::endl;
        // measure_get_index_cache(TEST_MAX);

        // store.reset();

        // std::cout << "[GET Test - Bloom Filter + Index Cache]" << std::endl;
        // measure_get_bloom_filter_cache(TEST_MAX);

        // std::cout << "KVStore Speed Test (3)" << std::endl;

        // store.reset();

        // std::cout << "[PUT Test with Compaction]" << std::endl;
        // measure_put_with_compaction(TEST_MAX);
    }
};

int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    SpeedTest test("./data", "./data/vlog", verbose);

    test.start_test();

    return 0;
}
