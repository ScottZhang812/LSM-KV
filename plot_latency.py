import matplotlib.pyplot as plt

def read_latency_file(file_path):
    latencies = []
    with open(file_path, 'r') as file:
        for line in file:
            latencies.append(float(line.strip()))
    return latencies

def plot_latency(latencies):
    plt.figure(figsize=(10, 6))
    plt.plot(list(map(lambda x: 1/x, latencies[:5000])), label='PUT throughput')
    plt.xlabel('Cumulative Operation Count', fontsize=24)
    plt.ylabel('Throughput (Mops/sec)', fontsize=24)
    plt.title('PUT Operation Throughput Over Time', fontsize=24)
    plt.xticks(range(0, 5000 + 1, 1000), fontsize=24)  # Set x-ticks every 1000 operations
    plt.yticks(fontsize=24)
    plt.legend(fontsize=24)
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    file_path = 'put_latency.txt'
    latencies = read_latency_file(file_path)
    plot_latency(latencies)
