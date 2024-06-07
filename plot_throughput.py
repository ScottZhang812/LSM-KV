import matplotlib.pyplot as plt

def read_throughput_file(file_path):
    latencies = []
    with open(file_path, 'r') as file:
        for line in file:
            latencies.append(float(line.strip()))
    return latencies

def plot_throughput(latencies):
    plt.figure(figsize=(10, 6))
    plt.plot(latencies[:10000], label='PUT throughput')
    plt.xlabel('Cumulative Operation Count', fontsize=24)
    plt.ylabel('throughput (Mops/sec)', fontsize=24)
    plt.title('PUT Operation throughput Over Time', fontsize=24)
    plt.xticks(range(0, 10000 + 1, 1000), fontsize=24)  # Set x-ticks every 1000 operations
    plt.yticks(fontsize=24)
    plt.legend(fontsize=24)
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    file_path = 'put_throughput.txt'
    latencies = read_throughput_file(file_path)
    plot_throughput(latencies)
