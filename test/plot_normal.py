import matplotlib.pyplot as plt
import numpy as np

# 数据
data = {
    '1000': [23932.6, 2.69341e+06, 57462.7, 892.938],
    '10000': [2441.93, 996435, 23127.7, 69.3376],
    '65536': [503.191, 247808, 3203.66, 1.8634],
    '100000': [364.35, 157331, 2923.68, 0.552885]
}

# 横坐标标签
x_labels = ['PUT Throughput', 'GET Throughput', 'DEL Throughput', 'SCAN Throughput']

# 创建图形和轴
fig, ax = plt.subplots(figsize=(10, 6))

# 颜色列表
colors = ['blue', 'green', 'red', 'purple']

# 绘制每条折线
for i, (key, values) in enumerate(data.items()):
    ax.plot(x_labels, values, marker='o', color=colors[i], label=f'Data Size={key}')

# 设置图例
ax.legend()

# 设置标题和标签
ax.set_title('Operation Throughput for Different Data Sizes (3 larger cases)', fontsize=14)
ax.set_xlabel('Operation Type', fontsize=12)
ax.set_ylabel('Throughput (ops/sec)', fontsize=12)

# 显示网格
ax.grid(True)

# 显示图形
plt.tight_layout()
plt.show()