import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv('memory_usage_ray1.csv')

data['Timestamp'] -= 1685622094

x = data['Timestamp']
y = data[' Memory Usage']

plt.plot(x, y, '-', linewidth=1.5)

plt.xlim(13, 24)
plt.ylim(730000000, 1000000000)
plt.xlabel('Timestamp')
plt.ylabel('Memory Usage')
plt.title('Memory Fluctuation')

plt.savefig('Specific Memory Fluctuation.png')