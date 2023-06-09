import numpy as np
import matplotlib.pyplot as plt

# Dashboard measurements
# import numpy as np
# import matplotlib.pyplot as plt

# six_peak_ram = {'Head': 413.36, 'Sink': 470.71, 'Owner': 33.62, 'Worker': 2509.91}
# ten_peak_ram = {'Head': 2367.38, 'Sink': 686.29, 'Owner': 112.73, 'Worker': 3487.58}

# six_nodes = list(six_peak_ram.keys())
# six_val = list(six_peak_ram.values())

# ten_nodes = list(ten_peak_ram.keys())
# ten_val = list(ten_peak_ram.values())

# bar_width = 0.35
# offset = np.arange(len(six_nodes))

# fig, ax = plt.subplots(figsize=(10, 5))

# ax.barh(offset, six_val, bar_width, color='blue', label='Six Videos')
# ax.barh(offset + bar_width, ten_val, bar_width, color='green', label='Ten Videos')

# ax.set_yticks(offset + bar_width / 2)
# ax.set_yticklabels(six_nodes)

# ax.axvline(x=953.67, color='red', linestyle='--')

# ax.set_xlabel("Peak Object Store Memory Usage (MB)")
# ax.set_ylabel("Nodes")
# ax.set_title("Peak Object Store Memory Usage for Video Processing Tasks")
# ax.legend()

# plt.savefig('test.png')


# TO PLOT JUST SIX VIDEOS
import numpy as np
import matplotlib.pyplot as plt

six_peak_ram = {'Head': 470.764858, 'Sink': 508.094230, 'Owner': 39.403226, 'Worker': 2958.597628}

six_nodes = list(six_peak_ram.keys())
six_val = list(six_peak_ram.values())

bar_width = 0.35
offset = np.arange(len(six_nodes))

fig, ax = plt.subplots(figsize=(10, 5))

ax.barh(offset, six_val, bar_width, color='blue', label='Six Videos')

ax.set_yticks(offset)
ax.set_yticklabels(six_nodes)
ax.set_ylim([-bar_width/2, len(six_nodes)-bar_width/2])
ax.set_ylim([-0.5, len(six_nodes)-0.5])
ax.tick_params(axis='y', pad=10)

ax.axvline(x=1000, color='red', linestyle='--')

ax.set_xlabel("Peak Object Store Memory Usage (MB)")
ax.set_ylabel("Nodes")
ax.set_title("Peak Object Store Memory Usage for Video Processing Tasks")
ax.legend()

plt.savefig('test.png')