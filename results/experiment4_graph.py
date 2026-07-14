import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': ':',
})

df = pd.read_csv('experiment4_determinant_size_1784024742_577253.csv')

# logging_avg is already in ms (stored as ms in csv)
s = df.groupby('actual_size_bytes')['logging_avg'].agg(['mean','std']).reset_index()
s['size_kb'] = s['actual_size_bytes'] / 1024

print(s[['size_kb','mean','std']].round(3).to_string())

fig, ax = plt.subplots(figsize=(3.5, 2.8))

ax.errorbar(s['size_kb'], s['mean'], yerr=s['std'],
            fmt='o-', color='#c0392b', linewidth=1.5, markersize=4,
            capsize=3, capthick=1.2, label='MongoDB write latency')

ax.set_xlabel('Determinant size (KB)', fontsize=9)
ax.set_ylabel('Write latency (ms)', fontsize=9)
ax.set_title('RQ2 — Write Latency vs. Determinant Size', fontsize=10)
ax.set_xscale('log')
ax.tick_params(labelsize=8)
ax.legend(fontsize=8)

plt.tight_layout()
plt.savefig('experiment4_determinant_size.pdf', dpi=1200, bbox_inches='tight')
plt.savefig('experiment4_determinant_size.png', dpi=1200, bbox_inches='tight')
plt.close()
print("Done.")