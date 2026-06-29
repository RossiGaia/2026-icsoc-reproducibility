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

df = pd.read_csv('experiment2_logging_overhead1782203981_451135.csv')

log_df  = df[df['overhead_type'] == 'logging']
proc_df = df[df['overhead_type'] == 'processing']

s_log  = log_df.groupby('updates_per_sec')['avg_write_ms'].agg(['mean','std']).reset_index()
s_proc = proc_df.groupby('updates_per_sec')['avg_write_ms'].agg(['mean','std']).reset_index()

rates = s_log['updates_per_sec'].values

fig, ax = plt.subplots(figsize=(3.5, 2.8))

ax.errorbar(rates, s_log['mean'], yerr=s_log['std'],
            fmt='s-', color='#c0392b', linewidth=1.5, markersize=4,
            capsize=3, capthick=1.2, label='MongoDB write')
ax.errorbar(rates, s_proc['mean'], yerr=s_proc['std'],
            fmt='o-', color='#2c6fad', linewidth=1.5, markersize=4,
            capsize=3, capthick=1.2, label='Transition function')

ax.set_xlabel('PT update rate (events/s)', fontsize=9)
ax.set_ylabel('Latency per event (ms)', fontsize=9)
ax.set_title('RQ2 — Per-Event Latency', fontsize=10)
ax.set_xscale('log')
ax.set_xticks(rates)
ax.set_xticklabels([str(r) for r in rates], rotation=30, fontsize=8)
ax.tick_params(axis='y', labelsize=8)
ax.legend(fontsize=8)
ax.set_ylim(0, 4.2)
plt.tight_layout()
plt.savefig('experiment2_logging_overhead.pdf', dpi=600, bbox_inches='tight')
plt.savefig('experiment2_logging_overhead.png', dpi=600, bbox_inches='tight')
plt.close()
print("Done.")