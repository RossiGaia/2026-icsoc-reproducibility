import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 20,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': ':',
})

df3 = pd.read_csv('experiment3_odte_recovery_1782286333_8976767.csv')

s3 = df3.groupby('n_events').agg(
    rebuild_mean=('rebuild_time_s','mean'),
    rebuild_std=('rebuild_time_s','std'),
    odte_mean=('odte_recovery_time_s','mean'),
    odte_std=('odte_recovery_time_s','std'),
).reset_index()

s3['total_mean'] = s3['rebuild_mean'] + s3['odte_mean']
s3['total_std'] = np.sqrt(s3['rebuild_std']**2 + s3['odte_std']**2)

ns = s3['n_events'].values

fig, ax = plt.subplots(figsize=(3.5, 2.8))

ax.fill_between(ns, 0, s3['rebuild_mean'], alpha=0.25, color='#2c6fad')
ax.fill_between(ns, s3['rebuild_mean'], s3['total_mean'], alpha=0.25, color='#e67e22')

ax.plot(ns, s3['rebuild_mean'], 'o-', color='#2c6fad',
        linewidth=1.5, markersize=4, label='State reproduction time')
ax.plot(ns, s3['total_mean'], 's-', color='#e67e22',
        linewidth=1.5, markersize=4, label='Total time')

ax.errorbar(ns, s3['total_mean'], yerr=s3['total_std'],
            fmt='none', color='#e67e22', capsize=3, capthick=1.2)

n_ann = 1000
idx = s3[s3['n_events'] == n_ann].index[0]
y_bot = s3['rebuild_mean'].iloc[idx]
y_top = s3['total_mean'].iloc[idx]
odte_avg = s3['odte_mean'].mean()
ax.annotate('', xy=(n_ann, y_top), xytext=(n_ann, y_bot),
            arrowprops=dict(arrowstyle='<->', color='#555', lw=1.2))
ax.text(n_ann * 1.15, (y_bot + y_top) / 2,
        f'~{odte_avg:.1f}s\n(constant)',
        fontsize=7, color='#555', va='center')

ax.set_xlabel('Number of stored determinants (N)', fontsize=9)
ax.set_ylabel('Time (s)', fontsize=9)
ax.set_title('RQ1, RQ3 — Time to Fully Operational DT', fontsize=10)
ax.set_xscale('log')
ax.set_xticks(ns)
ax.set_xticklabels([str(n) for n in ns], rotation=30, fontsize=8)
ax.tick_params(axis='y', labelsize=8)
ax.legend(fontsize=8, loc='upper left')
ax.set_ylim(0, None)

plt.tight_layout()
plt.savefig('experiment3_combined.pdf', dpi=1200, bbox_inches='tight')
plt.savefig('experiment3_combined.png', dpi=1200, bbox_inches='tight')
plt.close()
print("Done.")