import csv
import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

perf_path = 'benchmarks/monitoring/10gb-rewrite/perf100_v3.csv'
counters_path = 'benchmarks/monitoring/10gb-rewrite/counters100_v3.csv'
out = 'benchmarks/monitoring/10gb-rewrite/chart100_sort.png'

def parse_ts(ts):
    return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

# Perf-сэмплы: только сам sort (split 23:15 + merge 29:08 = 3143с), без check/multiset.
rows = []
with open(perf_path, encoding='utf-8-sig') as f:
    f.readline()
    for line in f:
        p = line.strip().split(',')
        if len(p) == 6:
            rows.append(p)

t0 = parse_ts(rows[0][0])
sort_end = 3143  # 52:23
x, read, write, dtime, idle, cpu = [], [], [], [], [], []
for r in rows:
    t = (parse_ts(r[0]) - t0).total_seconds()
    if t > sort_end:
        break
    x.append(t)
    read.append(float(r[1]) / 1e6)
    write.append(float(r[2]) / 1e6)
    dtime.append(float(r[4]))
    idle.append(float(r[3]))
    cpu.append(float(r[5]))

# Working set процесса из counters (сэмпл ~2с, формат длинный).
wx, ws = [], []
with open(counters_path, encoding='utf-8-sig') as f:
    for row in csv.reader(f):
        if len(row) >= 5 and 'dotnet.process.memory.working_set' in row[2]:
            try:
                t = (datetime.datetime.strptime(row[0], '%m/%d/%Y %H:%M:%S') - t0).total_seconds()
            except ValueError:
                continue
            if t > sort_end:
                break
            wx.append(t)
            ws.append(float(row[4]) / 1e6)

fig, axes = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
fig.suptitle('100GB sort: split 23:15 + merge 29:08 = 52:23  (F: HDD, 206 chunks)', fontsize=13)

ax = axes[0]
ax.plot(x, read, label='disk read, MB/s', color='tab:blue', lw=0.9)
ax.plot(x, write, label='disk write, MB/s', color='tab:red', lw=0.9)
ax.axvline(1395, color='k', ls='--', lw=0.8, alpha=0.6)
ax.text(700, 205, 'split', ha='center')
ax.text(2300, 205, 'merge', ha='center')
ax.set_ylabel('MB/s')
ax.set_ylim(0, 230)
ax.legend(loc='upper right', ncol=2)
ax.grid(alpha=0.3)

ax = axes[1]
ax.plot(x, dtime, label='disk time, %', color='tab:purple', lw=0.9)
ax.plot(x, idle, label='disk idle, %', color='tab:gray', lw=0.9)
ax.axvline(1395, color='k', ls='--', lw=0.8, alpha=0.6)
ax.set_ylabel('%')
ax.set_ylim(0, 420)
ax.legend(loc='upper right', ncol=2)
ax.grid(alpha=0.3)

ax = axes[2]
ax.plot(x, cpu, label='process cpu, %', color='tab:green', lw=0.9)
ax.axvline(1395, color='k', ls='--', lw=0.8, alpha=0.6)
ax.set_ylabel('cpu, %')
ax.set_ylim(0, 105)
ax.legend(loc='upper right')
ax.grid(alpha=0.3)

ax = axes[3]
ax.plot(wx, ws, label='working set, MB', color='tab:orange', lw=1.2)
ax.axvline(1395, color='k', ls='--', lw=0.8, alpha=0.6)
ax.set_ylabel('MB')
ax.set_xlabel('seconds')
ax.legend(loc='upper right')
ax.grid(alpha=0.3)

fig.tight_layout(rect=[0, 0, 1, 0.96])
fig.savefig(out, dpi=110)
print('saved', out, f'(samples: {len(x)} perf, {len(wx)} working-set, cut at {sort_end}s)')
