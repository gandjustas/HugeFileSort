import sys

path = sys.argv[1]
rows = []
with open(path, encoding='utf-8-sig') as f:
    header = f.readline()
    for line in f:
        parts = line.strip().split(',')
        if len(parts) != 6:
            continue
        t, rb, wb, idle, dtime, cpu = parts
        rows.append((t, float(rb), float(wb), float(idle), float(cpu)))

total = len(rows)
if total == 0:
    print('no data')
    sys.exit(0)

def fmt(x):
    return f'{x/1_000_000:6.1f} MB/s'

# Общая статистика
avg_read = sum(r[1] for r in rows) / total
avg_write = sum(r[2] for r in rows) / total
avg_idle = sum(r[3] for r in rows) / total
avg_cpu = sum(r[4] for r in rows) / total
print(f'samples={total} (interval 2s, ~{total*2}s)')
print(f'avg read={avg_read/1e6:.1f} MB/s  avg write={avg_write/1e6:.1f} MB/s  avg disk-idle={avg_idle:.0f}%  avg cpu={avg_cpu:.0f}%')

# Периоды простоя: и CPU <= 10%, и диск простаивает >= 90%
idle_streak = 0
start = None
worst = 0
gaps = []
for i, r in enumerate(rows):
    if r[3] >= 90 and r[4] <= 10:
        if start is None:
            start = r[0]
        idle_streak += 1
    else:
        if idle_streak >= 3:  # >= 6 секунд полного простоя
            gaps.append((start, rows[i-1][0], idle_streak*2))
            worst = max(worst, idle_streak*2)
        idle_streak = 0
        start = None
if idle_streak >= 3:
    gaps.append((start, rows[-1][0], idle_streak*2))
    worst = max(worst, idle_streak*2)

print(f'idle gaps (>=6s, CPU<=10% AND disk-idle>=90%): {len(gaps)}, longest={worst}s')
for g in gaps[:10]:
    print(f'  {g[0]} .. {g[1]} ({g[2]}s)')

# Минимумы/максимумы по фазам — покажем краткую сводку по каждым 25% времени
n = total // 4
for q in range(4):
    seg = rows[q*n:(q+1)*n if q < 3 else total]
    br = sum(r[1] for r in seg)/len(seg)
    bw = sum(r[2] for r in seg)/len(seg)
    bi = sum(r[3] for r in seg)/len(seg)
    bc = sum(r[4] for r in seg)/len(seg)
    print(f'  quarter {q+1}: read={br/1e6:6.1f} write={bw/1e6:6.1f} MB/s idle={bi:4.0f}% cpu={bc:4.0f}%')
