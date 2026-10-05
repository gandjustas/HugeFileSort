import csv
import sys

path = sys.argv[1]
with open(path, encoding='utf-8-sig') as f:
    rows = list(csv.reader(f))[1:]

def is_num(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def total(substr):
    return sum(float(r[4]) for r in rows if substr in r[2] and is_num(r[4]))

def lastof(substr):
    vals = [r for r in rows if substr in r[2] and is_num(r[4])]
    return float(vals[-1][4]) if vals else float('nan')

def maxof(substr):
    vals = [float(r[4]) for r in rows if substr in r[2] and is_num(r[4])]
    return max(vals) if vals else float('nan')

print(f'GC: gen0={total("gen0]"):.0f} gen1={total("gen1]"):.0f} gen2={total("gen2]"):.0f}')
print(f'total_allocated (накоплено): {lastof("total_allocated")/1e9:.1f} GB')
print(f'cpu.time суммарно: {total("cpu.time"):.0f}s')
print(f'lock contentions: {total("lock_contentions"):.0f}')
print(f'exceptions: {total("exceptions"):.0f}')
print(f'gc pause time: {total("pause.time"):.1f}s')
print(f'heap gen2 (последний): {lastof("heap.size (By)[gc.heap.generation=gen2]")/1e6:.0f} MB')
print(f'heap loh (последний): {lastof("heap.size (By)[gc.heap.generation=loh]")/1e6:.0f} MB')
print(f'working-set max: {maxof("working_set")/1e6:.0f} MB')
