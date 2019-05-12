from datetime import datetime
import os
import random
import glob
import re
import bisect


# YYYY-MM-DD HH:MM to Unix timestamp
def datetime2ts(s):
    dt = datetime.strptime(s, '%Y-%m-%d %H:%M')
    ts = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(ts)


def ts2datetime(ts):
    dt = datetime.utcfromtimestamp(ts)
    return datetime.strftime(dt, '%Y-%m-%d %H:%M')


def ip2int(ip):
    vals = [int(p) for p in ip.split('.')]
    return vals[0] << 24 | vals[1] << 16 | vals[2] << 8 | vals[3]


def gen_ip(count):
    cnt = 0
    assert count < 255 * 254

    for i in range(254):
        for j in range(1, 255):
            if cnt < count:
                cnt += 1
                yield "192.168.%d.%d" % (i, j)


def gen_one_log(ts, nserver, cpus, fpath):
    assert nserver < 254 * 255
    with open(fpath, 'w+', encoding='utf-8') as f:
        f.write('\t'.join(['timestamp', 'IP', 'cpu_id', 'usage']) + '\n')
        for ip in gen_ip(nserver):
            f.write("%d\t%s\t%d\t%d\n" % (ts, ip, 0, random.randrange(0, 100)))
            f.write("%d\t%s\t%d\t%d\n" % (ts, ip, 1, random.randrange(0, 100)))


def gen_logs(start_dt, end_dt, dst_dir, nservers=1000, cpus=2):
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    start_ts = datetime2ts(start_dt)
    end_ts = datetime2ts(end_dt)
    while start_ts < end_ts:
        fpath = os.path.join(dst_dir, "%s.log" % str(start_ts))
        gen_one_log(start_ts, nservers, cpus, fpath)
        start_ts += 60


# return a dict of log entries: {'ip':[[(ts, usage), ...], [(ts, usage),..]],..}
# some opitimzations to speed up query and reduce memory footprint:
# 1) use int for IP instead of string;
# 2) sort [(ts, usage)] list for each cpu.
def load_data(data_path, ncpus=2):
    servers = dict()
    for f in glob.glob(data_path.rstrip('/') + '/*.log'):
        entries = (line.strip().split('\t')
                   for line in open(f).readlines()[1:])
        for ts, ip, cpu, usage in entries:
            ip_val = ip2int(ip)
            if ip_val not in servers:
                servers[ip_val] = [[] for i in range(ncpus)]
            servers[ip_val][int(cpu)].append((int(ts), int(usage)))

    for s in servers:
        for cpu in range(len(servers[s])):
            servers[s][cpu].sort()

    return servers


def gen_logs_main(data_dir):
    gen_logs('2014-10-31 00:00', '2014-11-01 00:00', data_dir)
    return 0


def query_main(data_dir):
    records = load_data(data_dir)

    while True:
        os.sys.stdout.write('>')
        os.sys.stdout.flush()
        line = os.sys.stdin.readline().strip()
        if line == 'EXIT':
            break
        # match 'QUERY IP cpu_id time_start time_end', e.g:
        # QUERY 192.168.1.10 1 2014-10-31 00:00 2014-10-31 00:05
        patt = r'QUERY\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+([01])\s+([12]\d\d\d-[01]\d-[0123]\d [012]\d:[012345]\d)\s+([12]\d\d\d-[01]\d-[0123]\d [012]\d:[012345]\d)'
        m = re.match(patt, line)
        if not m:
            print("wrong command or query string!")
            continue

        ip, cpu_id, time_start, time_end = m.groups()
        start_ts = datetime2ts(time_start)
        end_ts = datetime2ts(time_end)
        ip_val = ip2int(ip)
        server = records.get(ip_val, None)
        cpu = int(cpu_id)
        if not server or cpu >= len(server):
            print("wrong command or query string!")
            continue

        # invalid range
        if start_ts > end_ts:
            print("wrong command or query string!")
            continue
        # out of range
        if server[cpu][0][0] > start_ts or start_ts > server[cpu][-1][0]:
            print("wrong command or query string!")
            continue

        # generic query implement:
        # results = [
        #     "({t}, {u}%)".format(t=ts2datetime(r[0]), u=r[1]) for r in server[cpu]
        #     if r[0] >= start_ts and r[0] < end_ts
        # ]

        # ts is sorted and exactly 1 min interval, so simiply compute the index:
        start_idx = (start_ts - server[cpu][0][0]) // 60
        results = []
        while start_idx < len(server[cpu]):
            log = server[cpu][start_idx]
            if log[0] >= end_ts:
                break
            start_idx += 1
            results.append("({t}, {u}%)".format(t=ts2datetime(log[0]), u=log[1]))

        print("CPU%s usage on %s:" % (cpu_id, ip))
        print(", ".join(results))

    return 0

if __name__ == '__main__':
    if len(os.sys.argv) != 3:
        os.sys.exit(-1)
    if os.sys.argv[1] == 'generate':
        os.sys.exit(gen_logs_main(os.sys.argv[2]))
    elif os.sys.argv[1] == 'query':
        os.sys.exit(query_main(os.sys.argv[2]))
    else:
        os.sys.exit(-2)
