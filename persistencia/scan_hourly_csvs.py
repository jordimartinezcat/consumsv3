import os
import glob

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'procesado', 'Data')
pattern = os.path.join(DATA_DIR, 'consumption_hourly_*.csv')
files = sorted(glob.glob(pattern))

print('Scanning', len(files), 'hourly CSV files')

for f in files:
    with open(f, 'r', encoding='utf-8', errors='replace') as fh:
        for i, line in enumerate(fh):
            if '2025-06-02' in line:
                print('\nFile:', os.path.basename(f), 'Line', i+1)
                print(line.strip())
            # also detect very large numbers patterns (20 million etc)
            if '20001041' in line or '20001041.000' in line or '20001041,000' in line:
                print('\nFile (big numeric):', os.path.basename(f), 'Line', i+1)
                print(line.strip())

print('\nDone scanning')
