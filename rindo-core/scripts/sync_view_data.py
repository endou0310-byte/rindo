# scripts/sync_view_data.py
import os, shutil, filecmp, pathlib
SRC=pathlib.Path("data/out"); DST=pathlib.Path("view/data/out")
DST.mkdir(parents=True, exist_ok=True)
for p in SRC.glob("*"):
    if p.is_dir(): 
        continue
    q = DST/p.name
    if not q.exists() or not filecmp.cmp(str(p), str(q), shallow=False):
        shutil.copy2(p, q); print("updated:", p.name)
print("sync done.")
