import os
import pandas as pd

base_dir = os.path.dirname(os.path.abspath(__file__))
rows = []

for name in os.listdir(base_dir):
    folder = os.path.join(base_dir, name)
    if not os.path.isdir(folder):
        continue
    for fname in os.listdir(folder):
        if fname.lower().endswith(".pdf"):
            title = fname[:-4]  # 去掉 .pdf
            rows.append({"机构名": name, "论文名": title})

df = pd.DataFrame(rows)
out_path = os.path.join(base_dir, "论文名称清单.xlsx")
df.to_excel(out_path, index=False)
print(f"共 {len(rows)} 条记录，已保存到 {out_path}")
