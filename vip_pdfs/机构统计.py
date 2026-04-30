#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VIP PDFs机构论文统计脚本
统计各个机构的PDF论文数量
"""

from pathlib import Path


def main():
    vip_pdfs_dir = Path(__file__).parent
    total = 0
    institutions = []

    for d in vip_pdfs_dir.iterdir():
        if not d.is_dir() or d.name.startswith('.'):
            continue
        name = d.name.replace('\u200b', '').replace('\ufeff', '')
        count = sum(1 for f in d.iterdir() if f.is_file() and f.suffix.lower() == '.pdf')
        institutions.append((name, count))
        total += count

    institutions.sort(key=lambda x: x[1], reverse=True)

    print("各机构论文统计:")
    print("-" * 40)
    for name, count in institutions:
        print(f"  {name}: {count} 篇")
    print("-" * 40)
    print(f"  共 {len(institutions)} 个机构，{total} 篇论文")


if __name__ == "__main__":
    main()
