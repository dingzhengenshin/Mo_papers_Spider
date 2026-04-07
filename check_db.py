import sqlite3
from pathlib import Path
import csv

# 数据库路径
DB_PATH = Path("./data/membrane_papers.db")
CSV_PATH = Path("./data/all_papers_data.csv")

def check_database():
    if not DB_PATH.exists():
        print(f" 找不到数据库文件: {DB_PATH.resolve()}")
        return

    print(f" 找到数据库: {DB_PATH.resolve()}")

    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # try:
    #     # 1. 检查表是否存在
    #     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='papers'")
    #     if not cursor.fetchone():
    #         print(" 数据库里没有 'papers' 这张表！")
    #         return
    #
    #     # 2. 统计总行数
    #     cursor.execute("SELECT COUNT(*) as total FROM papers")
    #     total_records = cursor.fetchone()["total"]
    #     print(f"\n 当前数据库共有 【{total_records}】 篇论文记录")
    #
    #     if total_records == 0:
    #         print(" 表是空的，没有任何数据。")
    #         return
    #
    #     # 3. 统计下载成功和跳过的数量
    #     cursor.execute("SELECT download_status, COUNT(*) as count FROM papers GROUP BY download_status")
    #     print("\n 状态统计:")
    #     for row in cursor.fetchall():
    #         print(f"  - {row['download_status']}: {row['count']} 篇")
    #
    #     cursor.execute("SELECT id, title, authors, source_db, download_status FROM papers ORDER BY id DESC")
    #     for row in cursor.fetchall():
    #         print(f"  [ID: {row['id']}] | 状态: {row['download_status']} | 来源: {row['source_db']}")
    #         print(f"  标题: {row['title']}")
    #         print(f"  作者: {row['authors']}")
    #         print("-" * 50)
    #
    # except sqlite3.Error as e:
    #     print(f" 数据库读取错误: {e}")
    # finally:
    #     conn.close()
    try:
        # 执行 SQL 查询，拿走所有数据
        cursor.execute("SELECT * FROM papers")
        rows = cursor.fetchall()

        if not rows:
            print(" 数据库里现在一条数据都没有哦。")
            return

        # 获取所有列名（表头）
        col_names = [description[0] for description in cursor.description]

        # 写入 CSV 文件
        with open(CSV_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(col_names)  # 写入第一行表头
            writer.writerows(rows)      # 瞬间写入所有行数据

        print(f" 太棒了！已成功导出 【{len(rows)}】 条完整数据！")
        print(f" 文件位置: {CSV_PATH.resolve()}")
        print(" 现在你可以直接双击用 Excel 或 WPS 打开它，查看所有行和列了！")

    except sqlite3.Error as e:
        print(f" 数据库操作出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_database()
