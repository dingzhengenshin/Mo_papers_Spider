import sqlite3
import os

def create_database():
    # 1. 确保 data 文件夹存在
    os.makedirs('data', exist_ok=True)

    # 2. 在 data 文件夹下创建 SQLite 数据库文件
    db_path = os.path.join('data', 'membrane_papers.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. 创建一张名为 papers 的数据表
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS papers (
                                                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                         title TEXT UNIQUE,       -- 论文/政策标题 (设置 UNIQUE 防止爬虫重复抓取)
                                                         authors TEXT,            -- 作者或发布部门
                                                         abstract_text TEXT,      -- 摘要或核心内容
                                                         keywords TEXT,           -- 关键词
                                                         source_db TEXT,          -- 来源库 (如：知网、浙江省政府网)
                                                         publish_year TEXT,       -- 发布年份
                                                         download_link TEXT,      -- 原始网页链接
                                                         pdf_local_path TEXT,     -- 本地 PDF 保存路径 (下载后填入)

                       -- 下面这三个字段现在留空，留给 Phase 2 的大模型来自动分析填入 --
                                                         ai_industry_category TEXT, -- AI研判：行业大类
                                                         ai_product_category TEXT,  -- AI研判：细分品类
                                                         ai_quality_issue TEXT,     -- AI研判：涉及的质量标准或问题

                                                         scrape_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 抓取时间
                   )
                   ''')

    conn.commit()
    conn.close()
    print(f"✅ 数据库初始化成功！\n碗准备好了，文件位置: {db_path}")

if __name__ == '__main__':
    create_database()
