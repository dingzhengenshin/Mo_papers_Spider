import os
import json
import pandas as pd
from collections import defaultdict
import re

def extract_paper_info_from_filename(filename):
    """从文件名中提取论文信息"""
    # 移除文件扩展名
    name_without_ext = os.path.splitext(filename)[0]

    # 尝试匹配不同的命名模式
    patterns = [
        r'(.+?)\s*-\s*(\d{4})\s*-\s*第(\d+)期',  # 期刊论文：标题-年份-期号
        r'(.+?)\s*-\s*(\d{4})',  # 论文：标题-年份
        r'(.+?)\s*第(\d+)期',  # 期刊：标题-期号
        r'(.+?)\s*(\d{4})',  # 其他：标题-年份
    ]

    title = name_without_ext
    year = None
    issue = None

    for pattern in patterns:
        match = re.match(pattern, name_without_ext)
        if match:
            groups = match.groups()
            if len(groups) == 3:  # 标题-年份-期号
                title, year, issue = groups
            elif len(groups) == 2:  # 标题-年份 或 标题-期号
                if groups[1].isdigit() and len(groups[1]) == 4:  # 标题-年份
                    title, year = groups
                else:  # 标题-期号
                    title, issue = groups
            break

    return title, year, issue

def categorize_papers_by_type(filename, title):
    """根据文件名和标题判断论文类型"""
    filename_lower = filename.lower()
    title_lower = title.lower()

    # 中文关键词分类
    if any(keyword in filename_lower for keyword in ['综述', '综', '述评', '评论']):
        return '综述'
    elif any(keyword in filename_lower for keyword in ['meta分析', 'meta-analysis', '系统评价', 'systematic']):
        return '系统评价/Meta分析'
    elif any(keyword in filename_lower for keyword in ['指南', '共识', '建议']):
        return '指南/共识'
    elif any(keyword in filename_lower for keyword in ['病例', 'case', 'case report']):
        return '病例报告'
    elif any(keyword in filename_lower for keyword in ['随机对照', 'rct', 'randomized']):
        return '随机对照试验'
    elif any(keyword in filename_lower for keyword in ['队列', 'cohort']):
        return '队列研究'
    elif any(keyword in filename_lower for keyword in ['横断面', 'cross-sectional']):
        return '横断面研究'
    elif any(keyword in filename_lower for keyword in ['病例对照', 'case-control']):
        return '病例对照研究'
    elif any(keyword in filename_lower for keyword in ['基础研究', '动物实验', '实验研究']):
        return '基础研究'
    elif any(keyword in filename_lower for keyword in ['临床研究', '临床观察']):
        return '临床研究'
    else:
        # 英文关键词分类
        if any(keyword in title_lower for keyword in ['review', 'survey', 'commentary']):
            return '综述'
        elif any(keyword in title_lower for keyword in ['meta-analysis', 'systematic review']):
            return '系统评价/Meta分析'
        elif any(keyword in title_lower for keyword in ['guideline', 'consensus', 'recommendation']):
            return '指南/共识'
        elif any(keyword in title_lower for keyword in ['case report', 'case study']):
            return '病例报告'
        elif any(keyword in title_lower for keyword in ['randomized', 'rct', 'random']):
            return '随机对照试验'
        elif any(keyword in title_lower for keyword in ['cohort', 'prospective']):
            return '队列研究'
        elif any(keyword in title_lower for keyword in ['cross-sectional']):
            return '横断面研究'
        elif any(keyword in title_lower for keyword in ['case-control', 'case-control']):
            return '病例对照研究'
        elif any(keyword in title_lower for keyword in ['animal', 'experimental', 'in vitro', 'in vivo']):
            return '基础研究'
        elif any(keyword in title_lower for keyword in ['clinical', 'patient', 'study']):
            return '临床研究'
        else:
            return '其他'

def count_papers():
    """统计vip_pdfs文件夹中的论文数量"""
    pdfs_folder = 'vip_pdfs'

    if not os.path.exists(pdfs_folder):
        print(f"文件夹 {pdfs_folder} 不存在")
        return

    # 统计数据
    total_count = 0
    type_stats = defaultdict(int)
    year_stats = defaultdict(int)
    issue_stats = defaultdict(int)
    paper_details = []

    # 遍历文件夹中的所有文件
    for filename in os.listdir(pdfs_folder):
        if filename.endswith('.pdf'):
            total_count += 1

            # 提取论文信息
            title, year, issue = extract_paper_info_from_filename(filename)
            paper_type = categorize_papers_by_type(filename, title)

            # 更新统计
            type_stats[paper_type] += 1
            if year:
                year_stats[year] += 1
            if issue:
                issue_stats[issue] += 1

            # 保存详细信息
            paper_details.append({
                'filename': filename,
                'title': title,
                'year': year,
                'issue': issue,
                'type': paper_type
            })

    # 输出统计结果
    print("=" * 50)
    print("VIP_PDFs 论文统计报告")
    print("=" * 50)
    print(f"论文总数: {total_count}")
    print()

    # 按类型统计
    print("1. 按论文类型统计:")
    sorted_types = sorted(type_stats.items(), key=lambda x: x[1], reverse=True)
    for paper_type, count in sorted_types:
        percentage = (count / total_count) * 100 if total_count > 0 else 0
        print(f"   {paper_type}: {count} 篇 ({percentage:.1f}%)")
    print()

    # 按年份统计
    if year_stats:
        print("2. 按年份统计:")
        sorted_years = sorted(year_stats.items(), key=lambda x: x[0], reverse=True)
        for year, count in sorted_years:
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            print(f"   {year}年: {count} 篇 ({percentage:.1f}%)")
        print()

    # 按期号统计（如果有）
    if issue_stats:
        print("3. 按期号统计:")
        sorted_issues = sorted(issue_stats.items(), key=lambda x: x[0], reverse=True)
        for issue, count in sorted_issues[:10]:  # 只显示前10个期号
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            print(f"   第{issue}期: {count} 篇 ({percentage:.1f}%)")
        if len(sorted_issues) > 10:
            print(f"   ... (其他 {len(sorted_issues) - 10} 个期号)")
        print()

    # 保存详细统计到CSV
    if paper_details:
        df = pd.DataFrame(paper_details)
        df.to_csv('vip_pdfs详细统计.csv', index=False, encoding='utf-8-sig')
        print("详细统计已保存到: vip_pdfs详细统计.csv")

    # 保存统计摘要到JSON
    summary = {
        'total_papers': total_count,
        'by_type': dict(sorted_types),
        'by_year': dict(sorted_years) if year_stats else {},
        'by_issue': dict(sorted_issues) if issue_stats else {}
    }

    with open('vip_pdfs统计摘要.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print("统计摘要已保存到: vip_pdfs统计摘要.json")

if __name__ == '__main__':
    count_papers()