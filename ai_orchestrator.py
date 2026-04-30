# -*- coding: utf-8 -*-
"""
AI 省份爬取调度器 — 按省份自动爬取全部机构，AI 决定下一步

用法：
  python ai_orchestrator.py                # 默认运行
  python ai_orchestrator.py --no-ai        # 纯规则模式
  python ai_orchestrator.py --status       # 打印当前进度
  python ai_orchestrator.py --reset        # 清空进度重新开始
  python ai_orchestrator.py --interval 30  # 检查间隔(秒)

前置：先手动登录各个 Chrome 端口的网站，再启动调度器。
"""

import argparse
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd
from openai import OpenAI

# ════════════════════════════════════════════
# 配置
# ════════════════════════════════════════════
DB_PATH       = Path("./data/membrane_papers.db")
EXCEL_PATH    = Path("./质检省一级单位统计.xlsx")
LOG_DIR       = Path("./data/logs")
PID_DIR       = Path("./data")
PROJECT_ROOT  = Path(__file__).parent.resolve()
PROGRESS_FILE = Path("./data/orchestrator_progress.json")
PAPERS_PER_PAGE = 20
CDP_TIMEOUT   = 3.0

# DeepSeek API（与 ai_browser_agent.py 一致）
DS_API_KEY  = "sk-fe9e74ce98ad4829a9c365ea06352ae3"
DS_BASE_URL = "https://api.deepseek.com"
DS_MODEL    = "deepseek-v4-flash"

# 端口 → 爬虫脚本 → 数据源（固定映射）
PORT_MAP = {
    9222: ("vip_institution_crawler.py",             "维普"),
    9223: ("cnki_mirror_crawler.py",                 "知网镜像站"),
    9224: ("vip_institution_crawler_threadtest1.py", "维普"),
    9225: ("hnsti_crawler.py",                       "HNSTI"),
}

LOOP_INTERVAL   = 60    # 主循环间隔(秒)
AI_EVERY_N      = 5     # 每 N 轮调一次 AI
MAX_ATTEMPTS    = 3     # 机构最大重试次数


# ════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════

def clean_name(s: str) -> str:
    """清理名称：去零宽空格 + strip"""
    return s.replace("\u200b", "").replace("\u200c", "").strip()


def load_institutions() -> dict[str, list[str]]:
    """读 Excel，返回 {省份: [机构列表]}，自动清理零宽空格和去重"""
    df = pd.read_excel(EXCEL_PATH)
    df.columns = ["col1", "province", "institution"] + list(df.columns[3:])
    df = df[["province", "institution"]].dropna()

    result: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        prov = clean_name(str(row["province"]))
        inst = clean_name(str(row["institution"]))
        if prov and inst:
            result.setdefault(prov, [])
            if inst not in result[prov]:
                result[prov].append(inst)
    return result


def check_cdp_alive(port: int) -> bool:
    try:
        url = f"http://127.0.0.1:{port}/json/version"
        resp = urllib.request.urlopen(url, timeout=CDP_TIMEOUT)
        json.loads(resp.read())
        return True
    except Exception:
        return False


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def get_db_conn():
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(DB_PATH, timeout=5)


# ════════════════════════════════════════════
# 进度状态管理
# ════════════════════════════════════════════

def bootstrap_progress(institutions: dict) -> dict:
    """创建空白进度状态"""
    now = datetime.now().isoformat()
    state = {
        "created_at": now,
        "last_updated": now,
        "current_province": None,
        "provinces": {},
        "port_assignments": {str(p): None for p in PORT_MAP},
    }
    for prov, inst_list in institutions.items():
        prov_data = {"status": "PENDING", "institutions": {}}
        for inst in inst_list:
            prov_data["institutions"][inst] = {
                "status": "PENDING",
                "source_dbs": {
                    src: {"papers_in_db": 0, "status": "PENDING"}
                    for src in ["维普", "知网镜像站", "HNSTI"]
                },
                "last_attempt": None,
                "attempts": 0,
            }
        state["provinces"][prov] = prov_data
    return state


def reconcile_with_db(state: dict):
    """从数据库更新已有数据"""
    conn = get_db_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # 查每个 institution + source_db 的论文数
        cur.execute("""
            SELECT institution, source_db, COUNT(*) as cnt
            FROM papers
            WHERE institution IS NOT NULL AND institution != ''
            GROUP BY institution, source_db
        """)
        for row in cur.fetchall():
            db_inst = clean_name(row[0])
            src_db = row[1]
            cnt = row[2]
            # 匹配到 progress 中的机构
            for prov_data in state["provinces"].values():
                if db_inst in prov_data["institutions"]:
                    inst_data = prov_data["institutions"][db_inst]
                    # source_db 名称可能不完全一致，模糊匹配
                    for src_key in inst_data["source_dbs"]:
                        if src_key in src_db or src_db in src_key:
                            inst_data["source_dbs"][src_key]["papers_in_db"] = cnt
                            inst_data["source_dbs"][src_key]["status"] = "COMPLETED"
                    break
        conn.close()
    except Exception as e:
        print(f"[ORCH] 数据库同步失败: {e}")
        if conn:
            conn.close()


def load_progress(institutions: dict) -> dict:
    """加载或创建进度"""
    if PROGRESS_FILE.exists():
        try:
            state = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            # 确保所有机构都存在（Excel可能更新）
            for prov, inst_list in institutions.items():
                if prov not in state["provinces"]:
                    state["provinces"][prov] = {
                        "status": "PENDING",
                        "institutions": {
                            inst: _new_inst_entry() for inst in inst_list
                        },
                    }
                else:
                    for inst in inst_list:
                        if inst not in state["provinces"][prov]["institutions"]:
                            state["provinces"][prov]["institutions"][inst] = _new_inst_entry()
            return state
        except Exception as e:
            print(f"[ORCH] 进度文件损坏，重新创建: {e}")

    state = bootstrap_progress(institutions)
    reconcile_with_db(state)
    return state


def _new_inst_entry() -> dict:
    return {
        "status": "PENDING",
        "source_dbs": {
            src: {"papers_in_db": 0, "status": "PENDING"}
            for src in ["维普", "知网镜像站", "HNSTI"]
        },
        "last_attempt": None,
        "attempts": 0,
    }


def save_progress(state: dict):
    state["last_updated"] = datetime.now().isoformat()
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ════════════════════════════════════════════
# 状态自动推导
# ════════════════════════════════════════════

def refresh_statuses(state: dict):
    """根据 source_dbs 数据重新推导机构/省份状态"""
    for prov_name, prov_data in state["provinces"].items():
        all_done = True
        for inst_name, inst_data in prov_data["institutions"].items():
            if inst_data["status"] in ("PENDING", "IN_PROGRESS"):
                # 检查是否所有 source_db 都完成了
                all_src_done = True
                has_any_data = False
                for src_key, src_data in inst_data["source_dbs"].items():
                    if src_data["status"] not in ("COMPLETED",):
                        all_src_done = False
                    if src_data["papers_in_db"] > 0:
                        has_any_data = True

                if all_src_done:
                    inst_data["status"] = "COMPLETED"
                elif inst_data["attempts"] >= MAX_ATTEMPTS and not has_any_data:
                    inst_data["status"] = "FAILED"

            if inst_data["status"] not in ("COMPLETED", "FAILED", "SKIPPED"):
                all_done = False

        if all_done and prov_data["status"] != "COMPLETED":
            prov_data["status"] = "COMPLETED"
        elif not all_done and prov_data["status"] == "COMPLETED":
            prov_data["status"] = "IN_PROGRESS"


def find_current_province(state: dict) -> str | None:
    """找到当前应该爬取的省份"""
    for prov_name, prov_data in state["provinces"].items():
        if prov_data["status"] == "IN_PROGRESS":
            return prov_name
    # 找第一个 PENDING 省份
    for prov_name, prov_data in state["provinces"].items():
        if prov_data["status"] == "PENDING":
            return prov_name
    return None


# ════════════════════════════════════════════
# 规则调度器（不用 AI）
# ════════════════════════════════════════════

def rule_based_plan(state: dict) -> list[dict]:
    """
    规则调度：当前省份 → 下一个 PENDING 机构 → 匹配空闲端口。
    返回 [{"port": N, "institution": "xxx", "script": "xxx.py", "page": 1}, ...]
    """
    assignments = []

    # 收集所有可用端口
    available_ports = []
    for port_str, assign in state["port_assignments"].items():
        port = int(port_str)
        if assign is None:
            available_ports.append(port)
        elif not is_process_alive(assign.get("pid", 0)):
            # 进程已死，回收端口
            _harvest_dead_crawler(state, port_str, assign)
            available_ports.append(port)

    if not available_ports:
        return []

    # 收集待爬工作项（按省份顺序）
    work_items = []
    for prov_name, prov_data in state["provinces"].items():
        if prov_data["status"] in ("COMPLETED",):
            continue
        for inst_name, inst_data in prov_data["institutions"].items():
            if inst_data["status"] not in ("PENDING",):
                continue
            # 看哪些 source_db 还没爬
            for src_key, src_data in inst_data["source_dbs"].items():
                if src_data["status"] not in ("PENDING",):
                    continue
                work_items.append({
                    "province": prov_name,
                    "institution": inst_name,
                    "source_db": src_key,
                })

    # 为工作项匹配端口
    for item in work_items:
        if not available_ports:
            break
        # 找对应 source_db 的端口
        for port in list(available_ports):
            script, src = PORT_MAP[port]
            if src == item["source_db"]:
                # 检查 CDP 是否活着
                if check_cdp_alive(port):
                    assignments.append({
                        "port": port,
                        "institution": item["institution"],
                        "script": script,
                        "page": 1,
                    })
                    available_ports.remove(port)
                    break

    return assignments


# ════════════════════════════════════════════
# AI 调度器（DeepSeek）
# ════════════════════════════════════════════

PLANNER_PROMPT = """你是一个爬虫调度AI。分析当前爬取进度，决定下一步操作。

端口映射（固定）：
- 9222: vip_institution_crawler.py (维普)
- 9223: cnki_mirror_crawler.py (知网镜像站)
- 9224: vip_institution_crawler_threadtest1.py (维普)
- 9225: hnsti_crawler.py (HNSTI)

决策原则：
1. 按省份顺序推进，一个省份完成再进入下一个
2. 省份内按顺序爬取机构
3. 每个机构要在所有数据源爬取（维普、知网、HNSTI）
4. 已有数据的机构标记完成
5. 失败3次的机构跳过

直接返回JSON（不要markdown代码块）：
{
  "thought": "分析当前进度和决策理由",
  "action": "assign" | "wait" | "all_done",
  "assignments": [
    {"port": 9222, "institution": "机构名", "script": "vip_institution_crawler.py", "page": 1}
  ]
}
"""


def ai_plan(state: dict) -> list[dict]:
    """调 DeepSeek 决定下一步"""
    # 构建紧凑的进度摘要
    context_lines = ["=== 爬取进度 ==="]

    prov_count = 0
    inst_done = 0
    inst_total = 0
    for prov_name, prov_data in state["provinces"].items():
        pstatus = prov_data["status"]
        done = sum(1 for i in prov_data["institutions"].values()
                   if i["status"] in ("COMPLETED",))
        total = len(prov_data["institutions"])
        inst_done += done
        inst_total += total

        if pstatus != "COMPLETED":
            prov_count += 1
            context_lines.append(f"\n{prov_name} [{pstatus}] ({done}/{total})")
            for inst_name, inst_data in prov_data["institutions"].items():
                if inst_data["status"] not in ("COMPLETED", "FAILED"):
                    srcs = " ".join(
                        f"{k}:{v['status']}({v['papers_in_db']}篇)"
                        for k, v in inst_data["source_dbs"].items()
                    )
                    context_lines.append(
                        f"  {inst_name} [{inst_data['status']}] "
                        f"attempts={inst_data['attempts']} {srcs}"
                    )

    context_lines.append(f"\n总进度: {inst_done}/{inst_total} 机构, "
                         f"{prov_count} 省份未完成")

    # 端口状态
    context_lines.append("\n=== 端口状态 ===")
    for port_str, assign in state["port_assignments"].items():
        port = int(port_str)
        cdp_ok = check_cdp_alive(port)
        if assign:
            alive = is_process_alive(assign.get("pid", 0))
            context_lines.append(
                f"  {port_str}: {'爬取中' if alive else '进程已死'} "
                f"[{assign['institution']}] PID={assign.get('pid')} "
                f"CDP={'OK' if cdp_ok else 'OFF'}"
            )
        else:
            context_lines.append(f"  {port_str}: 空闲 CDP={'OK' if cdp_ok else 'OFF'}")

    context_lines.append("\n请决定下一步。")

    user_msg = "\n".join(context_lines)

    try:
        client = OpenAI(api_key=DS_API_KEY, base_url=DS_BASE_URL)
        resp = client.chat.completions.create(
            model=DS_MODEL,
            messages=[
                {"role": "system", "content": PLANNER_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=1024,
            temperature=0.1,
        )
        text = resp.choices[0].message.content.strip()
        print(f"  [AI] {text[:200]}")

        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            result = json.loads(json_match.group())
        else:
            print(f"  [AI] 无法解析，回退规则模式")
            return rule_based_plan(state)

        if result.get("action") == "all_done":
            return "ALL_DONE"

        return result.get("assignments", [])

    except Exception as e:
        print(f"  [AI] 调用失败: {e}，回退规则模式")
        return rule_based_plan(state)


# ════════════════════════════════════════════
# 爬虫管理
# ════════════════════════════════════════════

def launch_crawler(script: str, port: int, institution: str, page: int) -> int:
    """启动爬虫进程，返回 PID"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"orchestrator_{Path(script).stem}_{institution[:20]}.log"

    cmd = [sys.executable, script, "--institution", institution]
    if page > 1:
        cmd += ["--page", str(page)]

    print(f"  启动: {' '.join(cmd)}")
    with open(log_file, "a", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=lf,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
    return proc.pid


def _harvest_dead_crawler(state: dict, port_str: str, assign: dict):
    """处理死掉的爬虫：更新进度，回收端口"""
    inst_name = assign.get("institution", "")
    pid = assign.get("pid", 0)
    print(f"  [回收] 端口{port_str} PID={pid} 机构={inst_name} 已停止")

    # 查数据库看这个机构爬了多少
    conn = get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            script_name = assign.get("script", "")
            src_db = ""
            for p, (s, sd) in PORT_MAP.items():
                if s == script_name:
                    src_db = sd
                    break

            cur.execute(
                "SELECT COUNT(*) FROM papers WHERE institution LIKE ?",
                (f"%{inst_name}%",)
            )
            cnt = cur.fetchone()[0]
            print(f"    数据库中有 {cnt} 篇论文")

            # 更新机构进度
            for prov_data in state["provinces"].values():
                if inst_name in prov_data["institutions"]:
                    inst = prov_data["institutions"][inst_name]
                    inst["attempts"] = inst.get("attempts", 0) + 1
                    if src_db and src_db in inst["source_dbs"]:
                        if cnt > 0:
                            inst["source_dbs"][src_db]["papers_in_db"] = cnt
                            inst["source_dbs"][src_db]["status"] = "COMPLETED"
                        else:
                            inst["source_dbs"][src_db]["status"] = "PENDING"
                    break
            conn.close()
        except Exception as e:
            print(f"    查询失败: {e}")
            if conn:
                conn.close()

    state["port_assignments"][port_str] = None


def check_and_harvest(state: dict):
    """检查所有运行中的爬虫，回收死掉的"""
    for port_str, assign in list(state["port_assignments"].items()):
        if assign is None:
            continue
        pid = assign.get("pid", 0)
        if not is_process_alive(pid):
            _harvest_dead_crawler(state, port_str, assign)


def execute_assignments(state: dict, assignments: list[dict]):
    """执行分配方案"""
    for asgn in assignments:
        port = asgn.get("port")
        institution = asgn.get("institution", "")
        script = asgn.get("script", "")
        page = asgn.get("page", 1)

        if not all([port, institution, script]):
            continue

        port_str = str(port)

        # 端口已被占用？
        if state["port_assignments"].get(port_str) is not None:
            continue

        # CDP 不通？
        if not check_cdp_alive(port):
            print(f"  [跳过] 端口{port} CDP不通")
            continue

        # 启动爬虫
        pid = launch_crawler(script, port, institution, page)
        state["port_assignments"][port_str] = {
            "institution": institution,
            "script": script,
            "pid": pid,
            "started_at": datetime.now().isoformat(),
        }

        # 更新机构状态
        for prov_data in state["provinces"].values():
            if institution in prov_data["institutions"]:
                prov_data["status"] = "IN_PROGRESS"
                prov_data["institutions"][institution]["status"] = "IN_PROGRESS"
                prov_data["institutions"][institution]["last_attempt"] = datetime.now().isoformat()
                # 更新对应的 source_db
                src_db = ""
                for p, (s, sd) in PORT_MAP.items():
                    if p == port:
                        src_db = sd
                        break
                if src_db and src_db in prov_data["institutions"][institution]["source_dbs"]:
                    prov_data["institutions"][institution]["source_dbs"][src_db]["status"] = "IN_PROGRESS"
                break

        print(f"  已分配 端口{port} → {institution} (PID={pid})")


# ════════════════════════════════════════════
# 状态报告
# ════════════════════════════════════════════

def print_status(state: dict):
    """打印调度器状态"""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'='*70}")
    print(f"  AI 省份爬取调度器  {now}")
    print(f"{'='*70}")

    # 省份进度
    inst_done = 0
    inst_total = 0
    prov_done = 0
    for prov_name, prov_data in state["provinces"].items():
        pstatus = prov_data["status"]
        done = sum(1 for i in prov_data["institutions"].values()
                   if i["status"] in ("COMPLETED",))
        total = len(prov_data["institutions"])
        inst_done += done
        inst_total += total
        if pstatus == "COMPLETED":
            prov_done += 1

        if pstatus != "COMPLETED":
            bar_len = 20
            filled = int(bar_len * done / total) if total else 0
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"  {prov_name:12s} [{bar}] {done}/{total} {pstatus}")

            # 显示当前/下一个机构
            for inst_name, inst_data in prov_data["institutions"].items():
                if inst_data["status"] in ("IN_PROGRESS",):
                    srcs = " ".join(
                        f"{k}:{v['status']}"
                        for k, v in inst_data["source_dbs"].items()
                        if v["status"] != "COMPLETED"
                    )
                    print(f"    ▶ {inst_name} {srcs}")
                elif inst_data["status"] == "PENDING":
                    print(f"    · {inst_name}")
                    break  # 只显示第一个 pending

    total_prov = len(state["provinces"])
    print(f"\n  总进度: {prov_done}/{total_prov} 省份, "
          f"{inst_done}/{inst_total} 机构")

    # 端口状态
    print(f"\n  {'端口':<6} {'CDP':<5} {'状态':<8} {'机构':<20} {'PID':<8}")
    print(f"  {'-'*50}")
    for port_str, assign in state["port_assignments"].items():
        port = int(port_str)
        cdp = "OK" if check_cdp_alive(port) else "OFF"
        if assign:
            alive = is_process_alive(assign.get("pid", 0))
            status = "运行中" if alive else "已死"
            print(f"  {port_str:<6} {cdp:<5} {status:<8} "
                  f"{assign['institution'][:20]:<20} {assign.get('pid', '')}")
        else:
            print(f"  {port_str:<6} {cdp:<5} {'空闲':<8}")

    print(f"{'='*70}")


def print_status_only():
    """--status 模式：只打印状态"""
    institutions = load_institutions()
    state = load_progress(institutions)
    reconcile_with_db(state)
    refresh_statuses(state)
    print_status(state)

    # 额外统计
    conn = get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM papers")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM papers WHERE download_status='ok'")
            ok = cur.fetchone()[0]
            print(f"  数据库: {total} 篇论文, {ok} 篇已下载PDF")
            conn.close()
        except:
            if conn:
                conn.close()


# ════════════════════════════════════════════
# 主循环
# ════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="AI 省份爬取调度器")
    parser.add_argument("--interval", type=int, default=LOOP_INTERVAL,
                        help=f"检查间隔秒数 (默认{LOOP_INTERVAL})")
    parser.add_argument("--no-ai", action="store_true",
                        help="纯规则模式，不调 DeepSeek")
    parser.add_argument("--status", action="store_true",
                        help="只打印当前状态")
    parser.add_argument("--reset", action="store_true",
                        help="清空进度重新开始")
    args = parser.parse_args()

    # --status 模式
    if args.status:
        print_status_only()
        return

    # --reset
    if args.reset and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        print("[ORCH] 已清空进度文件")

    # 加载
    print("[ORCH] 加载机构列表...")
    institutions = load_institutions()
    total_inst = sum(len(v) for v in institutions.values())
    print(f"  {len(institutions)} 个省份, {total_inst} 个机构")

    print("[ORCH] 加载/同步进度...")
    state = load_progress(institutions)
    reconcile_with_db(state)
    refresh_statuses(state)
    save_progress(state)

    print("[ORCH] 调度器启动！按 Ctrl+C 停止\n")
    print_status(state)

    cycle = 0
    try:
        while True:
            cycle += 1
            time.sleep(args.interval)

            # 1. 检查爬虫进程，回收死掉的
            check_and_harvest(state)

            # 2. 定期刷新 DB 数据
            if cycle % 3 == 0:
                reconcile_with_db(state)

            # 3. 推导状态
            refresh_statuses(state)

            # 4. 检查是否全部完成
            all_done = all(
                p["status"] == "COMPLETED"
                for p in state["provinces"].values()
            )
            if all_done:
                print("\n[ORCH] 全部省份爬取完成！")
                print_status(state)
                break

            # 5. 调度
            if args.no_ai:
                assignments = rule_based_plan(state)
            elif cycle % AI_EVERY_N == 0:
                print(f"\n[AI 调度] 第 {cycle} 轮...")
                assignments = ai_plan(state)
                if assignments == "ALL_DONE":
                    print("\n[ORCH] AI 判定全部完成！")
                    print_status(state)
                    break
            else:
                assignments = rule_based_plan(state)

            # 6. 执行
            if assignments:
                print(f"\n[调度] 第 {cycle} 轮分配:")
                execute_assignments(state, assignments)

            # 7. 保存 + 打印
            save_progress(state)
            if cycle % 2 == 0:
                print_status(state)

    except KeyboardInterrupt:
        print("\n[ORCH] 已停止。进度已保存，下次启动会继续。")
        save_progress(state)


if __name__ == "__main__":
    main()
