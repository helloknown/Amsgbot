"""
多源快讯聚合推送 实时快讯推送（akshare）
- 统一格式化输出
- 钉钉推送
- 自动去重（sent_ids）
- 本地 JSON 持久化，程序重启仍可避免重复推送
- 定期清理旧记录
依赖: pip install akshare requests pandas
"""
import logging
import time
import requests
import akshare as ak
import pandas as pd
import json
import os
from difflib import SequenceMatcher
import hashlib
from collections import deque
import re
from logger_util import setup_logger

logger = setup_logger("app_log", log_dir="logs", level=logging.INFO)

# ========== 配置 ==========
DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token="
FETCH_INTERVAL = 30      # 秒
MAX_SENT_IDS = 10000     # 最多保留记录
MAX_AGE = 24 * 3600       # 保留时间（秒）
CLEAN_INTERVAL = 1440     # 每多少轮清理一次(24*(3600/30))
SENT_IDS_FILE = "sent_ids.json"  # 本地持久化文件

# 钉钉
MAX_PER_MINUTE = 20
last_minute = time.time()
sent_count = 0
# ==========================

# sent_ids: {uid: timestamp}
sent_ids = {}
loop_count = 0

# 保存最近已推送的内容文本（限定容量，防内存爆）
recent_contents = deque(maxlen=1000)

# ---------- 加载本地 sent_ids ----------
def load_sent_ids():
    global sent_ids
    if os.path.exists(SENT_IDS_FILE):
        try:
            with open(SENT_IDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # 过滤过期记录
                now = time.time()
                sent_ids = {uid: ts for uid, ts in data.items() if now - ts < MAX_AGE}
                logger.info(f"[加载] {len(sent_ids)} 条历史记录已加载")
        except Exception as e:
            logger.error(f"[x] 加载 sent_ids 文件失败: {e}")
            sent_ids = {}
    else:
        sent_ids = {}

# ---------- 保存本地 sent_ids ----------
def save_sent_ids():
    try:
        with open(SENT_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(sent_ids, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"[x] 保存 sent_ids 文件失败: {e}")


# ---------- 钉钉推送 ----------
def send_to_dingtalk(source: str, ts: str, content: str):
    msg = f"📰【{source}】{ts}\n{content}"
    payload = {"msgtype": "text", "text": {"content": msg}}
    try:
        requests.post(DINGTALK_WEBHOOK, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"[x] 钉钉推送失败: {e}")

def send_rate_limited(source, ts, content):
    global last_minute, sent_count
    now = time.time()
    if now - last_minute >= 60:
        last_minute = now
        sent_count = 0
    if sent_count >= MAX_PER_MINUTE:
        # 等待到下一分钟
        sleep_time = 60 - (now - last_minute)
        time.sleep(sleep_time)
        last_minute = time.time()
        sent_count = 0
    send_to_dingtalk(source, ts, content)
    sent_count += 1


# ---------- 财联社 ----------
def process_cls(symbol: str = "全部") -> pd.DataFrame:
    try:
        df = ak.stock_info_global_cls(symbol=symbol)
        if df is None or df.empty:
            return pd.DataFrame(columns=["时间", "内容", "来源"])
        df["时间"] = df["发布日期"].astype(str) + " " + df["发布时间"].astype(str)
        df["内容"] = df["标题"].astype(str) + "：" + df["内容"].astype(str)
        df["来源"] = "财联社"
        return df[["时间", "内容", "来源"]]
    except Exception as e:
        logger.error(f"[x] 财联社数据处理失败: {e}")
        return pd.DataFrame(columns=["时间", "内容", "来源"])


# ---------- 新浪财经 ----------
def process_sina() -> pd.DataFrame:
    try:
        df = ak.stock_info_global_sina()
        if df is None or df.empty:
            return pd.DataFrame(columns=["时间", "内容", "来源"])

        df["时间"] = pd.to_datetime(df["时间"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["内容"] = df["内容"].astype(str)
        df["来源"] = "新浪财经"
        return df[["时间", "内容", "来源"]]
    except Exception as e:
        logger.error(f"[x] 新浪财经数据处理失败: {e}")
        return pd.DataFrame(columns=["时间", "内容", "来源"])

# ---------- 同花顺财经 ----------
def process_ths() -> pd.DataFrame:
    try:
        df = ak.stock_info_global_ths()
        if df is None or df.empty:
            return pd.DataFrame(columns=["时间", "内容", "来源"])

        df["时间"] = pd.to_datetime(df["发布时间"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["内容"] = df["标题"].astype(str) + "：" + df["内容"].astype(str)
        df["来源"] = "同花顺"
        return df[["时间", "内容", "来源"]]
    except Exception as e:
        logger.error(f"[x] 同花顺经数据处理失败: {e}")
        return pd.DataFrame(columns=["时间", "内容", "来源"])


def normalize_text(text: str) -> str:
    # 去除HTML标签、换行符、多余空格
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ------- 计算内容相似度 -------
def is_similar(a: str, b: str, threshold=0.85) -> bool:
    return SequenceMatcher(None, a, b).ratio() > threshold

def is_recently_sent(content: str) -> bool:
    """检查该内容是否与最近已推送内容相似"""
    normalized = normalize_text(content)
    for old in recent_contents:
        if is_similar(normalized, old):
            return True
    return False

# ---------- 唯一ID ----------
def make_uid(row) -> str:
    key = f"{row.get('来源','')}_{row.get('时间','')}_{row.get('内容','')}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


# ---------- 主循环 ----------
def main_loop():
    global loop_count, sent_ids
    load_sent_ids()
    logger.info("🚀 启动 财联社 + 新浪财经 实时快讯推送（含持久化）...\n")

    while True:
        loop_count += 1
        frames = []

        # --------------------------
        df_cls = process_cls()
        if not df_cls.empty:
            frames.append(df_cls)

        df_sina = process_sina()
        if not df_sina.empty:
            frames.append(df_sina)

        df_ths = process_ths()
        if not df_ths.empty:
            frames.append(df_ths)

        #---------------------------
        if not frames:
            logger.info("无数据")
            time.sleep(FETCH_INTERVAL)
            continue

        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.drop_duplicates(subset=["来源", "时间", "内容"])
        df_all = df_all.sort_values("时间", ascending=True).reset_index(drop=True)

        new_count = 0
        for _, row in df_all.iterrows():
            uid = make_uid(row)
            if uid in sent_ids:
                continue
            # 去重：是否相似内容已推送
            if is_recently_sent(row["内容"]):
                logger.info(f"[{row['来源']}]相似内容不推送！")
                continue
            sent_ids[uid] = time.time()
            send_rate_limited(row["来源"], row["时间"], row["内容"])
            logger.info(f"[✓] 推送：[{row['来源']}] {row['时间']} - {row['内容'][:120]}...")
            recent_contents.append(normalize_text(row["内容"]))

            new_count += 1
            time.sleep(1)

        logger.info(f"✅ 本轮推送 {new_count} 条")
        save_sent_ids()  # 保存到本地 JSON

        # ---------- 清理 sent_ids ----------
        if loop_count % CLEAN_INTERVAL == 0:
            now = time.time()
            before = len(sent_ids)
            sent_ids = {
                uid: ts for uid, ts in sent_ids.items()
                if now - ts < MAX_AGE
            }
            if len(sent_ids) > MAX_SENT_IDS:
                sent_ids = dict(
                    sorted(sent_ids.items(), key=lambda x: x[1], reverse=True)[:MAX_SENT_IDS]
                )
            after = len(sent_ids)
            logger.info(f"[清理] sent_ids: {before} → {after} (时间窗口 {MAX_AGE/3600:.1f}h, 最大 {MAX_SENT_IDS})")

        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main_loop()
