"""
多源快讯聚合推送 实时快讯推送（akshare）
- 统一格式化输出
- 钉钉推送
- 自动去重（sent_ids + 内容相似度）
- 本地 JSON 持久化，程序重启仍可避免重复推送
- 定期清理旧记录
依赖: pip install akshare requests pandas
"""
import logging
import time
from datetime import datetime, timezone, timedelta
import stock_info as ak
import pandas as pd
import json
import os
from difflib import SequenceMatcher
import hashlib
from collections import deque
import re
from typing import Dict, List, Set, Any
from dataclasses import dataclass

from dingTalk import DingTalkDispatcher
from logger_util import setup_logger

logger = setup_logger("app_log", log_dir="logs", level=logging.INFO)

# ========== 配置 ==========
FETCH_INTERVAL = 30      # 秒
MAX_SENT_IDS = 10000     # 最多保留记录
MAX_AGE = 24 * 3600      # 保留时间（秒）
CLEAN_INTERVAL = 1440    # 每多少轮清理一次(24*(3600/30))
STATE = "data/state.json"  # 最近内容持久化文件
DATA_FILE = "data/messages.json"

# ==========================

@dataclass
class NewsItem:
    """新闻项数据类"""
    source: str
    timestamp: str
    content: str
    uid: str = ""

    def __post_init__(self):
        if not self.uid:
            self.uid = self.generate_uid()

    def generate_uid(self) -> str:
        """生成唯一ID"""
        key = f"{self.source}_{self.timestamp}_{self.content}"
        return hashlib.md5(key.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "content": self.content,
            "uid": self.uid
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NewsItem':
        return cls(
            source=data["source"],
            timestamp=data["timestamp"],
            content=data["content"],
            uid=data.get("uid", "")
        )


class NewsProcessor:
    """新闻处理器基类"""

    def __init__(self, source_name: str):
        self.source_name = source_name

    def fetch_news(self) -> List[NewsItem]:
        """获取新闻数据，子类需实现"""
        raise NotImplementedError


class CLSProcessor(NewsProcessor):
    """财联社处理器"""

    def __init__(self):
        super().__init__("财联社")

    def fetch_news(self) -> List[NewsItem]:
        try:
            logger.info("[财联社] -> 获取信息开始")
            # df = ak.stock_info_global_cls(symbol="全部")
            df = ak.stock_info_global_cls(symbol="重点")
            if df is None or df.empty:
                return []

            news_items = []
            for _, row in df.iterrows():
                timestamp = pd.to_datetime(f"{row['发布日期']} {row['发布时间']}").strftime("%Y-%m-%d %H:%M:%S")
                content = f"{row['标题']}：{row['内容']}" if pd.notna(row['标题']) and str(row['标题']).strip() else str(row['内容'])
                news_items.append(NewsItem(
                    source=self.source_name,
                    timestamp=timestamp,
                    content=content
                ))
            return news_items
        except Exception as e:
            logger.error(f"[x] 财联社数据处理失败: {e}")
            return []


class SinaProcessor(NewsProcessor):
    """新浪财经处理器"""

    def __init__(self):
        super().__init__("新浪财经")

    def fetch_news(self) -> List[NewsItem]:
        try:
            logger.info("[新浪财经] -> 获取信息开始")
            df = ak.stock_info_global_sina()
            if df is None or df.empty:
                return []

            news_items = []
            for _, row in df.iterrows():
                timestamp = pd.to_datetime(row["时间"]).strftime("%Y-%m-%d %H:%M:%S")
                content = str(row["内容"])
                news_items.append(NewsItem(
                    source=self.source_name,
                    timestamp=timestamp,
                    content=content
                ))
            return news_items
        except Exception as e:
            logger.error(f"[x] 新浪财经数据处理失败: {e}")
            return []


class THSProcessor(NewsProcessor):
    """同花顺处理器"""

    def __init__(self):
        super().__init__("同花顺")

    def fetch_news(self) -> List[NewsItem]:
        try:
            logger.info("[同花顺] -> 获取信息开始")
            df = ak.stock_info_global_ths()
            if df is None or df.empty:
                return []

            news_items = []
            for _, row in df.iterrows():
                local_dt = datetime.strptime(row["发布时间"], "%Y-%m-%d %H:%M:%S")
                timestamp = int(local_dt.timestamp())
                timestamp = datetime.fromtimestamp(timestamp, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
                content = f"{row['标题']}：{row['内容']}" if pd.notna(row['标题']) and str(row['标题']).strip() else str(row['内容'])
                news_items.append(NewsItem(
                    source=self.source_name,
                    timestamp=timestamp,
                    content=content
                ))
            return news_items
        except Exception as e:
            logger.error(f"[x] 同花顺数据处理失败: {e}")
            return []

class StateManager:
    """统一状态管理器，合并 sent_ids + recent_contents"""

    def __init__(self, state_file: str, max_sent_ids: int = 10000, max_recent: int = 1000):
        self.state_file = state_file
        self.max_sent_ids = max_sent_ids
        self.max_recent = max_recent
        self.sent_ids: Dict[str, float] = {}
        self.recent_contents: deque = deque(maxlen=max_recent)
        self.load()

    def load(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.sent_ids = data.get("sent_ids", {})
                    recent = data.get("recent_contents", [])
                    self.recent_contents = deque(recent, maxlen=self.max_recent)
                    logger.info(f"[加载] 状态文件成功: sent={len(self.sent_ids)}, recent={len(self.recent_contents)}")
            except Exception as e:
                logger.error(f"[x] 加载状态文件失败: {e}")

    def save(self):
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump({
                    "sent_ids": self.sent_ids,
                    "recent_contents": list(self.recent_contents)
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[x] 保存状态文件失败: {e}")

    def add_sent(self, uid: str):
        self.sent_ids[uid] = time.time()
        # 限制数量
        if len(self.sent_ids) > self.max_sent_ids:
            # 按时间保留最新
            self.sent_ids = dict(sorted(self.sent_ids.items(), key=lambda x: x[1], reverse=True)[:self.max_sent_ids])

    def clean_old(self, max_age: int):
        now = time.time()
        before = len(self.sent_ids)
        self.sent_ids = {uid: ts for uid, ts in self.sent_ids.items() if now - ts < max_age}
        after = len(self.sent_ids)
        if before != after:
            logger.info(f"[清理] sent_ids: {before} → {after}")

    def normalize_text(self, text: str) -> str:
        text = re.sub(r"<.*?>", "", text)
        text = re.sub(r"[^\w\u4e00-\u9fff\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """计算文本相似度"""
        return SequenceMatcher(None, text1, text2).ratio()

    def is_similar(self, text: str, threshold=0.8) -> bool:
        normalized = self.normalize_text(text)
        for old in self.recent_contents:
            if self.calculate_similarity(normalized, old) > threshold:
                return True
        return False

    def add_recent(self, text: str):
        normalized = self.normalize_text(text)
        self.recent_contents.append(normalized)

class NewsAggregator:
    """新闻聚合器"""

    def __init__(self):
        self.state  = StateManager(STATE)
        self.dingtalk_sender = DingTalkDispatcher()
        # 注册数据源处理器
        self.processors = [
            CLSProcessor(),
            SinaProcessor(),
            THSProcessor()
        ]
        self.loop_count = 0

    def save_message_to_local(self, news: NewsItem):
        os.makedirs("data", exist_ok=True)
        messages = []

        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                try:
                    messages = json.load(f)
                except json.JSONDecodeError:
                    messages = []

        new_item = {
            "id": news.uid,
            "time": news.timestamp,
            "source": news.source,
            "content": news.content
        }
        # 插入到最前面
        messages.insert(0, new_item)
        # 只保留最新 1000 条
        messages = messages[:1000]

        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)

    def safe_parse_time(self, ts_str: str) -> float:
        """将时间字符串安全转换为时间戳（统一为北京时间）"""
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            # 确保按北京时间 (+8)
            return dt.replace(tzinfo=timezone(timedelta(hours=8))).timestamp()
        except Exception:
            return 0.0

    def process_and_send_news(self):
        """处理并发送新闻"""
        all_news: List[NewsItem] = []

        # 从所有数据源获取新闻
        for processor in self.processors:
            try:
                news_items = processor.fetch_news()
                all_news.extend(news_items)
                logger.info(f"[{processor.source_name}] 获取到 {len(news_items)} 条新闻")
            except Exception as e:
                logger.error(f"[x] {processor.source_name} 获取失败: {e}")

        if not all_news:
            logger.info("本轮无新闻数据")
            return 0

        # 去重并排序
        unique_news = []
        seen: Set[str] = set()

        for news_item in all_news:
            if news_item.uid not in seen:
                seen.add(news_item.uid)
                unique_news.append(news_item)

        # 按时间排序
        unique_news.sort(key=lambda x: self.safe_parse_time(x.timestamp))

        # 发送新新闻
        sent_count = 0
        for news_item in unique_news:
            if news_item.uid in self.state.sent_ids:
                continue
            if self.state.is_similar(news_item.content):
                logger.info(f"[去重] 相似内容跳过: {news_item.content[:80]}...")
                continue

            self.save_message_to_local(news_item)
            self.state.add_sent(news_item.uid)
            self.state.add_recent(news_item.content)
            self.dingtalk_sender.enqueue_message(news_item)
            sent_count += 1
            logger.info(f"[✓] 推送: [{news_item.source}] {news_item.timestamp} - {news_item.content[:100]}...")

            time.sleep(1)  # 避免发送过快

        # 保存状态
        self.state.save()
        return sent_count

    def cleanup_old_records(self):
        """清理旧记录"""
        self.state.clean_old(MAX_AGE)
        self.state.save()

    def run(self):
        """运行主循环"""
        logger.info("🚀 启动多源快讯聚合推送系统...")

        while True:
            self.loop_count += 1

            try:
                sent_count = self.process_and_send_news()
                logger.info(f"✅ 第 {self.loop_count} 轮完成，推送 {sent_count} 条新闻")

                # 定期清理
                if self.loop_count % CLEAN_INTERVAL == 0:
                    self.cleanup_old_records()

            except Exception as e:
                logger.error(f"[x] 主循环异常: {e}")

            time.sleep(FETCH_INTERVAL)

def main():
    """主函数"""
    aggregator = NewsAggregator()
    aggregator.run()


if __name__ == "__main__":
    main()