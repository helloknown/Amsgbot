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
import requests
import akshare as ak
import pandas as pd
import json
import os
from difflib import SequenceMatcher
import hashlib
from collections import deque
import re
from typing import Dict, List, Set, Any
from dataclasses import dataclass
from logger_util import setup_logger

logger = setup_logger("app_log", log_dir="logs", level=logging.INFO)

# ========== 配置 ==========
DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=4bcc16f75f95ee7d0235902664f5bc8bf530285b4a73edc6224d90f15deea0a8"
FETCH_INTERVAL = 30      # 秒
MAX_SENT_IDS = 10000     # 最多保留记录
MAX_AGE = 24 * 3600      # 保留时间（秒）
CLEAN_INTERVAL = 1440    # 每多少轮清理一次(24*(3600/30))
SENT_IDS_FILE = "sent_ids.json"  # 本地持久化文件
RECENT_CONTENTS_FILE = "recent_contents.json"  # 最近内容持久化文件

# 钉钉推送限流
MAX_PER_MINUTE = 20
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
            df = ak.stock_info_global_cls(symbol="重点")
            if df is None or df.empty:
                return []

            news_items = []
            for _, row in df.iterrows():
                timestamp = f"{row['发布日期']} {row['发布时间']}"
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
            df = ak.stock_info_global_sina()
            if df is None or df.empty:
                return []

            news_items = []
            for _, row in df.iterrows():
                timestamp = pd.to_datetime(row["时间"], errors="coerce").strftime("%Y-%m-%d %H:%M:%S")
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


class DingTalkSender:
    """钉钉消息发送器"""

    def __init__(self, webhook: str, max_per_minute: int = 20):
        self.webhook = webhook
        self.max_per_minute = max_per_minute
        self.last_minute = time.time()
        self.sent_count = 0

    def send_message(self, news_item: NewsItem) -> bool:
        """发送消息到钉钉"""
        # 限流控制
        now = time.time()
        if now - self.last_minute >= 60:
            self.last_minute = now
            self.sent_count = 0

        if self.sent_count >= self.max_per_minute:
            sleep_time = 60 - (now - self.last_minute)
            logger.info(f"🚦 达到速率限制，等待 {sleep_time:.1f} 秒")
            time.sleep(sleep_time)
            self.last_minute = time.time()
            self.sent_count = 0

        # 发送消息
        msg = f"📰【{news_item.source}】{news_item.timestamp}\n{news_item.content}"
        payload = {"msgtype": "text", "text": {"content": msg}}

        try:
            response = requests.post(self.webhook, json=payload, timeout=10)
            if response.status_code == 200:
                self.sent_count += 1
                return True
            else:
                logger.error(f"[x] 钉钉推送失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"[x] 钉钉推送异常: {e}")
            return False


class ContentDeduplicator:
    """内容去重器"""

    def __init__(self, recent_contents_file: str, max_recent_contents: int = 1000):
        self.recent_contents_file = recent_contents_file
        self.max_recent_contents = max_recent_contents
        self.recent_contents: deque = deque(maxlen=max_recent_contents)
        self.load_recent_contents()

    def normalize_text(self, text: str) -> str:
        """标准化文本"""
        # 移除HTML标签
        text = re.sub(r"<.*?>", "", text)
        # 移除特殊字符和多余空格
        text = re.sub(r"[^\w\u4e00-\u9fff\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        # 转换为小写
        return text.lower()

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """计算文本相似度"""
        return SequenceMatcher(None, text1, text2).ratio()

    def is_similar_content(self, content: str, threshold: float = 0.8) -> bool:
        """检查内容是否与最近内容相似"""
        normalized = self.normalize_text(content)

        for old_content in self.recent_contents:
            if self.calculate_similarity(normalized, old_content) > threshold:
                return True
        return False

    def add_content(self, content: str):
        """添加内容到最近内容列表"""
        normalized = self.normalize_text(content)
        self.recent_contents.append(normalized)
        self.save_recent_contents()

    def load_recent_contents(self):
        """加载最近内容"""
        if os.path.exists(self.recent_contents_file):
            try:
                with open(self.recent_contents_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.recent_contents = deque(data, maxlen=self.max_recent_contents)
                logger.info(f"[加载] {len(self.recent_contents)} 条最近内容已加载")
            except Exception as e:
                logger.error(f"[x] 加载最近内容文件失败: {e}")
                self.recent_contents = deque(maxlen=self.max_recent_contents)

    def save_recent_contents(self):
        """保存最近内容"""
        try:
            with open(self.recent_contents_file, "w", encoding="utf-8") as f:
                json.dump(list(self.recent_contents), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[x] 保存最近内容文件失败: {e}")


class NewsAggregator:
    """新闻聚合器"""

    def __init__(self):
        self.sent_ids: Dict[str, float] = {}
        self.deduplicator = ContentDeduplicator(RECENT_CONTENTS_FILE)
        self.dingtalk_sender = DingTalkSender(DINGTALK_WEBHOOK, MAX_PER_MINUTE)

        # 注册数据源处理器
        self.processors = [
            CLSProcessor(),
            SinaProcessor(),
            THSProcessor()
        ]

        self.loop_count = 0
        self.load_sent_ids()

    def load_sent_ids(self):
        """加载已发送ID"""
        if os.path.exists(SENT_IDS_FILE):
            try:
                with open(SENT_IDS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # 过滤过期记录
                    now = time.time()
                    self.sent_ids = {uid: ts for uid, ts in data.items() if now - ts < MAX_AGE}
                logger.info(f"[加载] {len(self.sent_ids)} 条历史记录已加载")
            except Exception as e:
                logger.error(f"[x] 加载 sent_ids 文件失败: {e}")
                self.sent_ids = {}
        else:
            self.sent_ids = {}

    def save_sent_ids(self):
        """保存已发送ID"""
        try:
            with open(SENT_IDS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.sent_ids, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[x] 保存 sent_ids 文件失败: {e}")

    def is_duplicate_news(self, news_item: NewsItem) -> bool:
        """检查新闻是否重复"""
        # 检查UID是否已存在
        if news_item.uid in self.sent_ids:
            return True

        # 检查内容是否相似
        if self.deduplicator.is_similar_content(news_item.content):
            logger.info(f"[去重] 相似内容不推送: ({news_item.source}){news_item.content[:100]}...")
            return True

        return False

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
        seen_uids: Set[str] = set()

        for news_item in all_news:
            if news_item.uid not in seen_uids:
                seen_uids.add(news_item.uid)
                unique_news.append(news_item)

        # 按时间排序
        unique_news.sort(key=lambda x: x.timestamp)

        # 发送新新闻
        sent_count = 0
        for news_item in unique_news:
            if not self.is_duplicate_news(news_item):
                if self.dingtalk_sender.send_message(news_item):
                    # 记录已发送
                    self.sent_ids[news_item.uid] = time.time()
                    self.deduplicator.add_content(news_item.content)
                    sent_count += 1
                    logger.info(f"[✓] 推送: [{news_item.source}] {news_item.timestamp} - {news_item.content[:100]}...")

                time.sleep(1)  # 避免发送过快

        return sent_count

    def cleanup_old_records(self):
        """清理旧记录"""
        now = time.time()
        before = len(self.sent_ids)

        # 清理过期记录
        self.sent_ids = {
            uid: ts for uid, ts in self.sent_ids.items()
            if now - ts < MAX_AGE
        }

        # 限制最大数量
        if len(self.sent_ids) > MAX_SENT_IDS:
            self.sent_ids = dict(
                sorted(self.sent_ids.items(), key=lambda x: x[1], reverse=True)[:MAX_SENT_IDS]
            )

        after = len(self.sent_ids)
        if before != after:
            logger.info(f"[清理] sent_ids: {before} → {after} (时间窗口 {MAX_AGE/3600:.1f}h, 最大 {MAX_SENT_IDS})")

    def run(self):
        """运行主循环"""
        logger.info("🚀 启动多源快讯聚合推送系统...")

        while True:
            self.loop_count += 1

            try:
                sent_count = self.process_and_send_news()
                logger.info(f"✅ 第 {self.loop_count} 轮完成，推送 {sent_count} 条新闻")

                # 保存状态
                self.save_sent_ids()

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