import time
import threading
import queue
import requests
import logging

logger = logging.getLogger("app_log")

# 钉钉推送限流
MAX_PER_MINUTE = 20
DINGTALK_WEBHOOK = ["https://oapi.dingtalk.com/robot/send?access_token=1fbfdec75b3fc790be76f0ed78829ba37a0df233bd5319bfe95fa9cde0dpopnd9"]

class DingTalkSender:
    """单个钉钉机器人发送器（带独立限流）"""

    def __init__(self, webhook: str, max_per_minute: int = 20):
        self.webhook = webhook
        self.max_per_minute = max_per_minute
        self.last_minute = time.time()
        self.sent_count = 0
        self.lock = threading.Lock()

    def can_send(self) -> bool:
        """判断是否可以发送"""
        with self.lock:
            now = time.time()
            if now - self.last_minute >= 60:
                self.last_minute = now
                self.sent_count = 0
            return self.sent_count < self.max_per_minute

    def send_message(self, news_item):
        """发送消息"""
        msg = f"📰【{news_item.source}】{news_item.timestamp}\n{news_item.content}"
        payload = {"msgtype": "text", "text": {"content": msg}}

        try:
            response = requests.post(self.webhook, json=payload, timeout=10)
            if response.status_code == 200:
                with self.lock:
                    self.sent_count += 1
                logger.info(f"✅ [{self.webhook[-6:]}] 推送成功：{news_item.source}")
                return True
            else:
                logger.error(f"[x] 钉钉推送失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"[x] 钉钉推送异常: {e}")
            return False


class DingTalkDispatcher:
    """钉钉消息调度器：管理多个机器人并限流发送"""

    def __init__(self, webhooks=None, max_per_minute: int = 20, monitor_interval: int = 30):
        if webhooks is None:
            webhooks = DINGTALK_WEBHOOK
        self.senders = [DingTalkSender(w, max_per_minute) for w in webhooks]
        self.msg_queue = queue.Queue()
        self.stop_flag = False
        self.index = 0  # 用于轮询
        self.monitor_interval = monitor_interval
        # 启动工作线程
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self.monitor_thread.start()

    def enqueue_message(self, news_item):
        """添加消息到总队列"""
        self.msg_queue.put(news_item)
        qsize = self.msg_queue.qsize()
        if qsize > 100:
            logger.warning(f"⚠️ 队列积压严重：当前 {qsize} 条消息未发送！")
        logger.debug(f"📩 已入队：{news_item}")

    def _get_next_sender(self):
        """轮询选出下一个可用的 sender"""
        for _ in range(len(self.senders)):
            sender = self.senders[self.index]
            self.index = (self.index + 1) % len(self.senders)
            if sender.can_send():
                return sender
        return None

    def _worker(self):
        """后台发送线程"""
        while not self.stop_flag:
            try:
                news_item = self.msg_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.5)
                continue

            sender = self._get_next_sender()
            if sender:
                ok = sender.send_message(news_item)
                if ok:
                    qsize = self.msg_queue.qsize()
                    logger.info(f"✅ 推送成功 | 队列剩余：{qsize}")
                else:
                    logger.error("❌ 推送失败，消息重新入队")
                    self.msg_queue.put(news_item)
            else:
                # 所有机器人都达上限，等待下分钟
                logger.warning("🚦 所有机器人都到达速率限制，等待 10 秒后重试")
                time.sleep(10)
                self.msg_queue.put(news_item)  # 重新放回队列

            self.msg_queue.task_done()

    def _monitor(self):
        """定期输出队列和机器人状态"""
        while not self.stop_flag:
            time.sleep(self.monitor_interval)
            statuses = []
            for i, s in enumerate(self.senders, 1):
                statuses.append(f"R{i}:{s.sent_count}/min")
            qsize = self.msg_queue.qsize()
            logger.info(f"📊 队列监控 | 剩余：{qsize} | {' | '.join(statuses)}")

            if qsize > 200:
                logger.warning(f"⚠️ 队列严重积压：{qsize} 条，请检查钉钉发送是否受限！")

    def stop(self):
        """安全停止"""
        self.stop_flag = True
        self.thread.join(timeout=3)
        logger.info("🛑 调度器已停止")
