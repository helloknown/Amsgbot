# logger_util.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time

def setup_logger(name="quant_news", log_dir="logs", level=logging.INFO):
    """
    初始化日志记录器
    :param name: 日志名称（用于多模块区分）
    :param log_dir: 日志文件目录
    :param level: 日志级别
    :return: logger 对象
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # 防止重复打印

    # --- 控制台输出 ---
    console_handler = logging.StreamHandler()
    console_fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    # --- 文件输出（每天切分） ---
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, f"{name}.log"),
        when="midnight",  # 每天0点切分
        backupCount=7,    # 保留7天
        encoding="utf-8"
    )

    # 设置为北京时间（UTC+8）
    logging.Formatter.converter = lambda *args: time.localtime(time.time() + 8 * 3600)
    file_fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    return logger
