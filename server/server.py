import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import json

app = FastAPI()

# ==== 静态文件目录设置 ====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "web")
DATA_FILE = os.path.join(os.path.dirname(BASE_DIR), "data", "messages.json")

app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
async def index():
    """返回首页 HTML"""
    return FileResponse(os.path.join(WEB_DIR, "index.html"))


@app.get("/api/messages")
async def get_messages(skip: int = 0, limit: int = 100):
    """返回消息数据，支持懒加载分页"""
    if not os.path.exists(DATA_FILE):
        return JSONResponse([])

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 最新的在前面
    data = list(data)
    sliced = data[skip:skip + limit]
    return JSONResponse(sliced)


if __name__ == "__main__":
    # uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
    # 从环境变量或命令行读取端口
    import argparse

    parser = argparse.ArgumentParser(description="启动财聚合快讯网页服务")
    parser.add_argument("--host", default="0.0.0.0", help="绑定主机地址（默认 0.0.0.0）")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", 8000)), help="端口号（默认 8000）")
    args = parser.parse_args()

    uvicorn.run("server:app", host=args.host, port=args.port, reload=True)
