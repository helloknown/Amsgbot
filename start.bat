@echo off
echo 启动 main.py（抓取程序）...
start "" python main.py

echo 启动 FastAPI 服务...
uvicorn server.server:app --host 0.0.0.0 --port 8000 --reload
pause
