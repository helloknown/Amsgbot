import multiprocessing
import os
import time
import sys
import uvicorn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_PORT = 8000


def run_main():
    print("[main] 启动中...")
    sys.path.insert(0, BASE_DIR)
    import main
    main.main()  # ✅ 直接调用 main() 函数


def run_server():
    print(f"[server] 启动中 (端口 {SERVER_PORT}) ...")
    uvicorn.run("server.server:app", host="0.0.0.0", port=SERVER_PORT, reload=False)


if __name__ == "__main__":
    print("🚀 启动 聚合财经快讯系统 ...")
    main_proc = multiprocessing.Process(target=run_main)
    server_proc = multiprocessing.Process(target=run_server)

    main_proc.start()
    time.sleep(2)
    server_proc.start()

    print(f"✅ 系统运行中：http://127.0.0.1:{SERVER_PORT}\n按 Ctrl+C 退出。")

    try:
        main_proc.join()
        server_proc.join()
    except KeyboardInterrupt:
        print("\n🛑 终止中...")
        main_proc.terminate()
        server_proc.terminate()
