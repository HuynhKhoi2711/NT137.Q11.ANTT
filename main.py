from modules.collector.logs_window_collector import collect_windows_logs
from utils.File_queue import File_queue
if __name__ == "__main__":
    logs = collect_windows_logs()
    queue = File_queue("./data/logs/window_log.queue")
    queue.push(logs)
    print(f"Số lượng item trong hàng đợi: {queue.size()}") 
