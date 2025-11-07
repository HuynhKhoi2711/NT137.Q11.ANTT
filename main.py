from modules.collector.logs_window_collector import log_collector
from utils.File_queue import File_queue
if __name__ == "__main__":
    logs = log_collector(None,"System Application Network KHOIHUYNH")
    print(f"Số lượng item trong hàng đợi: {File_queue("data/logs/window_log.queue").size()}") 
