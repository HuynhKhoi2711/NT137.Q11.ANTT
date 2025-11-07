
import socket
import win32evtlog
import threading
from utils.File_queue import File_queue
def detect_log_level(message):
    message = message.lower()
    if any(x in message for x in ["error", "fail", "critical", "fatal"]):
        return "ERROR"
    elif any(x in message for x in ["warn", "deprecated"]):
        return "WARNING"
    elif any(x in message for x in ["info", "start", "success", "login"]):
        return "INFO"
    else:
        return "DEBUG"

def collect_logs(limit, log_type, hostname):
    logs = []

    handle = win32evtlog.OpenEventLog(None, log_type)
    flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ

    count = 0
    while True:
        events = win32evtlog.ReadEventLog(handle, flags, 0)
        if not events:
            break

        for event in events:
            # Nếu có giới hạn và đã đủ log => dừng
            if limit is not None and count >= limit:
                break

            log_entry = {
                "timestamp": event.TimeGenerated.Format(),
                "source": log_type.lower(),
                "level": detect_log_level(event.SourceName),
                "message": event.StringInserts if event.StringInserts else "",
                "host": hostname,
                "process": event.SourceName,
                "pid": event.EventID,
            }
            logs.append(log_entry)
            count += 1

        if limit is not None and count >= limit:
            break
        
    win32evtlog.CloseEventLog(handle)
    return logs

def collect_worker(limit, log_type, hostname, queue: File_queue):
    logs = collect_logs(limit, log_type, hostname)
    for log in logs:
        queue.push(log)
    print(f"[✓] Collected {len(logs)} logs from {log_type}")

def log_collector(limit=None, log_types="System"):
    hostname = socket.gethostname()
    queue = File_queue("data/logs/window_log.queue")

    threads = []
    for log_type in log_types.split():
        t = threading.Thread(target=collect_worker, args=(limit, log_type, hostname, queue))
        threads.append(t)
        t.start()
        print(f"Collecting from {log_type}...")

    for t in threads:
        t.join()

    print("✅ Collecting completely")
