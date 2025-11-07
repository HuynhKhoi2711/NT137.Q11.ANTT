# network_collector.py
# Module dùng Scapy để bắt gói tin mạng, xử lý và ghi vào File_queue
# Không dùng class, tận dụng File_queue đã có sẵn

from scapy.all import sniff, IP, TCP, UDP, DNS, DNSQR, Raw  # Thư viện Scapy
import time
import threading
from utils.File_queue import File_queue  # Queue thread-safe, hỗ trợ buffer

# ======================
# Biến toàn cục
# ======================
queue = None          # File_queue toàn cục
running = False       # Flag để dừng capture
packet_count = 0      # Đếm tổng số gói đã capture

# ======================
# Hàm xử lý gói tin
# ======================
def _process_packet(pkt, batch, batch_size):
    """
    Xử lý 1 gói tin Scapy, chuyển thành dict log và thêm vào batch.
    Khi batch đủ batch_size sẽ trả về True để flush vào File_queue.

    Args:
        pkt: gói tin Scapy
        batch: danh sách tạm lưu log
        batch_size: số lượng log tối đa trong batch trước khi flush

    Returns:
        True nếu batch đầy, False nếu chưa
    """
    log = {}
    try:
        if IP in pkt:  # Nếu gói có IP
            # Thông tin cơ bản
            log["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            log["src_ip"] = pkt[IP].src
            log["dst_ip"] = pkt[IP].dst
            log["length"] = len(pkt)

            # Xác định giao thức
            if TCP in pkt:
                log["protocol_name"] = "TCP"
                log["src_port"] = pkt[TCP].sport
                log["dst_port"] = pkt[TCP].dport
            elif UDP in pkt:
                log["protocol_name"] = "UDP"
                log["src_port"] = pkt[UDP].sport
                log["dst_port"] = pkt[UDP].dport
            else:
                log["protocol_name"] = "OTHER"
                log["src_port"] = None
                log["dst_port"] = None

            # Xử lý DNS query
            if DNS in pkt and pkt.haslayer(DNSQR):
                log["dns_query"] = pkt[DNSQR].qname.decode() if pkt[DNSQR].qname else None

            # Xử lý HTTP-like payload
            if TCP in pkt and pkt.haslayer(Raw):
                payload = pkt[Raw].load
                try:
                    payload_str = payload.decode(errors='ignore')  # Convert byte → string
                    if "HTTP" in payload_str:
                        lines = payload_str.splitlines()
                        for line in lines:
                            if line.lower().startswith("host:"):
                                log["http_host"] = line.split(":",1)[1].strip()
                            elif line.lower().startswith("user-agent:"):
                                log["http_user_agent"] = line.split(":",1)[1].strip()
                except Exception:
                    pass  # Bỏ qua nếu payload không decode được

        # Nếu log có dữ liệu → thêm vào batch
        if log:
            batch.append(log)
        return len(batch) >= batch_size  # True nếu batch đầy
    except Exception as e:
        print(f"[ERROR] process_packet: {e}")
        return False

# ======================
# Hàm bắt gói tin
# ======================
def start_capture(interface, queue_path, batch_size, limit, bpf_filter):
    """
    Bắt gói tin mạng và push log vào File_queue theo batch.

    Args:
        interface: tên interface (ví dụ 'Ethernet'), None để tự chọn
        queue_path: đường dẫn file queue
        batch_size: số log tối đa 1 batch trước khi flush
        limit: số gói tối đa để bắt, None = vô hạn
        bpf_filter: filter BPF cho sniff (ví dụ "tcp or udp")
    """
    global queue, running, packet_count

    # Khởi tạo File_queue
    queue = File_queue(queue_path)
    batch = []
    running = True
    packet_count = 0

    print(f"[*] Bắt gói tin trên interface: {interface or 'auto-detect'} với filter: {bpf_filter}")

    # Callback cho sniff
    def _sniff_callback(pkt):
        nonlocal batch
        global packet_count
        packet_count += 1

        # Xử lý packet và kiểm tra batch có cần flush không
        flush = _process_packet(pkt, batch, batch_size=batch_size)

        if flush or (limit and packet_count >= limit):
            # Push tất cả log batch vào File_queue
            for item in batch:
                queue.push(item)
            batch.clear()
            # Nếu đạt limit → dừng capture
            if limit and packet_count >= limit:
                global running
                running = False

    # Bắt đầu sniff
    sniff(
        iface=interface,
        prn=_sniff_callback,    # Callback xử lý gói
        store=False,            # Không lưu gói trong memory
        filter=bpf_filter,      # Lọc gói theo BPF
        stop_filter=lambda x: not running  # Dừng khi flag = False
    )

    # Ghi batch còn lại nếu có
    if batch:
        for item in batch:
            queue.push(item)

    print(f"[+] Dừng bắt gói tin. Tổng số gói đã ghi: {packet_count}")
    print(f"[+] Số lượng log trong queue: {queue.size()}")

# ======================
# Hàm dừng capture
# ======================
def stop_capture():
    """
    Dừng bắt gói tin bằng cách set running = False
    """
    global running
    running = False


def network_collector(interface=None, queue_path="./data/logs/network_packets.queue",batch_size=100,limit=None,bpf_filter=""):
    thread = threading.Thread(target=start_capture,  args=(interface,queue_path,batch_size,limit,bpf_filter))
    thread.start()
    try:
        while thread.is_alive():
            pass
    except:
        stop_capture()
    
    print("Hoàn tất capture.")