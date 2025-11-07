
import os, json, time, threading

class File_queue:
    def __init__(self, queue_path, buffered=True, flush_interval=1.0, max_buffer_size=100):
        self.queue_path = queue_path
        self.lock = threading.Lock()

        # Cấu hình buffer
        self.buffered = buffered            # Nếu False → ghi trực tiếp
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self._buffer = []
        self._last_flush = time.time()

        # Tạo thư mục chứa file queue nếu chưa tồn tại
        queue_dir = os.path.dirname(queue_path)
        if not os.path.exists(queue_dir):
            os.makedirs(queue_dir)


        # Nếu bật buffer → khởi động thread flush
        if self.buffered:
            flush_thread = threading.Thread(target=self._background_flush, daemon=True)
            flush_thread.start()

    # ==== Lock helpers ====
    def _acquire_lock(self):
        self.lock.acquire()

    def _release_lock(self):
        if self.lock.locked():
            self.lock.release()

    # ==== Buffer & Flush ====
    def _flush_buffer(self):
        """Ghi toàn bộ dữ liệu trong buffer xuống file"""
        if not self._buffer:
            return

        self._acquire_lock()
        try:
            with open(self.queue_path, 'a', encoding='utf-8') as f:
                for item in self._buffer:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            self._buffer.clear()
            self._last_flush = time.time()
        finally:
            self._release_lock()

    def _background_flush(self):
        """Luồng nền tự động flush định kỳ"""
        while True:
            time.sleep(self.flush_interval)
            if time.time() - self._last_flush >= self.flush_interval:
                self._flush_buffer()

    # ==== API ====
    def push(self, item):
        """Thêm item vào hàng đợi"""
        if self.buffered:
            self._buffer.append(item)
            if len(self._buffer) >= self.max_buffer_size:
                self._flush_buffer()
        else:
            # Ghi trực tiếp xuống file
            self._acquire_lock()
            try:
                with open(self.queue_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            finally:
                self._release_lock()

    def pop(self):
        """Lấy và xóa phần tử đầu tiên"""
        if not os.path.exists(self.queue_path) or os.path.getsize(self.queue_path) == 0:
            return None

        self._acquire_lock()
        try:
            with open(self.queue_path, 'r+', encoding='utf-8') as f:
                lines = f.readlines()
                if not lines:
                    return None
                first_line = lines[0].strip()
                item = json.loads(first_line)
                f.seek(0)
                f.writelines(lines[1:])
                f.truncate()
            return item
        except (IOError, json.JSONDecodeError) as e:
            print(f"[ERROR] Could not pop from {self.queue_path}: {e}")
            return None
        finally:
            self._release_lock()

    def size(self):
        """Đếm số lượng item trong queue"""
        self._acquire_lock()
        try:
            if not os.path.exists(self.queue_path):
                return 0
            with open(self.queue_path, 'r', encoding='utf-8') as f:
                return len(f.readlines())
        finally:
            self._release_lock()