             +-------------------------------------------------+
             |           📥 1. LỚP THU THẬP                     |
             |-------------------------------------------------|
             | Mục tiêu: Thu thập dữ liệu thô, đảm bảo tin cậy. |
             | Modules:                                        |
             | - Module Thu thập Log (File, Event Log, Syslog) |
             | - Module Thu thập qua API (Cloud, SaaS)         |
             | - Module Thu thập Metadata Mạng                 |
             | - Module Phân tích Mạng (Packet/NIDS)           |
             +-----------------------+-------------------------+
                                     |
                                     v
             +-------------------------------------------------+
             |           🚦 2. LỚP HÀNG CHỞ                   |
             |-------------------------------------------------|
             | Mục tiêu: Đệm dữ liệu, tách rời các lớp xử lý.   |
             | Modules:                                        |
             | - Module Phân tích cú pháp (Parsing)            |
             | - Module Hàng chờ File (Lưu trữ tạm)            |
             +-----------------------+-------------------------+
                                     |
                                     v
             +-------------------------------------------------+
             |      ⚙️ 3. LỚP XỬ LÝ & LÀM GIÀU                 |
             |-------------------------------------------------|
             | Mục tiêu: Biến đổi dữ liệu thành dạng có cấu     |
             |           trúc, đồng nhất và giàu thông tin.     |
             | Modules:                                        |
             | - Module Chuẩn hóa (Normalization)              |
             | - Module Làm giàu (Enrichment - GeoIP, TI)      |
             +-----------------------+-------------------------+
                                     |
                                     v
             +-------------------------------------------------+
             |      🧠 4. LỚP PHÁT HIỆN & PHÂN TÍCH             |
             |-------------------------------------------------|
             | Mục tiêu: "Bộ não" - Tự động phân tích dữ liệu  |
             |           để tìm dấu hiệu độc hại (in-line).    |
             | Modules:                                        |
             | - Bộ máy chạy Quy tắc (Rule Engine)              |
             | - Bộ máy Tương quan Sự kiện (Correlation Engine)|
             +-----------------------+-------------------------+
                                     |
                                     v
             +-------------------------------------------------+
             |           🗃️ 5. LỚP LƯU TRỮ                      |
             |-------------------------------------------------|
             | Mục tiêu: Lưu trữ dữ liệu (và kết quả phát hiện)|
             |           để phục vụ truy vấn và phân tích.     |
             | Modules:                                        |
             | - CSDL Tìm kiếm & Phân tích (NoSQL)             |
             | - Module Lập chỉ mục (Indexing)                 |
             | - Module Quản lý Vòng đời Dữ liệu               |
             +-----------------------+-------------------------+
                                     |
                                     v
             +-------------------------------------------------+
             |           📈 6. LỚP TRỰC QUAN HÓA               |
             |-------------------------------------------------|
             | Mục tiêu: Cung cấp giao diện (GUI) để giám sát, |
             |           khám phá và điều tra (bao gồm cả      |
             |           hiển thị các phát hiện từ Lớp 4).      |
             | Modules:                                        |
             | - Module Bảng điều khiển (Dashboard)             |
             | - Module Trực quan hóa Dữ liệu                  |
             | - Giao diện Truy vấn (Query Interface)          |
             +-------------------------------------------------+