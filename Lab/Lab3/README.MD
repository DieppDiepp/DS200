# 📝 Sổ Tay Cài Đặt PySpark trên Ubuntu (WSL) & VS Code

**Thông tin phiên bản đã sử dụng:**
*   **Hệ điều hành:** Ubuntu 24.04 (chạy qua WSL trên Windows)
*   **Java JDK:** OpenJDK 17 (Bắt buộc từ Java 17 trở lên cho Spark 4.x)
*   **PySpark:** 4.1.1
*   **Py4J:** 0.10.9.9
*   **Python:** 3.12.3

---

### Bước 1: Cài đặt Java 17 (Nền tảng chạy Spark)
Spark chạy trên máy ảo Java (JVM), do đó bắt buộc phải cài đặt Java trước tiên.
```bash
# Cập nhật hệ thống và cài đặt OpenJDK 17
sudo apt update
sudo apt install openjdk-17-jdk-headless -y

# Đảm bảo hệ thống đang chọn Java 17 làm mặc định (Chọn số tương ứng với java-17)
sudo update-alternatives --config java

# Kiểm tra lại phiên bản
java -version
```

### Bước 2: Thiết lập môi trường ảo (Virtual Environment)
Ubuntu 24.04 rất khắt khe việc cài thư viện trực tiếp vào hệ thống, nên ta cần tạo môi trường ảo `spark_env`.
```bash
# Cài đặt công cụ pip và venv
sudo apt install python3-pip python3-venv -y

# Tạo môi trường ảo có tên là spark_env (tại thư mục /home/username/)
python3 -m venv spark_env

# Kích hoạt môi trường ảo (Phải thấy chữ (spark_env) ở đầu dòng lệnh)
source ~/spark_env/bin/activate
```

### Bước 3: Cài đặt PySpark (Dùng Mirror để tăng tốc)
Do PySpark rất nặng (~455MB), tải từ server gốc hay bị lỗi mạng (Hash mismatch). Khắc phục bằng cách dùng tham số `--no-cache-dir` và trỏ sang Mirror của Aliyun.
```bash
# Đảm bảo đang ở trong (spark_env) trước khi chạy lệnh này
pip install --no-cache-dir pyspark -i https://mirrors.aliyun.com/pypi/simple/
```

### Bước 4: Cấu hình VS Code để code PySpark
1. Mở thư mục code bằng lệnh: `code .`
2. **Cài Extension:** Mở tab Extensions (`Ctrl + Shift + X`), tìm và cài đặt tiện ích **Python** (của Microsoft) trên WSL.
3. **Trỏ đường dẫn Interpreter (Rất quan trọng):**
   * Nhấn `Ctrl + Shift + P` -> Gõ và chọn `Python: Select Interpreter`.
   * Chọn `Enter interpreter path...`
   * Nhập đường dẫn tuyệt đối (không dùng dấu `~` để tránh lỗi): `/home/nguyen/spark_env/bin/python`
4. Mở terminal mới trong VS Code (`Ctrl + \``) và luôn nhớ kích hoạt môi trường trước khi chạy code: `source ~/spark_env/bin/activate`.

---

### 🚀 Snippet Code Test Nhanh (Hello World)
Dùng đoạn code này để test xem cụm Spark đã hoạt động trơn tru chưa.

```python
from pyspark.sql import SparkSession

# 1. Khởi tạo Spark Session
spark = SparkSession.builder.master("local[*]").appName("Test_Spark").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 2. Xử lý dữ liệu cơ bản
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
rdd_mapped = rdd.map(lambda x: x * 2)

# 3. Action và in kết quả
ket_qua = rdd_mapped.collect()
print("Kết quả sau khi nhân đôi:", ket_qua)

# 4. Đóng Spark
spark.stop()
```

---

### Bước 5: Xem Giao Diện Spark Web UI (Localhost:4040)
Spark cung cấp một giao diện web trực quan để theo dõi luồng xử lý. Để giữ Web UI không bị tắt ngay sau khi code chạy xong, thêm dòng này vào trước `spark.stop()`:
```python
input("Mở http://localhost:4040 để xem UI. Nhấn Enter để kết thúc...")
```

---

## 🧠 Phần Mở Rộng: Hiểu Sâu Kiến Trúc Thực Thi Của Spark (Từ Code đến Web UI)

Để làm chủ Spark, cần phân biệt rõ cơ chế "Lười biếng" (Lazy Evaluation) và cách Spark phân chia công việc từ lớn đến nhỏ: **Job ➔ Stage ➔ Task**.

### 1. Phân biệt Transformations và Actions (Bản thiết kế vs. Thi công)
*   **Transformations (Lệnh "Ghi nợ" - Chưa thực thi):**
    *   Gồm các hàm như `map()`, `filter()`, `reduceByKey()`.
    *   Khi gọi các hàm này, Spark **không xử lý dữ liệu thật**. Nó đóng vai trò như kiến trúc sư, chỉ ghi chú lại các bước để vẽ ra một bản thiết kế tổng thể gọi là **DAG** (Đồ thị luân chuyển có hướng).
    *   **Mục đích:** Để tối ưu hóa đường đi (ví dụ: gộp các bước lọc và tính toán lại làm một trước khi chạy thật).
*   **Actions (Lệnh "Chốt hạ" - Thực thi):**
    *   Gồm các hàm như `collect()`, `count()`, `toLocalIterator()`, `saveAsTextFile()`.
    *   Khi gặp Action, Spark mới chốt bản thiết kế DAG và ra lệnh cho máy móc bắt tay vào chạy thật.

### 2. Job = 1 Lệnh Action
*   **Định nghĩa:** Cứ mỗi một lệnh Action được gọi, Spark sẽ đẩy một **Job** lên hệ thống.
*   **Liên hệ Spark UI:** Trong Tab *Jobs*, `Job Id 0` được sinh ra chính là do lệnh `collect()` được gọi ở dòng 19 trong file `test_spark.py`. Thời gian chạy (Duration) mất đúng 1 giây.

### 3. Stage & Vách ngăn Shuffle (Trao đổi chéo)
*   **Định nghĩa:** Một Job được chia thành nhiều **Stage** (Giai đoạn). Spark cắt ra một Stage mới khi dữ liệu cần phải xáo trộn và trao đổi chéo giữa các luồng CPU/Worker với nhau (gọi là quá trình **Shuffle**).
*   **Narrow Dependency (Cùng 1 Stage):** Nếu dùng `map()`, `filter()`, luồng nào xử lý data của luồng đó từ trên xuống dưới, không cần chờ nhau.
*   **Wide Dependency (Đẻ ra Stage mới):** Nếu dùng `reduceByKey()`, hệ thống phải dừng lại, gom chung các Key giống nhau đang nằm rải rác ở các luồng khác nhau về một mối để cộng gộp. Sự trao đổi chéo này cắt Job ra thành Stage mới.
*   **Liên hệ Spark UI:** Trong Tab *Stages*, bài test `map` nhân đôi số không hề có Shuffle. Do đó, toàn bộ quá trình trơn tru này được gom hết vào đúng **1 Stage (Stage 0)**. 

### 4. Task (Đơn vị nhỏ nhất) & Executor (Cỗ máy)
*   **Task:** Trong một Stage, dữ liệu được chặt thành nhiều khối nhỏ (Partition). Mỗi khối giao cho 1 luồng CPU xử lý, gọi là 1 **Task**.
*   **Executor:** Môi trường thực thi chứa RAM và CPU. Ở chế độ local, máy tính của bạn chính là Executor (Driver).
*   **Liên hệ Spark UI:** 
    *   Trong Tab *Executors*, hệ thống ghi nhận máy tính cấp cho Spark **16 Cores** (16 luồng xử lý).
    *   Vì Spark rất thông minh trong việc ép phần cứng chạy tối đa, nó tự động chia dữ liệu thành đúng **16 Tasks** (nhìn ở bảng Summary Metrics) để cả 16 luồng CPU cùng làm việc song song cùng một lúc.

---