import os
from pyspark.sql import SparkSession

# Khởi tạo môi trường và cấu hình Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Bai2")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Xử lý đường dẫn linh hoạt
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, "../data")
results_dir = os.path.join(current_dir, "../results")
os.makedirs(results_dir, exist_ok=True)

# Khai báo đường dẫn file
movies_path = f"file://{data_dir}/movies.txt"
users_path = f"file://{data_dir}/users.txt"
ratings_1_path = f"file://{data_dir}/ratings_1.txt"
ratings_2_path = f"file://{data_dir}/ratings_2.txt"
output_log_path = os.path.join(results_dir, "bai3_result.txt")

# CÁC HÀM XỬ LÝ (PARSE) DỮ LIỆU

# Hàm parse để trích xuất MovieID và Title từ dòng dữ liệu movies.txt
def parse_movie_title(line):
    parts = line.split(",", 2)
    movie_id = int(parts[0])
    title = parts[1]
    return (movie_id, title) # Ví dụ: (100, "The Avengers")

# Hàm parse để trích xuất UserID và Gender từ dòng dữ liệu users.txt
def parse_user_gender(line):
    parts = line.split(",", 4)
    user_id = int(parts[0])
    gender = parts[1]
    return (user_id, gender) # Ví dụ: (101, "M")


# TẠO DICTIONARY PHIM (Dùng để tra cứu tên phim)
movies_dict = (
    sc.textFile(movies_path)    # Bước 1 (Transformation): Chia file text cho các Worker đọc.
    .map(parse_movie_title)     # Bước 2 (Transformation): Mỗi Worker tự gọt dòng text thành cặp (ID, Tên phim). Không Shuffle.
    .collectAsMap()             # Bước 3 (Action): Lệnh chốt hạ. Bắt Worker nộp hết kết quả về máy Driver để ghép thành 1 cuốn Từ điển (Dictionary).
)

# TẠO DICTIONARY NGƯỜI DÙNG (Dùng để tra cứu giới tính)
users_dict = (
    sc.textFile(users_path)     # Bước 1: Đọc file text.
    .map(parse_user_gender)     # Bước 2: Gọt dòng text thành cặp (ID, Giới tính).
    .collectAsMap()             # Bước 3: Thu về Driver thành Từ điển.
)

# Phát sóng 2 cuốn từ điển này đến RAM của các Worker
bc_movies = sc.broadcast(movies_dict)
bc_users = sc.broadcast(users_dict)

# ĐỌC RATINGS VÀ THÊM GIỚI TÍNH 
def process_rating(line):
    parts = line.split(",") # Dòng dữ liệu có cấu trúc: UserID, MovieID, Rating, Timestamp
    user_id = int(parts[0])
    movie_id = int(parts[1])
    rating = float(parts[2])
    
    # Lookup thông tin Giới tính từ bộ nhớ cục bộ (Biến Broadcast)
    gender = bc_users.value.get(user_id, "Unknown")
    
    # Định hình cấu trúc RDD: Dùng một Tuple (MovieID, Gender) làm KEY chung để gom nhóm sau này
    # Cấu trúc trả về: ((MovieID, Gender), (Rating, Count))
    # Ví dụ: ((25, 'F'), (4.0, 1))
    return ((movie_id, gender), (rating, 1))

# Phương thức: union() -> Transformation (Không Shuffle, Không tạo Stage mới)
ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))

# Phương thức: map() -> Transformation (Không Shuffle, Không tạo Stage mới)
# Mô tả luồng: Chuỗi String -> ((MovieID, Gender), (Rating, 1))
ratings_mapped = ratings_raw.map(process_rating)

# REDUCE ĐỂ TÍNH TỔNG ĐIỂM THEO PHIM & GIỚI TÍNH
def sum_ratings(val1, val2):
    # val1 và val2 là phần VALUE của những KEY giống nhau (Cùng phim, cùng giới tính)
    # Ví dụ: val1 = (4.0, 1), val2 = (5.0, 1) -> (9.0, 2)
    return (val1[0] + val2[0], val1[1] + val2[1])

# Phương thức: reduceByKey() -> Transformation (CÓ SHUFFLE, TẠO STAGE MỚI)
# Mô tả: Spark buộc phải dừng lại, tìm tất cả dữ liệu có chung Key ((MovieID, Gender)) 
# ở các Node khác nhau, xáo trộn qua mạng (Shuffle) và gom về 1 chỗ để tính toán.
# Cấu trúc sau khi reduce: ((MovieID, Gender), (Total_Rating, Total_Count))
# Ví dụ: ((25, 'F'), (120.5, 30))
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings)

# TÍNH TRUNG BÌNH
def calc_avg(row):
    key, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    # Cấu trúc: ((MovieID, Gender), (Avg_Rating, Total_Count))
    # Ví dụ: ((25, 'F'), (4.01, 30))
    return (key, (avg_rating, total_count))

# Phương thức: map() -> Transformation (Không Shuffle, Không tạo Stage mới)
ratings_avg = ratings_reduced.map(calc_avg)

# KẾT XUẤT VÀ GHI FILE 
print("\nDang xu ly va ghi ket qua, vui long doi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- DIEM TRUNG BINH THEO PHIM VA GIOI TINH ---\n")
    
    # Dictionary dùng để gom gọn kết quả của Nam và Nữ trên cùng 1 dòng
    movie_gender_summary = {}
    
    # Phương thức: toLocalIterator() -> ACTION
    # Mô tả: Chốt sổ DAG, thực thi toàn bộ các Stage và luồng trên cluster, kéo kết quả về Driver.
    for (movie_id, gender), (avg, cnt) in ratings_avg.toLocalIterator():
        if movie_id not in movie_gender_summary:
            # Lookup lấy tên phim từ Dictionary Movies
            title = bc_movies.value.get(movie_id, "Unknown Movie")
            movie_gender_summary[movie_id] = {"title": title, "M": "N/A", "F": "N/A"}
        
        # Gán điểm trung bình vào trường giới tính tương ứng
        movie_gender_summary[movie_id][gender] = f"{avg:.2f}"

    # Sắp xếp và in format ra file
    for movie_id in sorted(movie_gender_summary.keys()):
        data = movie_gender_summary[movie_id]
        line = f"MovieID: {movie_id:<5} | {data['title'][:40]:<40} | Male Avg: {data['M']:<5} | Female Avg: {data['F']:<5}"
        print(line)
        f.write(line + "\n")

print(f"\nHoan tat! Xem toan bo ket qua tai: {output_log_path}")

spark.stop()