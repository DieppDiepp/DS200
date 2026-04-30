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
output_log_path = os.path.join(results_dir, "bai4_result.txt")


# CÁC HÀM XỬ LÝ (PARSE) DỮ LIỆU

def parse_movie_title(line):
    parts = line.split(",", 2)
    return (int(parts[0]), parts[1]) # (MovieID, Title)

def classify_age_group(age):
    """Hàm phụ trợ để phân loại tuổi thành các nhóm"""
    if age < 18: return "<18"
    elif age <= 24: return "18-24"
    elif age <= 34: return "25-34"
    elif age <= 44: return "35-44"
    elif age <= 49: return "45-49"
    elif age <= 55: return "50-55"
    else: return "56+"

def parse_user_age_group(line):
    parts = line.split(",", 4)
    user_id = int(parts[0])
    age = int(parts[2]) # Tuổi nằm ở cột thứ 3 (index 2)
    
    age_group = classify_age_group(age)
    return (user_id, age_group) # (UserID, AgeGroup) Ví dụ: (101, "25-34")


# TẠO DICTIONARY VÀ BROADCAST (Thay thế Join)

# TẠO DICTIONARY PHIM
movies_dict = (
    sc.textFile(movies_path)    # Bước 1 (Transformation)
    .map(parse_movie_title)     # Bước 2 (Transformation): Chuyển thành Pair RDD (MovieID, Title)
    .collectAsMap()             # Bước 3 (Action): Kéo về Driver làm Dictionary
)

# TẠO DICTIONARY NHÓM TUỔI
users_age_dict = (
    sc.textFile(users_path)     # Bước 1 (Transformation)
    .map(parse_user_age_group)  # Bước 2: Chuyển thành Pair RDD (UserID, AgeGroup)
    .collectAsMap()             # Bước 3 (Action): Kéo về Driver làm Dictionary
)

# Phát sóng (Broadcast) 2 cuốn từ điển
bc_movies = sc.broadcast(movies_dict)
bc_users_age = sc.broadcast(users_age_dict)


# ĐỌC RATINGS VÀ THÊM NHÓM TUỔI
def process_rating(line):
    parts = line.split(",")
    user_id = int(parts[0])
    movie_id = int(parts[1])
    rating = float(parts[2])
    
    # Lookup thông tin Nhóm tuổi từ Broadcast Variable
    age_group = bc_users_age.value.get(user_id, "Unknown")
    
    # Dùng Tuple (MovieID, AgeGroup) làm KEY
    # Cấu trúc: ((MovieID, AgeGroup), (Rating, Count))
    return ((movie_id, age_group), (rating, 1)) # Ví dụ: ((25, '25-34'), (4.0, 1))

# Phương thức: union() & map() -> Transformation (Không Shuffle)
ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))
ratings_mapped = ratings_raw.map(process_rating)


# REDUCE TÍNH TỔNG VÀ TÍNH TRUNG BÌNH
def sum_ratings(val1, val2):
    return (val1[0] + val2[0], val1[1] + val2[1])

# Phương thức: reduceByKey() -> Transformation (CÓ SHUFFLE, TẠO STAGE MỚI)
# Cấu trúc: ((MovieID, AgeGroup), (Total_Rating, Total_Count))
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings)

def calc_avg(row):
    key, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    # Cấu trúc: ((MovieID, AgeGroup), (Avg_Rating, Total_Count))
    return (key, (avg_rating, total_count))

# Phương thức: map() -> Transformation (Không Shuffle)
ratings_avg = ratings_reduced.map(calc_avg)


# KẾT XUẤT VÀ GHI FILE
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- ĐIỂM TRUNG BÌNH PHIM THEO NHÓM TUỔI ---\n")
    
    # Dùng Dictionary lồng nhau để lưu gom dữ liệu theo từng phim
    movie_age_summary = {}
    
    # Phương thức: toLocalIterator() -> ACTION
    # Kéo toàn bộ kết quả về Driver
    for (movie_id, age_group), (avg, cnt) in ratings_avg.toLocalIterator():
        if movie_id not in movie_age_summary:
            title = bc_movies.value.get(movie_id, "Unknown Movie")
            movie_age_summary[movie_id] = {"title": title, "data": {}}
        
        # Lưu điểm trung bình vào nhóm tuổi tương ứng của bộ phim đó
        movie_age_summary[movie_id]["data"][age_group] = f"{avg:.2f} ({cnt} votes)"

    # Các nhóm tuổi chuẩn để in theo thứ tự cho đẹp
    age_keys = ["<18", "18-24", "25-34", "35-44", "45-49", "50-55", "56+"]

    # Sắp xếp theo MovieID và in ra file
    for movie_id in sorted(movie_age_summary.keys()):
        info = movie_age_summary[movie_id]
        
        # Bắt đầu tạo dòng chuỗi định dạng
        line = f"ID: {movie_id:<4} | {info['title'][:35]:<35} | "
        
        # Duyệt qua từng nhóm tuổi, nếu không có ai đánh giá thì để 'N/A'
        age_strings = []
        for ak in age_keys:
            val = info["data"].get(ak, "N/A")
            age_strings.append(f"{ak}: {val:<15}")
        
        line += " | ".join(age_strings)
        
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

spark.stop()