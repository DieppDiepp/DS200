import os
from pyspark.sql import SparkSession

# Khởi tạo môi trường và cấu hình Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Bai5")
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
users_path = f"file://{data_dir}/users.txt"
occupation_path = f"file://{data_dir}/occupation.txt"
ratings_1_path = f"file://{data_dir}/ratings_1.txt"
ratings_2_path = f"file://{data_dir}/ratings_2.txt"
output_log_path = os.path.join(results_dir, "bai5_result.txt")


# CÁC HÀM XỬ LÝ (PARSE) DỮ LIỆU

def parse_occupation(line):
    parts = line.split(",", 1)
    occ_id = int(parts[0])
    occ_name = parts[1]
    return (occ_id, occ_name) # (OccupationID, OccupationName)

def parse_user_occ(line):
    parts = line.split(",", 4)
    user_id = int(parts[0])
    occ_id = int(parts[3])
    return (user_id, occ_id) # (UserID, OccupationID)


# TẠO DICTIONARY VÀ BROADCAST (Thay thế Join)

# TẠO DICTIONARY TÊN NGHỀ NGHIỆP
occ_dict = (
    sc.textFile(occupation_path)    # (Transformation): Đọc file occupation.txt
    .map(parse_occupation)          # (Transformation): Chuyển thành Pair RDD (OccupationID, OccupationName)
    .collectAsMap()                 # (Action): Kéo về Driver làm Dictionary
)

# TẠO DICTIONARY USER -> ID NGHỀ NGHIỆP
users_occ_dict = (
    sc.textFile(users_path)         # (Transformation): Đọc file users.txt
    .map(parse_user_occ)            # (Transformation): Chuyển thành Pair RDD (UserID, OccupationID)
    .collectAsMap()                 # (Action): Kéo về Driver làm Dictionary
)

# Phát sóng (Broadcast) 2 cuốn từ điển
bc_occ = sc.broadcast(occ_dict)
bc_users_occ = sc.broadcast(users_occ_dict)


# ĐỌC RATINGS VÀ THÊM NGHỀ NGHIỆP
def process_rating(line):
    parts = line.split(",")
    user_id = int(parts[0])
    rating = float(parts[2])
    
    # Lookup ID nghề nghiệp của User, sau đó tra cứu Tên nghề nghiệp tương ứng
    occ_id = bc_users_occ.value.get(user_id, -1)
    occ_name = bc_occ.value.get(occ_id, "Unknown")
    
    # Dùng Tên nghề nghiệp làm KEY để gom nhóm
    # Cấu trúc: (OccupationName, (Rating, Count))
    return (occ_name, (rating, 1)) # Ví dụ: ('programmer', (4.0, 1))

# Phương thức: union() & map() -> Transformation (Không Shuffle)
ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))
ratings_mapped = ratings_raw.map(process_rating)


# REDUCE TÍNH TỔNG VÀ TÍNH TRUNG BÌNH
def sum_ratings(val1, val2):
    return (val1[0] + val2[0], val1[1] + val2[1])

# Phương thức: reduceByKey() -> Transformation (CÓ SHUFFLE, TẠO STAGE MỚI)
# Cấu trúc: (OccupationName, (Total_Rating, Total_Count))
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings)

def calc_avg(row):
    occ_name, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    # Cấu trúc: (OccupationName, Avg_Rating, Total_Count)
    return (occ_name, avg_rating, total_count)

# Phương thức: map() -> Transformation (Không Shuffle)
ratings_avg = ratings_reduced.map(calc_avg)


# KẾT XUẤT VÀ GHI FILE
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- ĐIỂM TRUNG BÌNH THEO NGHỀ NGHIỆP ---\n")
    
    # Phương thức: collect() -> ACTION
    # Vì số lượng nghề nghiệp rất ít (khoảng ~21 nghề), ta có thể kéo thẳng toàn bộ về Driver
    results = ratings_avg.collect()
    
    # Sắp xếp danh sách dựa trên điểm trung bình (phần tử thứ 2 của tuple) giảm dần
    results_sorted = sorted(results, key=lambda x: x[1], reverse=True)
    
    for occ_name, avg, cnt in results_sorted:
        line = f"Occupation: {occ_name:<20} | AvgRating: {avg:.2f} | Total Ratings: {cnt}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

spark.stop()