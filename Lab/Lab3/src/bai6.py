import os
import datetime
from pyspark.sql import SparkSession

# Khởi tạo môi trường và cấu hình Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Bai6")
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

# Khai báo đường dẫn file (Chỉ cần 2 file ratings)
ratings_1_path = f"file://{data_dir}/ratings_1.txt"
ratings_2_path = f"file://{data_dir}/ratings_2.txt"
output_log_path = os.path.join(results_dir, "bai6_result.txt")


# CÁC HÀM XỬ LÝ (PARSE) DỮ LIỆU

def parse_rating_to_year(line):
    parts = line.split(",")
    # Cấu trúc: UserID, MovieID, Rating, Timestamp
    rating = float(parts[2])
    timestamp = int(parts[3])
    
    # Chuyển đổi Timestamp (Unix epoch) sang Năm
    # Ví dụ: 978300760 -> 2000
    year = datetime.datetime.fromtimestamp(timestamp).year
    
    # Cấu trúc trả về: (Year, (Rating, 1))
    return (year, (rating, 1)) # Ví dụ: (2000, (4.0, 1)) đại diện cho một lượt đánh giá 4.0 vào năm 2000


# ĐỌC RATINGS VÀ TRÍCH XUẤT NĂM

# Phương thức: union() -> Transformation (Không Shuffle)
ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))

# Phương thức: map() -> Transformation (Không Shuffle)
# Mỗi Worker sẽ tự động import thư viện datetime và chuyển đổi timestamp ngay tại bộ nhớ cục bộ
ratings_mapped = ratings_raw.map(parse_rating_to_year)


# REDUCE TÍNH TỔNG VÀ TÍNH TRUNG BÌNH

def sum_ratings(val1, val2):
    return (val1[0] + val2[0], val1[1] + val2[1])

# Phương thức: reduceByKey() -> Transformation (CÓ SHUFFLE, TẠO STAGE MỚI)
# Cấu trúc sau khi reduce: (Year, (Total_Rating, Total_Count))
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings)

def calc_avg(row):
    year, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    # Cấu trúc: (Year, Avg_Rating, Total_Count)
    return (year, avg_rating, total_count)

# Phương thức: map() -> Transformation (Không Shuffle)
ratings_avg = ratings_reduced.map(calc_avg)


# KẾT XUẤT VÀ GHI FILE
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- ĐIỂM TRUNG BÌNH VÀ TỔNG LƯỢT ĐÁNH GIÁ THEO NĂM ---\n")
    
    # Phương thức: collect() -> ACTION
    # Dữ liệu gộp theo năm có số lượng dòng cực nhỏ (chỉ vài chục dòng), nên kéo thẳng về Driver
    results = ratings_avg.collect()
    
    # Sắp xếp danh sách theo Năm (phần tử đầu tiên của tuple) tăng dần
    results_sorted = sorted(results, key=lambda x: x[0])
    
    for year, avg, cnt in results_sorted:
        line = f"Year: {year} | AvgRating: {avg:.2f} | Total Ratings: {cnt}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

spark.stop()