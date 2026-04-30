import os
from pyspark.sql import SparkSession

# Khởi tạo môi trường và cấu hình Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Bai1")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Xử lý đường dẫn linh hoạt
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, "../data")
results_dir = os.path.join(current_dir, "../results")

# Tự động tạo thư mục results nếu chưa tồn tại
os.makedirs(results_dir, exist_ok=True)

# Khai báo đường dẫn file
movies_path = f"file://{data_dir}/movies.txt"
ratings_1_path = f"file://{data_dir}/ratings_1.txt"
ratings_2_path = f"file://{data_dir}/ratings_2.txt"
output_log_path = os.path.join(results_dir, "bai1_result.txt")

# Đọc file movies.txt và tạo một map (MovieID -> Title)
def parse_movies(line):
    parts = line.split(",", 2)
    return (int(parts[0]), parts[1]) # MovieID nằm ở vị trí thứ 1 (index 0), Title nằm ở vị trí thứ 2 (index 1)

movies_rdd = sc.textFile(movies_path).map(parse_movies).cache() # Hàm map là một transformation, lưu vào DAG

# Đọc file ratings_1.txt và ratings_2.txt, map MovieID -> (Rating, 1)
def parse_ratings(line):
    parts = line.split(",")
    movie_id = int(parts[1]) # MovieID nằm ở vị trí thứ 2 (index 1)
    rating = float(parts[2]) # Rating nằm ở vị trí thứ 3 (index 2)
    return (movie_id, (rating, 1)) # Trả về (MovieID, (Rating, 1)), trong đó 1 đại diện cho số lượt đánh giá (count) ban đầu là 1 cho mỗi dòng dữ liệu

ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path)) # Union là một transformation, kết hợp hai RDD lại với nhau
ratings_mapped = ratings_raw.map(parse_ratings)

# Reduce để tính tổng điểm và số lượt đánh giá
def sum_ratings(val1, val2):
    # val1 và val2 đều có dạng (total_rating, total_count)
    # Spark sẽ tự bỏ qua Key khi gọi hàm này, chỉ truyền giá trị (value) của các cặp (key, value) vào hàm
    # Ví dụ: val1 = (total_rating1, total_count1), val2 = (total_rating2, total_count2)
    return (val1[0] + val2[0], val1[1] + val2[1])

# ReduceByKey là một transformation, nhóm theo MovieID và áp dụng hàm sum_ratings để tính tổng điểm và số lượt đánh giá cho mỗi phim
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings) # Cần Shuffle và tạo ra 1 stage mới trong DAG, lưu trữ trên ổ đĩa cứng chứ không phải RAM
# Sau khi reduceByKey, chúng ta có một RDD với cấu trúc (MovieID, (total_rating, total_count)), trong đó total_rating.

# Tính điểm trung bình
def calc_avg(row):
    movie_id, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    return (movie_id, (avg_rating, total_count))

ratings_avg = ratings_reduced.map(calc_avg)

# Nối (Join) RDD phim và RDD điểm để lấy Tên Phim
# Join là một transformation, kết hợp hai RDD dựa trên khóa MovieID để tạo ra một RDD mới có cấu trúc (MovieID, (Title, (AvgRating, Count)))
# Ví dụ: (100, ("The Avengers", (4.5, 200)))
final_rdd = movies_rdd.join(ratings_avg).cache()

# Tìm phim có điểm trung bình cao nhất (>= 5 lượt đánh giá)
filtered_rdd = final_rdd.filter(lambda x: x[1][1][1] >= 5)

# KẾT XUẤT VÀ GHI FILE (ACTIONS)
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- DANH SÁCH TỔNG HỢP PHIM ---\n")
    
    # Kéo dữ liệu về để ghi file
    for m_id, (title, (avg, cnt)) in final_rdd.toLocalIterator():
        line = f"MovieID: {m_id} | Title: {title} | AvgRating: {avg:.2f} | Count: {cnt}"
        f.write(line + "\n")
        
    f.write("\n--- PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHẤT (>= 5 LƯỢT ĐÁNH GIÁ) ---\n")
    
    # Kiểm tra và in ra phim cao điểm nhất
    if not filtered_rdd.isEmpty():
        top_movie = filtered_rdd.max(key=lambda x: x[1][1][0])
        top_m_id, (top_title, (top_avg, top_cnt)) = top_movie
        
        result_text = f"WINNER -> Title: '{top_title}' | Điểm: {top_avg:.2f} | Lượt đánh giá: {top_cnt}"
        print(result_text)
        f.write(result_text + "\n")
    else:
        print("Không có phim nào đạt điều kiện >= 5 lượt đánh giá.")
        f.write("Không có phim nào đạt điều kiện >= 5 lượt đánh giá.\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

spark.stop()