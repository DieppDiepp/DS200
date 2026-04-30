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
ratings_1_path = f"file://{data_dir}/ratings_1.txt"
ratings_2_path = f"file://{data_dir}/ratings_2.txt"
output_log_path = os.path.join(results_dir, "bai2_result.txt")

# Bước 1: Tạo map (MovieID -> List of Genres)
def parse_movie_genres(line):
    parts = line.split(",", 2)
    movie_id = int(parts[0])
    # Genres nằm ở cột thứ 3, các thể loại phân cách bằng dấu "|"
    genres_list = parts[2].split("|")
    return (movie_id, genres_list)

# Dùng collectAsMap() để chuyển RDD nhỏ thành Python Dictionary.
# Action này chạy ngay lập tức để nạp dữ liệu phim vào bộ nhớ Driver.
movies_genres_dict = sc.textFile(movies_path).map(parse_movie_genres).collectAsMap()

# Broadcast biến Dictionary này đến tất cả Worker để lookup nhanh, tránh dùng Join gây Shuffle
# Không dùng Join vì sẽ tạo ra một stage mới và phải shuffle dữ liệu, trong khi chúng ta chỉ cần lookup thông tin thể loại phim dựa trên MovieID.
# Broadcast Variable là một cách để gửi dữ liệu chỉ đọc (read-only) từ Driver đến tất cả các Worker một cách hiệu quả, tránh việc phải gửi đi nhiều lần trong quá trình tính toán.
bc_movies_genres = sc.broadcast(movies_genres_dict)

# Bước 2: Map từ MovieID -> Rating -> [(Genre1, Rating), (Genre2, Rating)...]
def parse_rating_to_genres(line):
    parts = line.split(",")
    movie_id = int(parts[1])
    rating = float(parts[2])
    
    # Lookup danh sách thể loại từ Broadcast Variable
    # Nếu không tìm thấy phim, trả về list rỗng []
    genres = bc_movies_genres.value.get(movie_id, [])
    
    # Tạo ra nhiều cặp (Genre, (Rating, 1)) cho mỗi lượt đánh giá
    # Ví dụ phim Action|Sci-Fi được 4 điểm -> [(Action, (4.0, 1)), (Sci-Fi, (4.0, 1))]
    return [(genre, (rating, 1)) for genre in genres]

ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))

# Dùng flatMap để "trải phẳng" mảng chứa nhiều tuple thành các tuple riêng biệt trong RDD
# flatmap khác với map ở chỗ nó sẽ lấy mỗi phần tử đầu vào và trả về một iterable (như list), sau đó "trải phẳng" tất cả các iterable này thành một RDD mới. 
# Trong khi map sẽ trả về một RDD chứa các iterable, flatMap sẽ trả về một RDD chứa các phần tử riêng biệt.
genres_mapped = ratings_raw.flatMap(parse_rating_to_genres)

# Bước 3: Tính trung bình điểm đánh giá cho từng thể loại
def sum_ratings(val1, val2):
    return (val1[0] + val2[0], val1[1] + val2[1])

# Gom nhóm theo Genre (Lưu ý: reduceByKey sẽ tạo Stage mới)
genres_reduced = genres_mapped.reduceByKey(sum_ratings)

def calc_avg(row):
    genre, (total_rating, total_count) = row
    avg_rating = total_rating / total_count
    return (genre, avg_rating, total_count)

# Kết quả RDD: (Genre, AvgRating, Count)
genres_avg = genres_reduced.map(calc_avg)

# KẾT XUẤT VÀ GHI FILE (ACTIONS)
print("\nĐang xử lý và ghi kết quả, vui lòng đợi...\n")

with open(output_log_path, "w", encoding="utf-8") as f:
    f.write("--- ĐIỂM TRUNG BÌNH THEO THỂ LOẠI PHIM ---\n")
    
    # Kéo dữ liệu về để ghi file
    for genre, avg, cnt in genres_avg.toLocalIterator():
        line = f"Genre: {genre:15} | AvgRating: {avg:.2f} | Total Ratings: {cnt}"
        print(line)
        f.write(line + "\n")

print(f"\nHoàn tất! Xem toàn bộ kết quả tại: {output_log_path}")

spark.stop()