import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Khởi tạo môi trường và cấu hình Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab4_Bai1")
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
customer_list_path = f"file://{data_dir}/Customer_List.csv"
order_items_path = f"file://{data_dir}/Order_Items.csv"
order_reviews_path = f"file://{data_dir}/Order_Reviews.csv"
orders_path = f"file://{data_dir}/Orders.csv"
products_path = f"file://{data_dir}/Products.csv"

output_log_path = os.path.join(results_dir, "bai3_result.txt")

# 1. Hãy đọc dữ liệu từ các file csv, sử dụng tự suy ra kiểu dữ liệu cho mỗi cột.
customer_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(customer_list_path)
order_items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(order_items_path)
order_reviews_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(order_reviews_path)
orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(orders_path)
products_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(products_path)


# 3. Phân tích số lượng đơn hàng theo quốc gia, sắp xếp theo thứ tự giảm dần.
# Cần bảng Customer_List.csv, bảng Orders.csv
# Customer_Country;Customer_Country_Code ở Customer_List.csv
# Customer_Trx_ID ở Orders.csv và Customer_Trx_ID ở Customer_List.csv

orders_customer_df = orders_df.join(customer_df, on="Customer_Trx_ID")
orders_by_country = orders_customer_df.groupBy("Customer_Country").count().orderBy("count", ascending=False)

total_rows = orders_by_country.count()

print("Số lượng đơn hàng theo quốc gia:")
orders_by_country.show(n = total_rows, truncate=False)

## Output ra file

# Các tham số: (số_dòng_muốn_in, độ_dài_truncate, in_dọc)
# tham số đầu quy định in bao nhiêu dòng, 0 là truncate=False (không cắt chữ), False là không in dọc

table_string = orders_by_country._jdf.showString(total_rows, 0, False)

with open(output_log_path, "w", encoding="utf-8") as f: # dùng "a" (append) để ghi nối tiếp vào file
    f.write(f"\nSố lượng đơn hàng theo quốc gia:\n{table_string}\n")