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

output_log_path = os.path.join(results_dir, "bai2_result.txt")

# 1. Hãy đọc dữ liệu từ các file csv, sử dụng tự suy ra kiểu dữ liệu cho mỗi cột.
customer_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(customer_list_path)
order_items_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(order_items_path)
order_reviews_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(order_reviews_path)
orders_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(orders_path)
products_df = spark.read.options(header="true", sep=";", inferSchema="true").csv(products_path)


# 2. Thống kê tổng số đơn hàng, số lượng khách hàng và người bán.
total_orders = orders_df.count()
total_customers = orders_df.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items_df.select("Seller_ID").distinct().count()

print(f"Tổng số đơn hàng: {total_orders}")
print(f"Tổng số khách hàng: {total_customers}")
print(f"Tổng số người bán: {total_sellers}")

## Xuất kết quả ra file
with open(output_log_path, "w") as f:
    f.write(f"Tổng số đơn hàng: {total_orders}\n")
    f.write(f"Tổng số khách hàng: {total_customers}\n")
    f.write(f"Tổng số người bán: {total_sellers}\n")