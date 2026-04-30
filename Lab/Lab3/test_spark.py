from pyspark.sql import SparkSession

# Khởi tạo Spark Session
print("Đang khởi động Spark...")
spark = SparkSession.builder.master("local[*]").appName("Test_Spark").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Khởi tạo một list dữ liệu đơn giản
data = [1, 2, 3, 4, 5]

# Tạo RDD từ list dữ liệu
rdd = sc.parallelize(data)

# Nhân đôi mỗi phần tử trong RDD (Transformation)
rdd_mapped = rdd.map(lambda x: x * 2)

# Lấy kết quả về (Action)
ket_qua = rdd_mapped.collect()

print("Dữ liệu ban đầu:", data)
print("Kết quả sau khi nhân đôi:", ket_qua)

# Tạm dừng chương trình để xem Web UI
input("Spark Web UI đang mở! Hãy vào http://localhost:4040 trên trình duyệt. Nhấn Enter tại đây để tắt Spark...")

# Tắt Spark
spark.stop()
print("Đã tắt Spark. Chúc mừng bạn!")