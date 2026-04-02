import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    // Đọc các files rating  - RatigMapper
    // Mapper<Object, Text, Text, Text> có nghĩa là:
    // - Object: Kiểu dữ liệu của khóa đầu vào (ở đây là vị trí của dòng trong file, nhưng ta không dùng đến nên để Object)
    // - Text: Kiểu dữ liệu của giá trị đầu vào (ở đây là nội dung của từng dòng trong file)
    // - Text: Kiểu dữ liệu của khóa đầu ra (ở đây là Mã Phim)
    // - Text: Kiểu dữ liệu của giá trị đầu ra (ở đây là điểm số,ta sẽ dán nhãn "Rate:" vào trước để phân biệt với "Movie:")
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] catChuoi = dongDuLieu.split(","); 

            // Thêm trim() để xóa khoảng trắng dư
            String maPhim = catChuoi[1].trim(); 
            String diemSo = catChuoi[2].trim(); 

            context.write(new Text(maPhim), new Text("Rate:" + diemSo));
        }
    }

    // Đọc các files movie - MovieMapper
    // Mapper<Object, Text, Text, Text> có nghĩa là:
    // - Object: Kiểu dữ liệu của khóa đầu vào (ở đây là vị trí của dòng trong file, nhưng ta không dùng đến nên để Object)
    // - Text: Kiểu dữ liệu của giá trị đầu vào (ở đây là nội dung của từng dòng trong file)
    // - Text: Kiểu dữ liệu của khóa đầu ra (ở đây là Mã Phim)
    // - Text: Kiểu dữ liệu của giá trị đầu ra (ở đây là tên phim, ta sẽ dán nhãn "Movie:" vào trước để phân biệt với "Rate:")
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] catChuoi = dongDuLieu.split(",");

            String maPhim = catChuoi[0].trim();
            String tenPhim = catChuoi[1].trim();

            context.write(new Text(maPhim), new Text("Movie:" + tenPhim));
        }
    }

    //  Xử lý dữ liệu đã được gom nhóm theo Mã Phim - RatingReducer
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        double diemCaoNhat = 0.0;
        String phimCaoDiemNhat = "";

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tenPhim = "Chua_co_ten";
            double tongDiem = 0.0;
            int soNguoiDanhGia = 0;

            // Hadoop đã gom sẵn tất cả các đoạn có cùng Mã Phim vào cái rổ "values"
            for (Text val : values) {
                String kienHang = val.toString();
                
                if (kienHang.startsWith("Movie:")) {
                    // Nếu kiện hàng dán nhãn "Movie:", cắt bỏ 6 chữ đầu đi để lấy tên phim
                    tenPhim = kienHang.substring(6);
                } 
                else if (kienHang.startsWith("Rate:")) {
                    // Nếu kiện hàng dán nhãn "Rate:", cắt bỏ 5 chữ đầu đi để lấy điểm số
                    double diem = Double.parseDouble(kienHang.substring(5));
                    tongDiem = tongDiem + diem;
                    soNguoiDanhGia = soNguoiDanhGia + 1;
                }
            }

            // Tính điểm trung bình và xuất ra (chỉ xét phim có người đánh giá)
            if (soNguoiDanhGia > 0) {
                double diemTrungBinh = tongDiem / soNguoiDanhGia;
                
                String ketQuaTinh = "Diem Trung Binh: " + diemTrungBinh + " (Tong so luot: " + soNguoiDanhGia + ")";
                context.write(new Text(tenPhim), new Text(ketQuaTinh));

                if (soNguoiDanhGia >= 5 && diemTrungBinh > diemCaoNhat) {
                    diemCaoNhat = diemTrungBinh;
                    phimCaoDiemNhat = tenPhim;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String loiTuyenBo = phimCaoDiemNhat + " is the highest rated movie with an average rating of " + diemCaoNhat + " among movies with at least 5 ratings.";
            context.write(new Text("=> QUAN QUAN: "), new Text(loiTuyenBo));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tinh Diem Phim Don Gian");
        job.setJarByClass(Bai1.class);

        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}