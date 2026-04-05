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
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Bai2 {
    // Đọc các files rating  - RatingMapper
    // Object - vị trí dòng - không dùng
    // Text - nội dung full dòng
    // Text - Mã Phim
    // Text - Điểm số, dán nhãn "Rate:" để truyền đi
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        // static class để truy cập thẳng vô lớp không cần khởi tạo đối tượng

        // key - vị trí dòng - không dùng
        // value - nội dung full dòng
        // context - truyền đi dữ liệu sau khi xử lý
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] catChuoi = dongDuLieu.split(",");
            String maPhim = catChuoi[1].trim();
            String diemSo = catChuoi[2].trim();
            context.write(new Text(maPhim), new Text("Rate:" + diemSo));
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] catChuoi = dongDuLieu.split(",");
            String maPhim = catChuoi[0].trim();
            String theLoaiPhim = catChuoi[2].trim();
            // Ta cần tách các thể loại phim và gửi từng thể loại một với mã phim tương ứng
            // String[] theLoaiPhimArray = theLoaiPhim.split("\\|");
            // for (String theLoai : theLoaiPhimArray) {
            context.write(new Text(maPhim), new Text("Type:" + theLoaiPhim));

        }
    }

    // Ta cần 2 reducer, do key để join không phải key để gom nhóm, 
    // nên ta sẽ dùng 1 reducer để join dữ liệu theo mã phim, sau đó mới dùng 1 reducer nữa để tính điểm trung bình theo thể loại
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        // Tuy không có lệnh group by trong Reducer nhưng do ở Mapper ta đã dùng lệnh context.write(new Text(maPhim)
        // khi qua mạng (Shuffle & Sort) sẽ rà xoát tất cả kiện hàng, nào có cùng key (mã phim) sẽ được gom nhóm lại với nhau
        // Vì Hadoop đã tóm tất cả các kiện hàng có chung key lại, nên nó đóng gói thành một cái danh sách - Iterable<Text> values
        // Lúc này khi reducer chạy, nó sẽ cầm, ví dụ: key: "1043" | values: ["theLoai:Action", "theLoai:Action", "theLoai:Adventure", "theLoai:Adventure" "Rate:3.5", "Rate:5.0"] 
        // Giả sử phim này có thể loại là 2 thể loại Action và Adventure, và có 2 lượt đánh giá với điểm số lần lượt là 3.5 và 5.0

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String theLoaiPhim = "";
            // Phải dùng 1 cái list để hứng điểm, vì có thể bốc trúng Rate trước khi bốc trúng Type
            ArrayList<String> danhSachDiem = new ArrayList<>();

            for (Text value: values){
                String kienHang = value.toString();
                if (kienHang.startsWith("Type:")) {
                    theLoaiPhim = kienHang.substring(5); // Lấy phần sau "Type:"
                } else if (kienHang.startsWith("Rate:")) {
                    String diemSo = kienHang.substring(5); // Lấy phần sau "Rate:"
                    danhSachDiem.add(diemSo);
                }
            }

            // Nếu tìm thấy cả Thể loại và rating thì mới tính điểm trung bình và xuất ra
            if (!theLoaiPhim.isEmpty() && !danhSachDiem.isEmpty()) {
                String[] theLoaiPhimArray = theLoaiPhim.split("\\|");

                // Với mỗi thể loại ta sẽ gắn điểm số tương ứng và xuất ra
                for (String theLoai : theLoaiPhimArray) {
                    for (String Diem : danhSachDiem) {
                        // Kết quả xuất ra: Khóa là "Action", Giá trị là "4.0"
                        context.write(new Text(theLoai), new Text(Diem));
                    }
                }

            }
        }
    }

    // Mapper thứ 3 để tính điểm trung bình theo thể loại
    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Đọc file nháp từ JoinReducer xuất ra, có dạng: "Action   4.0"
            String[] parts = value.toString().split("\\t");

            // check nếu có đủ 2 phần (thể loại và điểm số) sau khi tách bằng tab
            if (parts.length >= 2) {
                String theLoai = parts[0].trim();
                String diemSo = parts[1].trim();
                context.write(new Text(theLoai), new Text(diemSo));
            }
        }
    }

    // Reducer thứ 2 để tính điểm trung bình theo thể loại
    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           double tongDiem = 0.0;
           int soNguoiDanhGia = 0;

           for (Text value: values){
            tongDiem += Double.parseDouble(value.toString());
            soNguoiDanhGia += 1;
           }

           if (soNguoiDanhGia > 0) {
            double diemTrungBinh = tongDiem / soNguoiDanhGia;
            String ketQua = String.format("Avg: %.2f, Count: %d", diemTrungBinh, soNguoiDanhGia);
            context.write(key, new Text(ketQua));
           }
        }
    }

    // main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Đường dẫn file tạm để Job 1 truyền dữ liệu cho Job 2
        Path tempDir = new Path("temp_bai2_nhap"); 

        // Chạy Job Join data
        Job job1 = Job.getInstance(conf, "Job 1: Ghép Dữ Liệu");
        job1.setJarByClass(Bai2.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class); // ratings
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MovieMapper.class);  // movies
        FileOutputFormat.setOutputPath(job1, tempDir); // Xuất ra file tạm

        // Chờ Job 1 chạy xong. Nếu Job 1 thất bại thì dừng chương trình
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Chạy JOB 2
        Job job2 = Job.getInstance(conf, "Job 2: Tinh Trung Binh");
        job2.setJarByClass(Bai2.class);
        job2.setMapperClass(GenreMapper.class);
        job2.setReducerClass(AvgReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Lấy file tạm của Job 1 làm đầu vào cho Job 2
        FileInputFormat.addInputPath(job2, tempDir);
        // Xuất ra thư mục kết quả cuối cùng
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean success = job2.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempDir, true);

        System.exit(success ? 0 : 1);
    }
}