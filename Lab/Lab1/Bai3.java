import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {
    
    // Đọc các files rating  - RatingMapper: key: userID
    public static class RatingMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            // tách chuỗi trong rating, 850, 1043, 4.5, 964983190 -- UserID, MovieID, Rating, Timestamp
            String[] dongDuLieuArray = dongDuLieu.split(",");

            if (dongDuLieuArray.length >= 4) {
                String userID = dongDuLieuArray[0].trim();
                String maPhim = dongDuLieuArray[1].trim();
                String diemSo = dongDuLieuArray[2].trim();

                // Gửi kiện hàng: key: maPhim | value: "Rate:MãPhim_ĐiểmSố"
                // Rate: để đánh dấu rating | _ để tách mã phim và điểm số
                context.write(new Text(userID), new Text("Rate:" + maPhim + "_" + diemSo));
            }
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String [] dongDuLieuArray = dongDuLieu.split(",");

            // UserID, Gender, Age, Occupation, Zip-code
            if (dongDuLieuArray.length >= 5) {
                String userID = dongDuLieuArray[0].trim();
                String userGender = dongDuLieuArray[1].trim();

                // Gửi kiện hàng: key: userID | value: "User:GiớiTính"
                context.write(new Text(userID), new Text("Gender:" + userGender));
            }
        }
    }

    // Vòng 1, gộp giới tính vào điểm số
    public static class UserJoinReducer extends Reducer<Text, Text, Text, Text>{

        // Rổ lúc này chứa: Key: userID | Values: ["Gender:M", "Rate:1043_4.5", "Rate:1043_5.0", "Rate:1050_3.0"]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String gioiTinh = "";
            ArrayList<String> danhSachRating = new ArrayList<>();

            // Tách giới tính và điểm số ra khỏi rổ
            for (Text value : values) {
                String kienHang = value.toString();
                if (kienHang.startsWith("Gender:")) {
                    gioiTinh = kienHang.substring(7); // Chứa: "M" hoặc "F"
                } else if (kienHang.startsWith("Rate:")) {
                    danhSachRating.add(kienHang.substring(5)); // Chứa: "1043_4.5", "1043_5.0", "1050_3.0"
                }
            }

            // Nếu tìm thấy giới tính, gán giới tính đó cho tưng bộ phhim mà user đã đánh giá
            if (!gioiTinh.isEmpty() && !danhSachRating.isEmpty()) {
                for (String rating : danhSachRating) {
                    String[] ratingArray = rating.split("_");
                    String maPhim = ratingArray[0];
                    String diemSo = ratingArray[1];

                    // Gửi kiện hàng: key: maPhim | value: "Gender:GiớiTính_ĐiểmSố", ví dụ: "F_4.0"
                    context.write(new Text(maPhim), new Text("Gender:" + gioiTinh + "_" + diemSo));
                }
            }
        }
    }

    // Vòng 2, tính điểm trung bình theo giới tính

    // Ta cần 1 mapper để đọc rating đã gán giới tính ở vòng 1
    public static class Job1OutputMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] dongDuLieuArray = dongDuLieu.split("\t"); // Tách bằng tab vì output của reducer mặc định sẽ tách key và value bằng tab

            if (dongDuLieuArray.length >= 2) {
                String maPhim = dongDuLieuArray[0].trim();
                String gioiTinh_DiemSo = dongDuLieuArray[1].trim(); // Vi dụ: "F_4.0"

                // Gửi kiện hàng: key: maPhim | value: "Gender:GiớiTính_ĐiểmSố"
                context.write(new Text(maPhim), new Text(gioiTinh_DiemSo));
                // Ví dụ: key: "1043" | value: "Gender:F_4.0"
            }
        }
    }

    // Ta cần 1 mapper để đọc movie name
    public static class MovieMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dongDuLieu = value.toString();
            String[] dongDuLieuArray = dongDuLieu.split(",");

            if (dongDuLieuArray.length >= 3) {
                String maPhim = dongDuLieuArray[0].trim();
                String tenPhim = dongDuLieuArray[1].trim();

                // Gửi kiện hàng: key: maPhim | value: "Movie:TênPhim"
                context.write(new Text(maPhim), new Text("Movie:" + tenPhim));
            }
        }
    }

    // Gộp full dữ liệu để tính điểm trung bình theo giới tính
    public static class FinalReducer extends Reducer<Text, Text, Text, Text>{
        // Rổ lúc này (Ví dụ key là Movie 1043): ["Title:Toy Story (1995)", "Gender:f_4.0", "Gender:F_5.0"...]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tenPhim = "";
            
            double tongDiemNam = 0.0;
            int soNguoiDanhGiaNam = 0;
            double tongDiemNu = 0.0;
            int soNguoiDanhGiaNu = 0;

            for (Text value : values) {
                String kienHang = value.toString();
                if (kienHang.startsWith("Movie:")) {
                    tenPhim = kienHang.substring(6); // Lấy phần sau "Movie:"
                } else if (kienHang.startsWith("Gender:")) {
                    String gioiTinh_DiemSo = kienHang.substring(7); // Lấy phần sau "Gender:"
                    String[] gioiTinh_DiemSoArray = gioiTinh_DiemSo.split("_");
                    if (gioiTinh_DiemSoArray.length >= 2) {
                        String gioiTinh = gioiTinh_DiemSoArray[0];
                        double diemSo = Double.parseDouble(gioiTinh_DiemSoArray[1]);

                        // Phân loại điểm số theo giới tính
                        if (gioiTinh.equals("M")) {
                            tongDiemNam += diemSo;
                            soNguoiDanhGiaNam ++;
                        } else if (gioiTinh.equals("F")) {
                            tongDiemNu += diemSo;
                            soNguoiDanhGiaNu ++;
                        }
                    }

                }
            
            }

            // Chỉ in ra nếu lấy được tên phim và có ít nhất 1 người đánh giá
            if (!tenPhim.isEmpty() && (soNguoiDanhGiaNam > 0 || soNguoiDanhGiaNu > 0)) {
                // Tránh lỗi chia cho 0 nếu phim đó chỉ có 1 giới tính đánh giá
                double tbNam = (soNguoiDanhGiaNam > 0) ? (tongDiemNam / soNguoiDanhGiaNam) : 0.0;
                double tbNu = (soNguoiDanhGiaNu > 0) ? (tongDiemNu / soNguoiDanhGiaNu) : 0.0;

                String ketQua = String.format("Male: %.2f, Female: %.2f", tbNam, tbNu);
                context.write(new Text(tenPhim), new Text(ketQua));

            }
        }
    }
    public static void main(String[] args) throws Exception {
        //  args[0]=ratings, args[1]=users, args[2]=movies, args[3]=output
        Configuration conf = new Configuration();
        Path tempDir = new Path("temp_bai3_nhap"); 

        // Chạy JOB 1
        Job job1 = Job.getInstance(conf, "Job 1: Ghep Rating va User");
        job1.setJarByClass(Bai3.class);
        job1.setReducerClass(UserJoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Nhận cửa 0 (ratings) và cửa 1 (users)
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class); 
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);  
        FileOutputFormat.setOutputPath(job1, tempDir); 

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Chạy JOB 2
        Job job2 = Job.getInstance(conf, "Job 2: Ghep Movie va Tinh Toan");
        job2.setJarByClass(Bai3.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Nhận cửa phụ (tempDir) và cửa 2 (movies)
        MultipleInputs.addInputPath(job2, tempDir, TextInputFormat.class, Job1OutputMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
        
        // Xuất ra cửa cuối cùng (args[3])
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean success = job2.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempDir, true);

        System.exit(success ? 0 : 1);
    }
}