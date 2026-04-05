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

public class Bai4 {
    // vòng 1: join rating với user để gán nhóm tuổi vào điểm số, output sẽ là: UserID \t MãPhim_ĐiểmSố, ví dụ: 850 \t Rate:1043_4.5
    // Trong đó Key là UserID, Value là "Rate:MãPhim_ĐiểmSố"
    public static class RatingMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dongDuLieuArray = value.toString().split(",");

            if (dongDuLieuArray.length >= 4) {
                String userID = dongDuLieuArray[0].trim();
                String maPhim = dongDuLieuArray[1].trim();
                String diemSo = dongDuLieuArray[2].trim();

                // Gửi kiện hàng: key: userID | value: "Rate:MãPhim_ĐiểmSố"
                context.write(new Text(userID), new Text("Rate:" + maPhim + "_" + diemSo));
            }
        }
    }

    // UserMapper, lấy nhóm tuổi của user
    // Output sẽ là: UserID \t AgeGroup:NhómTuổi, ví dụ: 850 \t AgeGroup:18-35
    public static class UserMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dongDuLieuArray = value.toString().split(",");

            // UserID, Gender, Age, Occupation, Zip-code
            if (dongDuLieuArray.length >= 5) {
                String userID = dongDuLieuArray[0].trim();
                String ageString = dongDuLieuArray[2].trim();

                try {
                    int age = Integer.parseInt(ageString);
                    String nhomTuoi = "";

                    // Phân loại nhóm tuổi
                    if (age <= 18) {
                        nhomTuoi = "0-18";
                    } else if (age <= 35) {
                        nhomTuoi = "18-35";
                    } else if (age <= 50) {
                        nhomTuoi = "35-50";
                    } else {
                        nhomTuoi = "50+";
                    }

                    // Gửi kiện hàng: key: userID | value: "AgeGroup:NhómTuổi"
                    context.write(new Text(userID), new Text("AgeGroup:" + nhomTuoi));
                } catch (NumberFormatException e) {
                    // Bỏ qua nếu cột tuổi bị lỗi không phải là số
                }
            }
        }
    }

    // Reducer để join dữ liệu theo UserID, output sẽ là: MãPhim \t NhómTuổi_ĐiểmSố, ví dụ: 1043 \t 18-35_4.5
    public static class UserJoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nhomTuoi = "";
            ArrayList<String> danhSachRating = new ArrayList<>();

            for (Text value : values) {
                String kienHang = value.toString();
                if (kienHang.startsWith("AgeGroup:")) {
                    nhomTuoi = kienHang.substring(9);
                } else if (kienHang.startsWith("Rate:")) {
                    danhSachRating.add(kienHang.substring(5));
                }
            }

            if (!nhomTuoi.isEmpty() && !danhSachRating.isEmpty()) {
                for (String rating : danhSachRating) {
                    String[] ratingArray = rating.split("_");
                    String maPhim = ratingArray[0];
                    String diemSo = ratingArray[1];

                    // Không dán nhãn ở đây, xuất thẳng file nháp: MãPhim \t NhómTuổi_ĐiểmSố
                    context.write(new Text(maPhim), new Text(nhomTuoi + "_" + diemSo));
                }
            }
        }
    }

    // vòng 2: join kết quả từ vòng 1 với file movie để lấy tên phim, sau đó tính điểm trung bình theo nhóm tuổi

    // Mapper để đọc kết quả từ vòng 1, output sẽ là: MãPhim \t GRate:NhómTuổi_ĐiểmSố, ví dụ: 1043 \t GRate:18-35_4.5
    public static class Job1OutputMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dongDuLieuArray = value.toString().split("\t"); 

            if (dongDuLieuArray.length >= 2) {
                String maPhim = dongDuLieuArray[0].trim();
                String nhomTuoi_DiemSo = dongDuLieuArray[1].trim();

                // Dán nhãn GRate (Gender Rate) để Reducer biết đây là điểm
                context.write(new Text(maPhim), new Text("GRate:" + nhomTuoi_DiemSo));
            }
        }
    }

    // Mapper để đọc file movie, output sẽ là: MãPhim \t Movie:TênPhim, ví dụ: 1043 \t Movie:Die Hard
    public static class MovieMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dongDuLieuArray = value.toString().split(",");

            if (dongDuLieuArray.length >= 3) {
                String maPhim = dongDuLieuArray[0].trim();
                String tenPhim = dongDuLieuArray[1].trim();

                context.write(new Text(maPhim), new Text("Movie:" + tenPhim));
            }
        }
    }

    // Reducer để join dữ liệu theo MãPhim, tính điểm trung bình theo nhóm tuổi và xuất ra kết quả cuối cùng
    // Output sẽ là: TênPhim \t 0-18: ĐiểmTrungBình   18-35: ĐiểmTrungBình   35-50: ĐiểmTrungBình   50+: ĐiểmTrungBình
    public static class FinalReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tenPhim = "";
            
            // Khai báo 4 cặp biến tính tổng và đếm cho 4 nhóm tuổi
            double tongDiem_0_18 = 0.0;     int count_0_18 = 0;
            double tongDiem_18_35 = 0.0;    int count_18_35 = 0;
            double tongDiem_35_50 = 0.0;    int count_35_50 = 0;
            double tongDiem_50_plus = 0.0;  int count_50_plus = 0;

            for (Text value : values) {
                String kienHang = value.toString();
                if (kienHang.startsWith("Movie:")) {
                    tenPhim = kienHang.substring(6); 
                } else if (kienHang.startsWith("GRate:")) {
                    String nhomTuoi_DiemSo = kienHang.substring(6); 
                    String[] parts = nhomTuoi_DiemSo.split("_");
                    
                    if (parts.length >= 2) {
                        String nhomTuoi = parts[0];
                        double diemSo = Double.parseDouble(parts[1]);

                        // Phân loại điểm số theo 4 nhóm
                        if (nhomTuoi.equals("0-18")) {
                            tongDiem_0_18 += diemSo;
                            count_0_18++;
                        } else if (nhomTuoi.equals("18-35")) {
                            tongDiem_18_35 += diemSo;
                            count_18_35++;
                        } else if (nhomTuoi.equals("35-50")) {
                            tongDiem_35_50 += diemSo;
                            count_35_50++;
                        } else if (nhomTuoi.equals("50+")) {
                            tongDiem_50_plus += diemSo;
                            count_50_plus++;
                        }
                    }
                }
            }

            // Xử lý tính toán và in kết quả (chỉ in nếu có tên phim và có ít nhất 1 đánh giá)
            if (!tenPhim.isEmpty() && (count_0_18 > 0 || count_18_35 > 0 || count_35_50 > 0 || count_50_plus > 0)) {
                
                // Nếu count > 0 thì tính trung bình, ngược lại in chữ "NA"
                String avg0_18   = (count_0_18 > 0)   ? String.format("%.2f", tongDiem_0_18 / count_0_18)     : "NA";
                String avg18_35  = (count_18_35 > 0)  ? String.format("%.2f", tongDiem_18_35 / count_18_35)   : "NA";
                String avg35_50  = (count_35_50 > 0)  ? String.format("%.2f", tongDiem_35_50 / count_35_50)   : "NA";
                String avg50_plus= (count_50_plus > 0)? String.format("%.2f", tongDiem_50_plus / count_50_plus) : "NA";

                String ketQua = String.format("0-18: %s   18-35: %s   35-50: %s   50+: %s", 
                                               avg0_18, avg18_35, avg35_50, avg50_plus);
                                               
                context.write(new Text(tenPhim), new Text(ketQua));
            }
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path tempDir = new Path("temp_bai4_nhap"); 

        // JOB 1
        Job job1 = Job.getInstance(conf, "Job 1: Ghep Rating va Nhom Tuoi");
        job1.setJarByClass(Bai4.class);
        job1.setReducerClass(UserJoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class); 
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);  
        FileOutputFormat.setOutputPath(job1, tempDir); 

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // JOB 2
        Job job2 = Job.getInstance(conf, "Job 2: Ghep Movie va Tinh Toan");
        job2.setJarByClass(Bai4.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, tempDir, TextInputFormat.class, Job1OutputMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean success = job2.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempDir, true);

        System.exit(success ? 0 : 1);
    }
}