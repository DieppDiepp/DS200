-- ## Bài 3: (Phiên bản an toàn tuyệt đối cho môi trường Local)

-- Xác định khía cạnh nào nhận nhiều đánh giá tiêu cực (negative) nhất, 
-- và khía cạnh nào nhận nhiều đánh giá tích cực nhất (positive) nhất.

raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- 1. Gom nhóm và tạo cột đếm số sentiment
grouped_aspect_sentiment = GROUP raw_reviews BY (aspect, sentiment);

counted = FOREACH grouped_aspect_sentiment GENERATE
        group.aspect AS aspect,
        group.sentiment AS sentiment,
        COUNT(raw_reviews) AS cnt;

-- 2. Tìm aspect có nhiều lượt negative nhất 
-- (Ý tưởng: Ta tìm MAX của bảng counted khi đã lọc chỉ có negative -> JOIN với bảng count ban đầu để lấy các tên aspect ra)
neg_counted = FILTER counted BY sentiment == 'negative';

-- Để dùng được MAX cần phải GROUP ALL lại, vì MAX chỉ hoạt động trên tập hợp đã được group
neg_all = GROUP neg_counted ALL;
max_neg_val = FOREACH neg_all GENERATE MAX(neg_counted.cnt) AS max_cnt;

-- JOIN bảng số liệu với giá trị MAX để lấy ra dòng chứa aspect tương ứng
joined_neg = JOIN neg_counted BY cnt, max_neg_val BY max_cnt;

final_neg = FOREACH joined_neg GENERATE neg_counted::aspect AS aspect, neg_counted::cnt AS negative_count;

STORE final_neg INTO 'output_bai3_negative' USING PigStorage(';');


-- 3. Tìm aspect có nhiều lượt positive nhất
pos_counted = FILTER counted BY sentiment == 'positive';

pos_all = GROUP pos_counted ALL;
max_pos_val = FOREACH pos_all GENERATE MAX(pos_counted.cnt) AS max_cnt;

joined_pos = JOIN pos_counted BY cnt, max_pos_val BY max_cnt;

final_pos = FOREACH joined_pos GENERATE pos_counted::aspect AS aspect, pos_counted::cnt AS positive_count;

STORE final_pos INTO 'output_bai3_positive' USING PigStorage(';');