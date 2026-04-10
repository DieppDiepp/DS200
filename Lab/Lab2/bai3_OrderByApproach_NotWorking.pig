-- ## Bài 3:

-- Xác định khía cạnh nào nhận nhiều đánh giá tiêu cực (negative) nhất, 
-- và khía cạnh nào nhận nhiều đánh giá tích cực nhất (positive) nhất.

raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- 1. TÌM ASPECT TIÊU CỰC (NEGATIVE) NHẤT

-- Thay vì gom nhóm trước và lọc sentiment, ta sẽ filter sentiment trước, 
-- để khâu group chỉ cần group 1 khóa và tăng tốc độ do ít dòng hơn
negative_reviews = FILTER raw_reviews BY sentiment == 'negative';

-- Gom nhóm với khóa aspect
grouped_neg = GROUP negative_reviews BY aspect;

-- Đếm số lượng
counted_neg = FOREACH grouped_neg GENERATE 
        group AS aspect, 
        COUNT(negative_reviews) AS cnt;

-- Sắp xếp giảm dần và lấy max 1
ordered_neg = ORDER counted_neg BY cnt DESC;
max_negative_aspect = LIMIT ordered_neg 1;

STORE max_negative_aspect INTO 'output_bai3_negative' USING PigStorage(';');


-- 2. TÌM ASPECT TÍCH CỰC (POSITIVE) NHẤT

positive_reviews = FILTER raw_reviews BY sentiment == 'positive';

grouped_pos = GROUP positive_reviews BY aspect;

counted_pos = FOREACH grouped_pos GENERATE 
        group AS aspect, 
        COUNT(positive_reviews) AS cnt;

ordered_pos = ORDER counted_pos BY cnt DESC;
max_positive_aspect = LIMIT ordered_pos 1;

STORE max_positive_aspect INTO 'output_bai3_positive' USING PigStorage(';');