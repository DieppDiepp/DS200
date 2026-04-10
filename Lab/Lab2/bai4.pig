-- ## Bài 4:

-- Thực hiện các phân tích sau:
-- - Dựa vào từng phân loại bình luận, hãy xác định 5 từ mang ý nghĩa tích cực nhất.
-- - Dựa vào từng phân loại bình luận, hãy xác định 5 từ mang ý nghĩa tiêu cực nhất.

raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stop_words = LOAD 'stopwords.txt' AS (stopword:chararray);

-- 1. Tìm 5 từ tích cực (positive) nhất theo category

-- Lọc bình luận tích cực
pos_reviews = FILTER raw_reviews BY sentiment == 'positive';

-- Tách từ, đưa về chữ thường và FLATTEN để có một dòng cho mỗi từ
pos_words = FOREACH pos_reviews GENERATE 
    category, 
    FLATTEN(TOKENIZE(LOWER(review), ' ')) AS word;

-- Loại bỏ stopword
pos_joined = JOIN pos_words BY word LEFT OUTER, stop_words BY stopword;
pos_filtered = FILTER pos_joined BY stop_words::stopword IS NULL;
pos_clean = FOREACH pos_filtered GENERATE pos_words::category AS category, pos_words::word AS word;

-- Gom nhóm theo (category, từ) để đếm số lần xuất hiện
pos_group_word = GROUP pos_clean BY (category, word);
pos_word_cnt = FOREACH pos_group_word GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(pos_clean) AS cnt;

-- Gom nhóm theo Category và dùng Nested FOREACH để lấy Top 5
pos_group_cat = GROUP pos_word_cnt BY category;

top5_positive = FOREACH pos_group_cat {
    -- Bên trong ngoặc nhọn này là xử lý cục bộ cho TỪNG category
    sorted_words = ORDER pos_word_cnt BY cnt DESC;
    top_words = LIMIT sorted_words 5;
    GENERATE group AS category, top_words AS top_5_words;
}

STORE top5_positive INTO 'output_bai4_positive' USING PigStorage(';');


-- 2. Tìm 5 từ tiêu cực (negative) nhất theo category

-- Thay đổi luồng dữ liệu thành negative
neg_reviews = FILTER raw_reviews BY sentiment == 'negative';

neg_words = FOREACH neg_reviews GENERATE 
    category, 
    FLATTEN(TOKENIZE(LOWER(review), ' ')) AS word;

neg_joined = JOIN neg_words BY word LEFT OUTER, stop_words BY stopword;
neg_filtered = FILTER neg_joined BY stop_words::stopword IS NULL;
neg_clean = FOREACH neg_filtered GENERATE neg_words::category AS category, neg_words::word AS word;

neg_group_word = GROUP neg_clean BY (category, word);
neg_word_cnt = FOREACH neg_group_word GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(neg_clean) AS cnt;

neg_group_cat = GROUP neg_word_cnt BY category;

top5_negative = FOREACH neg_group_cat {
    sorted_words = ORDER neg_word_cnt BY cnt DESC;
    top_words = LIMIT sorted_words 5;
    GENERATE group AS category, top_words AS top_5_words;
}

STORE top5_negative INTO 'output_bai4_negative' USING PigStorage(';');