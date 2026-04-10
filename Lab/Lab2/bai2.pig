-- ## Bài 2:

-- Thực hiện các thao tác thống kê sau:
-- - Thống kê tần số xuất hiện của các từ. Chỉ ra các từ xuất hiện trên 500 lần.
-- - Thống kê số bình luận theo từng phân loại (category).
-- - Thống kê số bình luận theo từng khía cạnh đánh giá (aspect).



-- 1. Thống kê tần số xuất hiện của các từ. Chỉ ra các từ xuất hiện trên 500 lần.
raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

lowercase_reviews = FOREACH raw_reviews GENERATE 
    id, 
    FLATTEN(TOKENIZE(LOWER(review), ' ')) AS lowercase_word,
    category AS lowercase_category,
    aspect AS lowercase_aspect,
    sentiment AS lowercase_sentiment;

group_table = GROUP lowercase_reviews BY lowercase_word;
count_all_words = FOREACH group_table GENERATE group AS word, COUNT(lowercase_reviews) AS total_words;
words_over_500 = FILTER count_all_words BY total_words >500;

STORE count_all_words INTO 'output_bai2_tat_ca_tu' USING PigStorage(';');
STORE words_over_500 INTO 'output_bai2_tu_tren_500' USING PigStorage(';');

-- 2. Thống kê số bình luận theo từng phân loại (category).
group_table_category = GROUP raw_reviews BY category;
count_category = FOREACH group_table_category GENERATE group AS category_name, COUNT(raw_reviews) AS total_review;

STORE count_category INTO 'output_bai2_category' USING PigStorage(';');


-- 3. Thống kê số bình luận theo từng khía cạnh đánh giá (aspect).
group_table_aspect = GROUP raw_reviews BY aspect;
count_aspect = FOREACH group_table_aspect GENERATE group AS aspect_name, COUNT(raw_reviews) AS total_reviews;

STORE count_aspect INTO 'output_bai2_aspect' USING PigStorage(';');

-- 4. In ra màn hình check
DUMP words_over_500;
DUMP count_category;
DUMP count_aspect;