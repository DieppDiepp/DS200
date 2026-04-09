-- ## Bài 1:

-- Thực hiện các thao tác sau đối với bộ dữ liệu:
-- - Đưa tất cả ký tự về chữ thường (lowercase).
-- - Tách các dòng bình luận thành dãy các từ (từ được tách ra từ câu theo khoảng trắng).
-- - Loại bỏ các stop word (dựa vào danh sách ```stopword.txt```)

raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:int, review:chararray, aspect:chararray, subaspect:chararray, sentiment:chararray);

stop_words = LOAD 'stopwords.txt' AS (stopword:chararray);

lowercase_reviews = FOREACH raw_reviews GENERATE 
    id, 
    FLATTEN(TOKENIZE(LOWER(review), ' ')) AS lowercase_word,
    LOWER(aspect) AS lowercase_aspect,
    LOWER(subaspect) AS lowercase_subaspect,
    LOWER(sentiment) AS lowercase_sentiment;

-- Left join
joined_table = JOIN lowercase_reviews BY lowercase_word LEFT OUTER, stop_words BY stopword;

-- Lọc các khóa Null là không dính stop word
filtered_joined_table = FILTER joined_table BY stop_words::stopword IS NULL;

final_results = FOREACH filtered_joined_table GENERATE lowercase_reviews::id AS id, lowercase_reviews::lowercase_word AS cleanword;

DUMP final_results;

STORE final_results INTO 'output_bai1' USING PigStorage(';');