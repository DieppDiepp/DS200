-- ## Bài 5
-- Dựa vào từng phân loại bình luận (category), xác định 5 từ liên quan nhất 

raw_reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stop_words = LOAD 'stopwords.txt' AS (stopword:chararray);

-- xác định 5 từ liên quan nhất theo category, ta coi như là 5 từ xuất hiện nhiều nhất trong mỗi category (bỏ qua stop word)
-- FLATTEN để có một dòng cho mỗi từ
all_words = FOREACH raw_reviews GENERATE 
    category, 
    FLATTEN(TOKENIZE(LOWER(review), ' ')) AS word;

-- Loại bỏ stopword
joined_words = JOIN all_words BY word LEFT OUTER, stop_words BY stopword;
filtered_words = FILTER joined_words BY stop_words::stopword IS NULL;
clean_words = FOREACH filtered_words GENERATE all_words::category AS category, all_words::word AS word;

-- Group theo (category, từ) để đếm số lần xuất hiện
group_word_cat = GROUP clean_words BY (category, word);
word_cat_cnt = FOREACH group_word_cat GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(clean_words) AS cnt;

-- Gom nhóm theo Category
group_final_cat = GROUP word_cat_cnt BY category;

top5_relevant = FOREACH group_final_cat {
    -- Sắp xếp cục bộ bên trong từng category
    sorted_words = ORDER word_cat_cnt BY cnt DESC;
    -- Cắt lấy 5 từ đầu tiên
    top_words = LIMIT sorted_words 5;
    GENERATE group AS category, top_words AS top_5_words;
}

STORE top5_relevant INTO 'output_bai5_relevant' USING PigStorage(';');