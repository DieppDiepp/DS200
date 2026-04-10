# 📊 DS200 - Big Data Analysis

![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Hadoop](https://img.shields.io/badge/Apache_Hadoop-66CC00?style=for-the-badge&logo=apachehadoop&logoColor=white)
![Pig](https://img.shields.io/badge/Apache_Pig-EA2027?style=for-the-badge&logo=apache&logoColor=white)

Welcome to the repository for **DS200 - Big Data Analysis**. This workspace contains practical lab assignments and projects focused on processing, analyzing, and extracting insights from large datasets using Big Data ecosystems.

## 📂 Labs Overview

### 🎬 [Lab 1: Movie Ratings Analysis](./Lab/Lab1)
* **Concepts:** MapReduce, Distributed Data Processing
* **Tech Stack:** Java, Apache Hadoop
* **Description:** Implemented a MapReduce algorithm to process and aggregate movie ratings from multiple datasets (`movies.txt`, `ratings.txt`). The program calculates the average rating for each movie and identifies the highest-rated movie with a minimum number of reviews.

### 🏨 [Lab 2: Hotel Reviews Text Analysis](./Lab/Lab2)
* **Concepts:** Text Processing, Data Exploration, Stopwords Filtering
* **Tech Stack:** Apache Pig, Apache Hadoop
* **Description:** Analyzed unstructured text data from `hotel-review.csv`. Involves data cleaning, removing common stopwords (`stopwords.txt`), and preparing datasets for frequency analysis using Pig Latin scripts.

## 🚀 How to Run

### Hadoop MapReduce (Java)
1. Compile the Java files into a JAR.
2. Run via Hadoop CLI:
   ```bash
   hadoop jar application.jar MainClass /input_path /output_path
   ```

### Apache Pig Scripts
Run the Pig scripts via the Pig shell (local or MapReduce mode):
```bash
pig -x local script.pig
```
# Or in MapReduce mode
```bash
pig script.pig
```

---
*Created for the DS200 Course. Note: The `.ipynb` files are just assignment instructions.*
