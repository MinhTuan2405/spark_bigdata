# Báo Cáo Lab Spark - Phân Tích Dữ Liệu Phim

## Thông Tin
- **Môn học**: IE212 - Big Data
- **Họ tên sinh viên**: Nguyễn Hà Minh Tuấn
- **MSSV**: 23521718

---

## Dựng Spark Cluster với Docker

### 1. Cấu trúc Docker Compose

Project sử dụng Docker Compose để dựng Spark Cluster gồm:
- **1 Spark Master**: Điều phối và quản lý cluster
- **1 Spark Worker**: Thực thi các task

### 2. File `docker-compose.yml`

```yaml
services:
  spark-master:
    image: apache/spark
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"  
      - "7077:7077"  
    volumes:
      - .:/opt/spark/work-dir
    
  spark-worker:
    image: apache/spark
    container_name: spark-worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    volumes:
      - .:/opt/spark/work-dir
```

### 3. Khởi động Spark Cluster

```bash
# Khởi động 
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

### 4. Truy cập Spark Web UI

Mở trình duyệt và truy cập: `http://localhost:8080`

---

## Cách Chạy Các Bài Tập

### Cấu trúc thư mục

```
Spark/
├── data/
│   ├── movies.txt
│   ├── ratings_1.txt
│   ├── ratings_2.txt
│   ├── users.txt
│   └── occupation.txt
├── result/
│   ├── bai1.txt
│   ├── bai2.txt
│   ├── bai3.txt
│   ├── bai4.txt
│   ├── bai5.txt
│   └── bai6.txt
├── bai1.py
├── bai2.py
├── bai3.py
├── bai4.py
├── bai5.py
├── bai6.py
└── docker-compose.yml
```

### Lệnh chạy từng bài

```bash
# Bài 1: Phân tích đánh giá trung bình của phim
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai1.py

# Bài 2: Phân tích đánh giá theo thể loại
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai2.py

# Bài 3: Phân tích đánh giá theo giới tính
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai3.py

# Bài 4: Phân tích đánh giá theo nhóm tuổi
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai4.py

# Bài 5: Phân tích đánh giá theo nghề nghiệp
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai5.py

# Bài 6: Phân tích đánh giá theo thời gian
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/bai6.py
```

---

## Kết Quả Từng Bài

### Bài 1: Phân Tích Đánh Giá Trung Bình Của Phim

**Kết quả**:
```
Sunset Boulevard (1950) AverageRating: 4.36 (TotalRatings: 7)
Psycho (1960) AverageRating: 4.00 (TotalRatings: 2)
The Silence of the Lambs (1991) AverageRating: 3.14 (TotalRatings: 7)
No Country for Old Men (2007) AverageRating: 3.89 (TotalRatings: 18)
The Lord of the Rings: The Return of the King (2003) AverageRating: 3.82 (TotalRatings: 11)
E.T. the Extra-Terrestrial (1982) AverageRating: 3.67 (TotalRatings: 18)
Mad Max: Fury Road (2015) AverageRating: 3.47 (TotalRatings: 18)
The Terminator (1984) AverageRating: 4.06 (TotalRatings: 18)
The Godfather: Part II (1974) AverageRating: 4.00 (TotalRatings: 17)
The Lord of the Rings: The Fellowship of the Ring (2001) AverageRating: 3.89 (TotalRatings: 18)
Gladiator (2000) AverageRating: 3.61 (TotalRatings: 18)
The Social Network (2010) AverageRating: 3.86 (TotalRatings: 7)
Lawrence of Arabia (1962) AverageRating: 3.44 (TotalRatings: 18)
Fight Club (1999) AverageRating: 3.50 (TotalRatings: 7)

Sunset Boulevard (1950) is the highest rated movie with an average rating of 4.36 among movies with at least 5 ratings.
```


---

### Bài 2: Phân Tích Đánh Giá Theo Thể Loại

**Kết quả**:
```
Sci-Fi - AverageRating: 3.73 (TotalRatings: 54)
Action - AverageRating: 3.71 (TotalRatings: 54)
Drama - AverageRating: 3.76 (TotalRatings: 128)
Thriller - AverageRating: 3.70 (TotalRatings: 27)
Horror - AverageRating: 4.00 (TotalRatings: 2)
Family - AverageRating: 3.67 (TotalRatings: 18)
Biography - AverageRating: 3.56 (TotalRatings: 25)
Film-Noir - AverageRating: 4.36 (TotalRatings: 7)
Mystery - AverageRating: 4.00 (TotalRatings: 2)
Adventure - AverageRating: 3.63 (TotalRatings: 83)
Fantasy - AverageRating: 3.86 (TotalRatings: 29)
Crime - AverageRating: 3.81 (TotalRatings: 42)
```

---

### Bài 3: Phân Tích Đánh Giá Theo Giới Tính

**Kết quả**:
```
The Lord of the Rings: The Return of the King (2003) - Male_Avg: 3.75, Female_Avg: 3.90
The Terminator (1984) - Male_Avg: 3.93, Female_Avg: 4.14
The Godfather: Part II (1974) - Male_Avg: 4.06, Female_Avg: 3.94
Psycho (1960) - Male_Avg: NA, Female_Avg: 4.00
Sunset Boulevard (1950) - Male_Avg: 4.33, Female_Avg: 4.50
Gladiator (2000) - Male_Avg: 3.59, Female_Avg: 3.64
The Silence of the Lambs (1991) - Male_Avg: 3.33, Female_Avg: 3.00
Mad Max: Fury Road (2015) - Male_Avg: 4.00, Female_Avg: 3.32
The Social Network (2010) - Male_Avg: 4.00, Female_Avg: 3.67
Lawrence of Arabia (1962) - Male_Avg: 3.55, Female_Avg: 3.31
Fight Club (1999) - Male_Avg: 3.50, Female_Avg: 3.50
No Country for Old Men (2007) - Male_Avg: 3.92, Female_Avg: 3.83
E.T. the Extra-Terrestrial (1982) - Male_Avg: 3.81, Female_Avg: 3.55
The Lord of the Rings: The Fellowship of the Ring (2001) - Male_Avg: 4.00, Female_Avg: 3.80
```
---

### Bài 4: Phân Tích Đánh Giá Theo Nhóm Tuổi

**Kết quả**:
```
The Lord of the Rings: The Return of the King (2003) - [0-18: NA, 18-35: 3.83, 35-50: 3.86, 50+: 3.50]
The Terminator (1984) - [0-18: NA, 18-35: 4.17, 35-50: 4.06, 50+: 3.83]
The Godfather: Part II (1974) - [0-18: NA, 18-35: 3.78, 35-50: 4.25, 50+: NA]
Psycho (1960) - [0-18: NA, 18-35: 4.50, 35-50: 3.50, 50+: NA]
Sunset Boulevard (1950) - [0-18: NA, 18-35: 4.17, 35-50: 4.50, 50+: 4.50]
Gladiator (2000) - [0-18: NA, 18-35: 3.43, 35-50: 3.81, 50+: 3.50]
The Silence of the Lambs (1991) - [0-18: NA, 18-35: 3.00, 35-50: 3.00, 50+: 4.00]
Mad Max: Fury Road (2015) - [0-18: NA, 18-35: 3.36, 35-50: 3.64, 50+: NA]
The Social Network (2010) - [0-18: NA, 18-35: 4.00, 35-50: 3.67, 50+: NA]
Lawrence of Arabia (1962) - [0-18: NA, 18-35: 3.60, 35-50: 3.32, 50+: 3.75]
Fight Club (1999) - [0-18: NA, 18-35: 3.50, 35-50: 3.50, 50+: 3.50]
No Country for Old Men (2007) - [0-18: NA, 18-35: 3.79, 35-50: 3.88, 50+: 4.17]
E.T. the Extra-Terrestrial (1982) - [0-18: NA, 18-35: 3.56, 35-50: 3.83, 50+: 3.00]
The Lord of the Rings: The Fellowship of the Ring (2001) - [0-18: NA, 18-35: 4.00, 35-50: 3.75, 50+: 4.25]
```

---

### Bài 5: Phân Tích Đánh Giá Theo Nghề Nghiệp

**Kết quả**:
```
Teacher - AverageRating: 3.70 (TotalRatings: 5)
Accountant - AverageRating: 3.58 (TotalRatings: 6)
Designer - AverageRating: 4.00 (TotalRatings: 13)
Manager - AverageRating: 3.47 (TotalRatings: 16)
Consultant - AverageRating: 3.86 (TotalRatings: 14)
Salesperson - AverageRating: 3.65 (TotalRatings: 17)
Nurse - AverageRating: 3.86 (TotalRatings: 11)
Programmer - AverageRating: 4.25 (TotalRatings: 10)
Lawyer - AverageRating: 3.65 (TotalRatings: 17)
Journalist - AverageRating: 3.85 (TotalRatings: 17)
Engineer - AverageRating: 3.56 (TotalRatings: 18)
Student - AverageRating: 4.00 (TotalRatings: 8)
Artist - AverageRating: 3.73 (TotalRatings: 11)
Doctor - AverageRating: 3.69 (TotalRatings: 21)
```


---

### Bài 6: Phân Tích Đánh Giá Theo Thời Gian

**Kết quả**:
```
2020 - TotalRatings: 184, AverageRating: 3.75
```
