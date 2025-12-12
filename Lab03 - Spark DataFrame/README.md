# Báo Cáo Lab Spark - Phân Tích Dữ Liệu E-commerce

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
      - ./:/opt/spark/work-dir
    
  spark-worker:
    image: apache/spark
    container_name: spark-worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    volumes:
      - ./:/opt/spark/work-dir
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
Lab03 - Spark DataFrame/
├── data/
│   ├── Customer_List.csv
│   ├── Orders.csv
│   ├── Order_Items.csv
│   ├── Order_Reviews.csv
│   └── Products.csv
├── result/
│   ├── bai1.txt
│   ├── bai2.txt
│   ├── bai3.txt
│   ├── bai4.txt
│   ├── bai5.txt
│   └── bai10.txt
├── bai1.py
├── bai2.py
├── bai3.py
├── bai4.py
├── bai5.py
└── bai10.py
```

### Lệnh chạy từng bài

```bash
# Bài 1: Đọc và hiển thị schema của các file CSV
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai1.py"

# Bài 2: Thống kê tổng số đơn hàng, khách hàng, người bán
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai2.py"

# Bài 3: Phân tích số đơn hàng theo quốc gia
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai3.py"

# Bài 4: Phân tích số đơn hàng theo thời gian
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai4.py"

# Bài 5: Phân tích đánh giá của khách hàng
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai5.py"

# Bài 10: Xếp hạng người bán theo doanh thu
docker-compose exec -T spark-master bash -lc "cd '/opt/spark/work-dir/Lab03 - Spark DataFrame' && /opt/spark/bin/spark-submit bai10.py"
```

---

## Kết Quả Từng Bài

### Bài 1: Đọc và Hiển Thị Schema Của Các File CSV

**Kết quả**:
```
Customer_List.csv
  - Customer_Trx_ID: string
  - Subscriber_ID: string
  - Subscribe_Date: date
  - First_Order_Date: date
  - Customer_Postal_Code: string
  - Customer_City: string
  - Customer_Country: string
  - Customer_Country_Code: string
  - Age: int
  - Gender: string
```

---

### Bài 2: Thống Kê Tổng Số Đơn Hàng, Khách Hàng, Người Bán

**Kết quả**:
```
Tổng số đơn hàng: 99441
Tổng số khách hàng: 102727
Tổng số người bán: 3095
```

---

### Bài 3: Phân Tích Số Đơn Hàng Theo Quốc Gia

**Kết quả**:
```
Germany: 41754 orders
France: 12848 orders
Netherlands: 11629 orders
Belgium: 5464 orders
Austria: 5043 orders
Switzerland: 3640 orders
United Kingdom: 3382 orders
Poland: 2139 orders
Czechia: 2034 orders
Italy: 2025 orders
```

---

### Bài 4: Phân Tích Số Đơn Hàng Theo Thời Gian

**Kết quả**:
```
2022/12:  1 orders
2022/10:  324 orders
2022/9:  4 orders
2023/12:  5673 orders
2023/11:  7544 orders
2023/10:  4631 orders
2023/9:  4285 orders
2023/8:  4331 orders
2023/7:  4026 orders
2023/6:  3245 orders
```

---

### Bài 5: Phân Tích Đánh Giá Của Khách Hàng

**Kết quả**:
```
Điểm đánh giá trung bình: 4.0864214950162765

1: 11424 đánh giá
2: 3151 đánh giá
3: 8179 đánh giá
4: 19141 đánh giá
5: 57328 đánh giá
```

---

### Bài 10: Xếp Hạng Người Bán Theo Doanh Thu

**Kết quả**:
```
4869f7a5dfa277a7dca6462dcf3b52b2:  229472.6300000003 Doanh thu | 1132 Số đơn hàng
53243585a1d6dc2643021fd1853d8905:  222776.05000000005 Doanh thu | 358 Số đơn hàng
4a3ca9315b744ce9f8e9374361493884:  200472.92000000068 Doanh thu | 1806 Số đơn hàng
fa1c13f2614d7b5c4749cbc52fecda94:  194042.0300000002 Doanh thu | 585 Số đơn hàng
7c67e1448b00f6e969d365cea6b010ab:  187923.89000000013 Doanh thu | 982 Số đơn hàng
7e93a43ef30c4f03f38b393420bc753a:  176431.87000000005 Doanh thu | 336 Số đơn hàng
da8622b14eb17ae2831f4ac5b9dab84a:  160236.5700000004 Doanh thu | 1314 Số đơn hàng
7a67c85e85bb2ce8582c35f2203ad736:  141745.53000000038 Doanh thu | 1160 Số đơn hàng
1025f0e2d44d7041d6cf58b6550e0bfa:  138968.55 Doanh thu | 915 Số đơn hàng
955fee9216a65b617aa5c0531780ce60:  135171.70000000007 Doanh thu | 1287 Số đơn hàng
```
