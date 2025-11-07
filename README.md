# Hướng dẫn Apache Hive sử dụng Docker

Dự án này sử dụng Docker để nhanh chóng khởi động một cụm máy chủ Hadoop/Hive giả lập (pseudo-distributed cluster). Nó bao gồm hai phần:
1.  **Hướng dẫn cơ bản:** Nạp một tệp CSV (`demon_slayer.csv`) và chạy các truy vấn đơn giản.
2.  **Hướng dẫn nâng cao:** Tạo một "DB Lớn" (Movie DB) với 3,6 triệu dòng dữ liệu, được chia thành 365 tệp (phân vùng) để thực hành các truy vấn HQL phức tạp (JOINS, Window Functions) và kiểm tra sức mạnh của Phân vùng (Partitioning).

## 🧰 Công cụ cần thiết

* **Docker Desktop**
* **Git**
* **Python** (Để chạy kịch bản tạo "DB Lớn")

---

## 🚀 Phần 1: Hướng dẫn Cơ bản (Ví dụ `demon_slayer`)

### 1. Khởi động môi trường

1.  Mở PowerShell/Terminal (ví dụ: VS Code) và tạo một thư mục mới.
2.  Tải mã nguồn:
    ```powershell
    git clone https://github.com/har1zxje/docker-hive-project.git
    ```
3.  Di chuyển vào thư mục dự án:
    ```powershell
    cd Docker-hive
    ```
4.  Đảm bảo Docker Desktop đang chạy và khởi động các container:
    ```powershell
    docker compose up -d
    ```
    *(Lần đầu chạy sẽ mất vài phút để tải).*
5.  (Tùy chọn) Kiểm tra xem tất cả các container đã chạy (Status `Up` hoặc `Healthy`):
    ```powershell
    docker ps
    ```

### 2. Đưa dữ liệu vào Cụm (HDFS)

1.  Copy tệp `demon_slayer.csv` (có sẵn) vào container `namenode`:
    ```powershell
    docker cp demon_slayer.csv docker-hive-namenode-1:/tmp
    ```
2.  "Nhảy" vào bên trong container `namenode`:
    ```powershell
    docker exec -it docker-hive-namenode-1 bash
    ```
3.  (Bên trong `namenode`) Đưa tệp từ `/tmp` của container lên `/tmp` của HDFS:
    ```bash
    hdfs dfs -put /tmp/demon_slayer.csv /tmp
    ```
4.  Thoát khỏi `namenode`:
    ```bash
    exit
    ```

### 3. Tương tác với Hive (Tạo bảng)

1.  (Quay lại PowerShell) "Nhảy" vào container `hive-server`:
    ```powershell
    docker exec -it docker-hive-hive-server-1 bash
    ```
2.  (Bên trong `hive-server`) Kết nối Hive bằng Beeline:
    ```bash
    /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
    ```
3.  (Bên trong `beeline>`) Tạo Database và Bảng:
    ```sql
    CREATE DATABASE demon_slayer;
    USE demon_slayer;

    CREATE TABLE demon_slayers (
        demon_slayer_id INT,
        name STRING,
        rank STRING,
        breathing_style STRING,
        division STRING,
        age INT,
        weapon STRING,
        special_ability STRING,
        mission_id INT
    )
    COMMENT 'Table to store information about each Demon Slayer'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    ```

### 4. Nạp dữ liệu và Truy vấn

1.  (Bên trong `beeline>`) Nạp dữ liệu từ HDFS vào bảng:
    ```sql
    LOAD DATA INPATH '/tmp/demon_slayer.csv' INTO TABLE demon_slayers;
    ```
2.  (Bên trong `beeline>`) Chạy truy vấn:
    ```sql
    SELECT * FROM demon_slayers LIMIT 10;
    ```
    ```sql
    -- Đếm số lượng theo cấp bậc
    SELECT rank, COUNT(*) FROM demon_slayers GROUP BY rank;
    ```

---

## 📈 Phần 2: Hướng dẫn Nâng cao (Thử nghiệm "DB Lớn")

Phần này chúng ta sẽ tạo và phân tích một cơ sở dữ liệu mô phỏng dịch vụ xem phim với 3,6 triệu dòng dữ liệu, được phân vùng theo 365 ngày.

### 1. Tạo Dữ liệu

1.  (Trong PowerShell, tại thư mục dự án) Chạy kịch bản Python để tạo dữ liệu:
    ```powershell
    python generate_movie_db.py
    ```
    *Việc này sẽ tạo 2 tệp (`dim_users.csv`, `dim_movies.csv`) và 1 thư mục (`fact_stream_views`).*

### 2. Khởi động và Tải dữ liệu lên HDFS

1.  (Nếu chưa chạy) Khởi động Docker:
    ```powershell
    docker compose up -d
    ```
2.  Copy 3 bộ dữ liệu vào `namenode`:
    ```powershell
    docker cp dim_users.csv docker-hive-namenode-1:/tmp
    docker cp dim_movies.csv docker-hive-namenode-1:/tmp
    docker cp fact_stream_views docker-hive-namenode-1:/tmp
    ```
3.  "Nhảy" vào `namenode`:
    ```powershell
    docker exec -it docker-hive-namenode-1 bash
    ```
4.  (Bên trong `namenode`) Đưa 3 bộ dữ liệu lên thư mục `/user` của HDFS:
    ```bash
    hdfs dfs -put /tmp/dim_users.csv /user
    hdfs dfs -put /tmp/dim_movies.csv /user
    hdfs dfs -put /tmp/fact_stream_views /user
    ```
5.  Thoát khỏi `namenode`:
    ```bash
    exit
    ```

### 3. Vào Hive và Tạo Bảng (Movie DB)

1.  (Quay lại PowerShell) "Nhảy" vào `hive-server`:
    ```powershell
    docker exec -it docker-hive-hive-server-1 bash
    ```
2.  (Bên trong `hive-server`) Kết nối Beeline:
    ```bash
    /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
    ```
3.  (Bên trong `beeline>`) Tạo Database và 3 Bảng:

    **Tạo Database:**
    ```sql
    CREATE DATABASE movie_db;
    USE movie_db;
    ```
    **Tạo Bảng 1 (Users):**
    ```sql
    CREATE TABLE dim_users (
        user_id STRING, name STRING, join_date STRING, country STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");
    ```
    **Tạo Bảng 2 (Movies):**
    ```sql
    CREATE TABLE dim_movies (
        movie_id STRING, title STRING, genre STRING, release_year INT
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");
    ```
    **Tạo Bảng 3 (Views - Bảng LỚN, có Phân vùng):**
    ```sql
    CREATE EXTERNAL TABLE fact_stream_views (
        view_id STRING, user_id STRING, movie_id STRING, 
        watch_timestamp STRING, duration_watched INT
    )
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/fact_stream_views';
    ```

### 4. Nạp Dữ liệu & Chạy Truy vấn HQL Nâng cao

1.  (Bên trong `beeline>`) Nạp dữ liệu cho 2 bảng "nhỏ":
    ```sql
    LOAD DATA INPATH '/user/dim_users.csv' INTO TABLE dim_users;
    LOAD DATA INPATH '/user/dim_movies.csv' INTO TABLE dim_movies;
    ```
2.  (Bên trong `beeline>`) Nạp dữ liệu cho bảng "LỚN" (Tự động quét 365 phân vùng):
    ```sql
    MSCK REPAIR TABLE fact_stream_views;
    ```

---

## 📊 Phần 3: Các ví dụ Truy vấn HQL Nâng cao

#### Câu 1: Top 5 Thể loại được xem nhiều nhất

```sql
SELECT 
    m.genre AS The_Loai, 
    SUM(v.duration_watched) AS Tong_Thoi_Gian_Xem
FROM 
    fact_stream_views v
JOIN 
    dim_movies m ON v.movie_id = m.movie_id
GROUP BY 
    m.genre
ORDER BY 
    Tong_Thoi_Gian_Xem DESC
LIMIT 5;
```

#### Câu 2: Top 3 phim trong TỪNG thể loại (Window Function)
```sql
WITH MovieWatchTimes AS (
    SELECT
        m.genre,
        m.title,
        SUM(v.duration_watched) AS total_duration
    FROM 
        fact_stream_views v
    JOIN 
        dim_movies m ON v.movie_id = m.movie_id
    GROUP BY 
        m.genre, m.title
),
RankedMovies AS (
    SELECT
        genre,
        title,
        total_duration,
        -- Xếp hạng TRONG TỪNG (PARTITION BY) thể loại
        RANK() OVER (PARTITION BY genre ORDER BY total_duration DESC) as rank_in_genre
    FROM
        MovieWatchTimes
)
-- Chỉ chọn top 3 của mỗi thể loại
SELECT 
    genre AS The_Loai,
    title AS Ten_Phim,
    total_duration AS Tong_Thoi_Gian_Xem,
    rank_in_genre AS Xep_Hang
FROM RankedMovies 
WHERE rank_in_genre <= 3
ORDER BY The_Loai, Xep_Hang;
```

#### Câu 3: Kiểm tra Phân vùng (Tốt vs. Tệ)
```sql
-- TRUY VẤN "TỆ" (Chậm - Quét toàn bộ 365 tệp)
SELECT COUNT(*) 
FROM fact_stream_views
WHERE watch_timestamp LIKE '2023-12-25%';
SQL

-- TRUY VẤN "TỐT" (Nhanh - Dùng Phân vùng)
SELECT COUNT(*) 
FROM fact_stream_views
WHERE dt = '2023-12-25';
```

## 💤 Phần 4: Tắt/Dọn dẹp
#### Thoát Beeline:
```bash
!exit
```
#### Thoát hive-server:

```bash
exit
```
(Quay lại PowerShell) Tắt và xóa toàn bộ cluster (bao gồm cả dữ liệu HDFS):

```powershell
docker compose down
```
(Lưu ý: Nếu bạn muốn tạm dừng mà giữ lại dữ liệu, hãy dùng docker compose stop).

## 📚 Tài liệu tham khảo

Dự án này được xây dựng và học hỏi từ các nguồn tài liệu tuyệt vời sau:

* Hive with Docker: A Step-by-Step Guide to Managing Data[https://kira07.medium.com/hive-with-docker-a-step-by-step-guide-to-managing-data-d8a4683a2611]
* Link GitHub tham khảo[https://github.com/Vivekpawar07/Docker-hive]
