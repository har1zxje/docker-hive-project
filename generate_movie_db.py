import os
import csv
import datetime
import random

# --- Cấu hình ---
NUM_USERS = 1000
NUM_MOVIES = 500
START_DATE = datetime.date(2023, 1, 1)
NUM_DAYS = 365 # Sẽ tạo 365 tệp/phân vùng
VIEWS_PER_DAY = 10000 # Số lượt xem mỗi ngày

# --- Dữ liệu giả ---
fake_names = ['An', 'Binh', 'Cuong', 'Dung', 'Em', 'Giang', 'Hien', 'Khanh', 'Long', 'Minh', 'Ngoc', 'Phuong', 'Quang', 'Son', 'Thanh', 'Tuan', 'Viet', 'Xuan', 'Yen']
fake_surnames = ['Nguyen', 'Tran', 'Le', 'Pham', 'Hoang', 'Phan', 'Vu', 'Dang', 'Bui', 'Do']
countries = ['Vietnam', 'USA', 'Japan', 'Korea', 'France', 'UK']
movie_first_part = ['The Rise of', 'Legend of', 'Fall of', 'Return of', 'Last', 'First', 'Shadow of', 'Attack of']
movie_second_part = ['Heroes', 'Titans', 'Warriors', 'Guardians', 'Dragons', 'Kings', 'Giants', 'Stars']
genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror', 'Romance', 'Documentary']

# --- Bảng 1: Tạo dim_users.csv ---
print("Đang tạo dim_users.csv...")
with open('dim_users.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['user_id', 'name', 'join_date', 'country'])
    user_ids = []
    for i in range(1, NUM_USERS + 1):
        user_id = f"u_{i:04}"
        user_ids.append(user_id)
        name = f"{random.choice(fake_names)} {random.choice(fake_surnames)}"
        join_date = START_DATE - datetime.timedelta(days=random.randint(30, 1000))
        country = random.choice(countries)
        writer.writerow([user_id, name, join_date, country])
print(f"Đã tạo {NUM_USERS} người dùng.")

# --- Bảng 2: Tạo dim_movies.csv ---
print("Đang tạo dim_movies.csv...")
with open('dim_movies.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['movie_id', 'title', 'genre', 'release_year'])
    movie_ids = []
    for i in range(1, NUM_MOVIES + 1):
        movie_id = f"m_{i:03}"
        movie_ids.append(movie_id)
        title = f"{random.choice(movie_first_part)} the {random.choice(movie_second_part)}"
        genre = random.choice(genres)
        release_year = random.randint(1980, 2023)
        writer.writerow([movie_id, title, genre, release_year])
print(f"Đã tạo {NUM_MOVIES} bộ phim.")

# --- Bảng 3: Tạo fact_stream_views (Nhiều tệp) ---
print("Đang tạo fact_stream_views (365 tệp)...")
view_id_counter = 1
for i in range(NUM_DAYS):
    current_date = START_DATE + datetime.timedelta(days=i)
    date_str = current_date.strftime('%Y-%m-%d')
    
    # Tạo thư mục phân vùng
    partition_path = os.path.join('fact_stream_views', f'dt={date_str}')
    os.makedirs(partition_path, exist_ok=True)
    
    file_path = os.path.join(partition_path, 'data.csv')
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if i == 0: # Chỉ ghi header cho tệp đầu tiên để tham khảo
            writer.writerow(['view_id', 'user_id', 'movie_id', 'watch_timestamp', 'duration_watched'])
            
        for _ in range(VIEWS_PER_DAY):
            view_id = f"v_{view_id_counter}"
            user_id = random.choice(user_ids)
            movie_id = random.choice(movie_ids)
            
            # Tạo mốc thời gian giả trong ngày
            fake_time = f"{random.randint(0,23):02}:{random.randint(0,59):02}:{random.randint(0,59):02}"
            timestamp_str = f"{date_str} {fake_time}"
            duration_watched = random.randint(1, 120) # Giả sử xem từ 1 đến 120 phút
            
            writer.writerow([view_id, user_id, movie_id, timestamp_str, duration_watched])
            view_id_counter += 1
            
    if i % 30 == 0:
        print(f"  Đã tạo phân vùng: {partition_path}")

total_views = (view_id_counter - 1)
print(f"Hoàn thành! Đã tạo 3 bảng với tổng cộng {total_views} lượt xem.")