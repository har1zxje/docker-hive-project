import os
import datetime
import random

# Hàm để tạo một dòng log CSV giả
def create_fake_log_line(current_time):
    user_id = random.randint(1000, 9999)
    actions = ['view', 'click', 'purchase', 'add_to_cart', 'logout']
    action = random.choice(actions)
    return f"{current_time},{user_id},{action}\n"

# Ngày bắt đầu và số ngày
start_date = datetime.date(2023, 1, 1)
num_days = 365
num_lines_per_file = 1000 # Tạo 1000 dòng log cho mỗi ngày

print("Bắt đầu tạo dữ liệu log giả lập...")

# Vòng lặp qua 365 ngày
for i in range(num_days):
    current_date = start_date + datetime.timedelta(days=i)
    
    # Lấy thông tin năm, tháng, ngày
    year = current_date.year
    month = current_date.month
    day = current_date.day
    
    # Tạo đường dẫn thư mục theo cấu trúc phân vùng của Hive
    # Ví dụ: logs/year=2023/month=1/day=1
    path = os.path.join('logs', f'year={year}', f'month={month}', f'day={day}')
    
    # Tạo thư mục nếu nó chưa tồn tại
    os.makedirs(path, exist_ok=True)
    
    # Tên tệp tin
    file_path = os.path.join(path, 'data.csv')
    
    # Ghi dữ liệu giả vào tệp
    try:
        with open(file_path, 'w') as f:
            # Ghi tiêu đề CSV
            f.write("timestamp,user_id,action\n")
            
            # Ghi 1000 dòng log giả
            for j in range(num_lines_per_file):
                # Tạo một mốc thời gian giả trong ngày
                fake_time = f"{random.randint(0,23):02}:{random.randint(0,59):02}:{random.randint(0,59):02}"
                timestamp_str = f"{current_date} {fake_time}"
                f.write(create_fake_log_line(timestamp_str))
                
        if (i % 30 == 0): # Chỉ in ra mỗi 30 ngày cho đỡ rối
            print(f"Đã tạo: {file_path}")

    except Exception as e:
        print(f"Lỗi khi tạo tệp {file_path}: {e}")

print("Hoàn thành!")
print(f"Đã tạo thành công 365 tệp log trong thư mục 'logs'.")