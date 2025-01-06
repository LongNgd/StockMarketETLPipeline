# Stock Market ETL Pipeline

## Mô tả
Đây là một dự án ETL pipeline được xây dựng để thu thập, xử lý và lưu trữ dữ liệu thị trường chứng khoán. Pipeline tự động hóa quy trình thu thập thông tin cổ phiếu, giá cổ phiếu, tỷ giá hối đoái, tính toán các chỉ số kỹ thuật, và tải dữ liệu lên Google BigQuery để phân tích.

Pipeline này được triển khai bằng **Apache Airflow**, giúp lập lịch và quản lý các tác vụ. Dữ liệu được xử lý với **pandas**, **yfinance**, và **BeautifulSoup**, đồng thời lưu trữ vào BigQuery thông qua thư viện **pandas-gbq**.

## Các tính năng chính
1. **Thu thập dữ liệu**:
   - Lấy thông tin các cổ phiếu hoạt động mạnh từ Yahoo Finance.
   - Tải giá cổ phiếu lịch sử trong 3 tháng qua.
   - Lấy dữ liệu tỷ giá hối đoái (USD/VND).

2. **Xử lý dữ liệu**:
   - Tính toán các chỉ số kỹ thuật như RSI, SMA, EMA, và Bollinger Bands.
   - Làm sạch và chuẩn hóa dữ liệu.

3. **Tải dữ liệu lên BigQuery**:
   - Tải dữ liệu đã xử lý lên Google BigQuery để phân tích và báo cáo.

4. **Quản lý với Airflow**:
   - Sử dụng Airflow để tự động hóa và lập lịch chạy pipeline hàng ngày.

## Công nghệ sử dụng
- **Apache Airflow**: Quản lý workflow và lập lịch.
- **Python**: Ngôn ngữ chính cho xử lý dữ liệu.
- **pandas**: Xử lý và phân tích dữ liệu.
- **yfinance**: Lấy dữ liệu tài chính.
- **BeautifulSoup**: Thu thập dữ liệu từ web.
- **Google BigQuery**: Lưu trữ và phân tích dữ liệu.
- **pandas-gbq**: Tích hợp pandas với BigQuery.

## Cách sử dụng
### 1. Cài đặt
1. Clone repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Cài đặt các thư viện cần thiết:
   ```bash
   pip install -r requirements.txt
   ```

3. Cấu hình Google Cloud Service Account để truy cập BigQuery:
   - Tải file JSON Service Account và thiết lập biến môi trường:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-file.json"
     ```

### 2. Chạy pipeline
1. Khởi động Airflow:
   ```bash
   airflow standalone
   ```

2. Truy cập giao diện Airflow tại [http://localhost:8080](http://localhost:8080) và kích hoạt DAG `etl_dag`.

3. Pipeline sẽ tự động chạy theo lịch hoặc bạn có thể khởi chạy thủ công.

