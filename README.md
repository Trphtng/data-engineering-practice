# Bài tập kĩ thuật dữ liệu
## Thông tin nhóm
1. Nguyễn Trường Phát 23697331 
2. Lê Trần Quang Nhân 23698431
   
## Lab 8
### Case 1: Xây dựng Pipeline tự động cào và trực quan hóa dữ liệu
- Phương pháp: Sử dụng mô hình ETL (Extract, Transform, Load) để thực hiện và sử dụng AirFlow để tự động hóa pipeline
- Các bước:
  
  B1: Thu thập dữ liệu (file csv, mySQL)
  
  B2: Viết script Python để xử lí trích xuất dữ liệu
  
  B3: Tải (Load) dữ liệu lên phần mềm PostgreSQL + Pandas
  
  B4: Trực quan hóa trên PowerBI
  
- Tổng kết:
    Dự án là một mô hình mô phỏng hệ thống ETL hoàn chỉnh trong lĩnh vực thương mại điện tử. Quy trình gồm các công đoạn từ trích xuất dữ liệu từ tệp hoặc cơ sở dữ liệu, xử lý và chuyển đổi dữ liệu, cho đến khâu nạp dữ liệu vào kho dữ liệu (Data Warehouse) sử dụng PostgreSQL hoặc MySQL. Apache Airflow được triển khai để tự động hóa và giám sát toàn bộ quá trình. Ngoài ra, hệ thống hỗ trợ tích hợp các plugin mở rộng, cho phép kết nối linh hoạt với nhiều loại cơ sở dữ liệu phổ biến khác.

### Case 2: Xây dựng Pipeline tự động cào dữ liệu và huấn luyện mô hình 
- Mục tiêu: Xây dựng và chạy thử được các DAGs cơ bản đến nâng cao.
- Công cụ: Sử dụng AirFlow để chạy Pipeline với cài đặt thời gian.


## Lab 9
### Mục tiêu:

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.


### Exercise 1 - Downloading files.
#### 1. Mục tiêu bài tập:

Bài tập nhằm giúp sinh viên luyện tập kỹ năng lập trình Python thông qua một tình huống thực tế: tải xuống và xử lý dữ liệu từ các nguồn trên Internet.

#### 2. Phân tích yêu cầu:
    
Bài toán cung cấp một danh sách các URL trỏ tới các tệp .zip. Nhiệm vụ của sinh viên là:

- Tạo thư mục downloads nếu chưa tồn tại.
- Tải từng tệp .zip từ danh sách URL.
- Giải nén các tệp .csv từ .zip.
- Xoá các tệp .zip sau khi giải nén thành công.
- Xử lý lỗi khi tải hoặc giải nén gặp sự cố.

#### 3. Kết quả thực hiện:

Khi chạy chương trình, thư mục downloads sẽ được tạo (nếu chưa có), sau đó các tệp .zip được tải về lần lượt. Nếu file tải thành công, nó sẽ được giải nén và xóa khỏi ổ đĩa, chỉ giữ lại tệp .csv. Các bước thực hiện được in ra màn hình để người dùng dễ theo dõi.

![z6570760647429_3f486af5dd911d108266d43edef81d0d](https://github.com/user-attachments/assets/06f1bca7-909e-4877-b361-99caf7972187)



### Exercise 2 - Web Scraping + Downloading + Pandas
#### 1. Mục tiêu bài tập:

Xây dựng một chương trình Python thu thập dữ liệu web, xây dựng uri, tải xuống tệp và sử dụng Pandas để thực hiện một số hành động tích lũy đơn giản. 

#### 2. Phân tích yêu cầu:
- Bước 1: Chuẩn bị môi trường và Docker
- Bước 2: Viết chương trình Python để quét dữ liệu
- Bước 3: Phân tích dữ liệu với Pandas
- Bước 4: Kiểm tra kết quả

#### 3. Kết luận:

- Bài tập giúp củng cố kiến thức về web scraping, xử lý file, phân tích dữ liệu và đặc biệt là triển khai script Python trong môi trường Docker.
- Trong quá trình thực hiện, đã xử lý các vấn đề như: quyền ghi file, đọc định dạng HTML động, kiểm tra lỗi HTTP và tương thích Docker.


![Screenshot 2025-05-05 160241](https://github.com/user-attachments/assets/3587ecd5-7674-4b60-bf36-26f4cf663656)


### Exercise 3 - Boto3 AWS + s3 + Python.
#### 1. Mục tiêu bài tập:

- Làm quen với thư viện boto3 để tương tác với dịch vụ Amazon S3.
- Thao tác tải file .gz từ một bucket công khai (commoncrawl).
- Đọc dữ liệu .gz trực tiếp từ bộ nhớ thay vì ghi ra đĩa.
- Trích xuất và xử lý file WET được chỉ định trong dòng đầu tiên.
- Extra Credit: Tối ưu bộ nhớ bằng cách stream dữ liệu, không tải toàn bộ vào RAM hoặc lưu file tạm.

#### 2. Phân tích yêu cầu:
- Bước 1: Khởi tạo môi trường làm việc
- Bước 2: Kết nối với AWS S3 bằng boto3
- Bước 3: Tải và đọc file wet.paths.gz trực tiếp từ bộ nhớ
- Bước 4: Tải file WET tương ứng và stream nội dung

#### 3. Kết luận:

- Đọc thành công dòng đầu tiên từ file wet.paths.gz.
- Tải file .wet.gz tương ứng từ commoncrawl.
- In ra từng dòng nội dung từ file .wet.gz một cách hiệu quả, không tải toàn bộ vào bộ nhớ, đảm bảo đúng yêu cầu của phần Extra Credit.

![z6570760492704_1c4cdafaa1432e82d08fa8a9754dfd96](https://github.com/user-attachments/assets/fb71263d-400c-46bd-a085-3e96f72aae7b)


### Exercise 4 - Convert JSON to CSV + Ragged Directories.
#### 1. Mục tiêu bài tập:
- Bài tập yêu cầu viết một chương trình Python chạy trong môi trường Docker để thực hiện:
- Duyệt thư mục data/ và các thư mục con để tìm tất cả các file .json
- Đọc nội dung các file .json, "làm phẳng" (flatten) cấu trúc dữ liệu lồng nhau
- Ghi dữ liệu ra file .csv tương ứng (một file .csv cho mỗi file .json)

#### 2. Phân tích yêu cầu:
- Bước 1: Viết chương trình Python main.py
- Bước 2: Viết file docker-compose.yml
- Bước 3: Viết Dockerfile
- Bước 4: Build và chạy Docker

#### 3. Kết luận:

- Các file .csv được tạo ra tương ứng với từng file .json trong thư mục data
- Mỗi dòng trong file .csv đại diện cho một object JSON đã được làm phẳng
- Chương trình hoạt động đúng trên các file .json có cấu trúc lồng nhau


![Screenshot 2025-05-05 160229](https://github.com/user-attachments/assets/4801a5a9-efdb-495e-8974-867e0eaff7cb)


### Exercise 5 - Data Modeling for Postgres + Python.
#### 1. Mục tiêu bài tập:
- Hiểu cách thiết kế mô hình dữ liệu quan hệ từ dữ liệu CSV.
- Viết câu lệnh CREATE TABLE phù hợp với cấu trúc dữ liệu.
- Thiết lập các khóa chính, khóa ngoại và chỉ mục để tối ưu truy vấn.
- Sử dụng thư viện psycopg2 để kết nối và tương tác với PostgreSQL bằng Python.
- Tự động hóa việc nạp dữ liệu từ file CSV vào cơ sở dữ liệu thông qua Docker.

#### 2. Phân tích yêu cầu:
- Bước 1: Khám phá dữ liệu đầu vào (CSV)
- Bước 2: Thiết kế mô hình dữ liệu (schema.sql)
- Bước 3: Viết chương trình Python xử lý
- Bước 4: Thiết lập Docker và chạy chương trình

#### 3. Kết luận:
- Dữ liệu đã được nạp thành công.
- Các bảng liên kết đúng thông qua khóa ngoại.
- Việc sử dụng chỉ mục giúp tối ưu hóa các truy vấn sau này.

![z6570760884268_d4168892f1e0a18aaf2bcf81eded90c2](https://github.com/user-attachments/assets/4177047d-d9f3-4e05-a962-bc7a71d8925f)


### Exercise 6 - Ingestion and Aggregation with PySpark.
#### 1. Mục tiêu bài tập:
- Làm quen với quy trình xử lý dữ liệu lớn (Big Data) bằng PySpark.
- Làm việc với dữ liệu nén .zip chứa file .csv.
- Viết các truy vấn Spark để phân tích dữ liệu.
- Xuất kết quả thành các file .csv báo cáo.

#### 2. Phân tích yêu cầu:
- Bước 1: Xây dựng môi trường với Docker
  + Build Docker image
  + Chạy container và thực thi Spark job
- Bước 2: Mã nguồn xử lý dữ liệu – main.py
  + Đọc file zip và giải nén
  + Đọc dữ liệu CSV bằng PySpark
  + Xử lý và tạo báo cáo
- Bước 3: Xuất báo cáo

#### 3. Kết luận:

- Học được cách sử dụng PySpark để xử lý dữ liệu lớn.
- Thực hành tổ chức quy trình xử lý dữ liệu trong Docker.
- Thành thạo việc kết nối nhiều công cụ: Spark, Python, Docker, và Pandas/PySpark SQL.
- Biết cách debug lỗi môi trường (thư mục, biến môi trường, cấu hình Docker).


![Screenshot 2025-05-05 160300](https://github.com/user-attachments/assets/00a57f80-6f8e-4913-8ebc-c21ed5e1a27b)


### Exercise 7 - Using Various PySpark Functions
#### 1. Mục tiêu bài tập:

Sử dụng các hàm có sẵn trong pyspark.sql.functions (không sử dụng UDF hoặc hàm Python thuần) để thực hiện các thao tác phân tích dữ liệu về lỗi ổ cứng, bao gồm:
- Thêm thông tin nguồn file và ngày file.
- Tách nhãn hiệu từ model.
- Xếp hạng dung lượng lưu trữ theo model.
- Tạo khóa chính cho từng dòng dữ liệu.

#### 2. Phân tích yêu cầu:
- Bước 1: Chuẩn bị môi trường và cấu trúc dự án
- Bước 2: Cấu hình Docker
- Bước 3: Viết code trong main.py
- Bước 4 (Tuỳ chọn): Viết Unit Test với PyTest
- Bước 5: Build Docker Image
- Bước 6: Chạy chương trình chính
- Bước 7 (Tùy chọn): Chạy Unit Test

#### 3. Kết luận:
- In ra DataFrame với các cột mới: source_file, file_date, brand, storage_ranking, primary_key.
- Mọi xử lý không dùng UDF.
- Docker chạy ổn định, không lỗi build/run.


![image](https://github.com/user-attachments/assets/fff5e483-2d74-4d6c-b91a-741f87a749e8)



