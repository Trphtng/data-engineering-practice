# Bài tập kĩ thuật dữ liệu
## Thông tin nhóm
1. Nguyễn Trường Phát 23697331 
2. Lê Trần Quang Nhân 23698431
   
## Lab 8
# Case 1: Xây dựng Pipeline tự động cào và trực quan hóa dữ liệu
- Phương pháp: Sử dụng mô hình ETL (Extract, Transform, Load) để thực hiện và sử dụng AirFlow để tự động hóa pipeline
- Các bước:
  
  B1: Thu thập dữ liệu (file csv, mySQL)
  
  B2: Viết script Python để xử lí trích xuất dữ liệu
  
  B3: Tải (Load) dữ liệu lên phần mềm PostgreSQL + Pandas
  
  B4: Trực quan hóa trên PowerBI
  
- Tổng kết:
    Dự án là một mô hình mô phỏng hệ thống ETL hoàn chỉnh trong lĩnh vực thương mại điện tử. Quy trình gồm các công đoạn từ trích xuất dữ liệu từ tệp hoặc cơ sở dữ liệu, xử lý và chuyển đổi dữ liệu, cho đến khâu nạp dữ liệu vào kho dữ liệu (Data Warehouse) sử dụng PostgreSQL hoặc MySQL. Apache Airflow được triển khai để tự động hóa và giám sát toàn bộ quá trình. Ngoài ra, hệ thống hỗ trợ tích hợp các plugin mở rộng, cho phép kết nối linh hoạt với nhiều loại cơ sở dữ liệu phổ biến khác.

# Case 2: Xây dựng Pipeline tự động cào dữ liệu và huấn luyện mô hình 
- Mục tiêu: Xây dựng và chạy thử được các DAGs cơ bản đến nâng cao.
- Công cụ: Sử dụng AirFlow để chạy Pipeline với cài đặt thời gian.


## Lab 9
# Mục tiêu:

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.


#### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests your ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.
`cd Exercises/Exercise-1` and see `README` in that location for instructions.

![z6570760647429_3f486af5dd911d108266d43edef81d0d](https://github.com/user-attachments/assets/06f1bca7-909e-4877-b361-99caf7972187)



#### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) 
tests your ability perform web scraping, build uris, download files, and use Pandas to
do some simple cumulative actions.
`cd Exercises/Exercise-2` and see `README` in that location for instructions.

![Screenshot 2025-05-05 160241](https://github.com/user-attachments/assets/3587ecd5-7674-4b60-bf36-26f4cf663656)


#### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills.
This time we  will be using a popular `aws` package called `boto3` to try to perform a multi-step
actions to download some open source `s3` data files.
`cd Exercises/Exercise-3` and see `README` in that location for instructions.

![z6570760492704_1c4cdafaa1432e82d08fa8a9754dfd96](https://github.com/user-attachments/assets/fb71263d-400c-46bd-a085-3e96f72aae7b)


#### Exercise 4 - Convert JSON to CSV + Ragged Directories.
The [fourth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-4) 
focuses more file types `json` and `csv`, and working with them in `Python`.
You will have to traverse a ragged directory structure, finding any `json` files
and converting them to `csv`.

![Screenshot 2025-05-05 160229](https://github.com/user-attachments/assets/4801a5a9-efdb-495e-8974-867e0eaff7cb)


#### Exercise 5 - Data Modeling for Postgres + Python.
The [fifth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-5) 
is going to be a little different than the rest. In this problem you will be given a number of
`csv` files. You must create a data model / schema to hold these data sets, including indexes,
then create all the tables inside `Postgres` by connecting to the database with `Python`.

![z6570760884268_d4168892f1e0a18aaf2bcf81eded90c2](https://github.com/user-attachments/assets/4177047d-d9f3-4e05-a962-bc7a71d8925f)


### Intermediate Exercises

#### Exercise 6 - Ingestion and Aggregation with PySpark.
The [sixth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-6) 
Is going to step it up a little and move onto more popular tools. In this exercise we are going
to load some files using `PySpark` and then be asked to do some basic aggregation.
Best of luck!

![Screenshot 2025-05-05 160300](https://github.com/user-attachments/assets/00a57f80-6f8e-4913-8ebc-c21ed5e1a27b)


#### Exercise 7 - Using Various PySpark Functions
The [seventh exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-7) 
Taking a page out of the previous exercise, this one is focus on using a few of the
more common build in PySpark functions `pyspark.sql.functions` and applying their
usage to real-life problems.

Many times to solve simple problems we have to find and use multiple functions available
from libraries. This will test your ability to do that.

![Screenshot 2025-05-05 163404](https://github.com/user-attachments/assets/510bb4bc-cde4-4429-bf30-2b47242b09ac)


#### Exercise 8 - Using DuckDB for Analytics and Transforms.
The [eighth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-8) 
Using new tools is imperative to growing as a Data Engineer. DuckDB is one of those new tools. In this
exercise you will have to complete a number of analytical and transformation tasks using DuckDB. This
will require an understanding of the functions and documenation of DuckDB.

#### Exercise 9 - Using Polars lazy computation.
The [ninth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-9) 
Polars is a new Rust based tool with a wonderful Python package that has taken Data Engineering by
storm. It's better than Pandas because it has both SQL Context and supports Lazy evalutation 
for larger than memory data sets! Show your Lazy skills!


### Advanced Exercises

#### Exercise 10 - Data Quality with Great Expectations
The [tenth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-10) 
This exercise is to help you learn Data Quality, specifically a tool called Great Expectations. You will
be given an existing datasets in CSV format, as well as an existing pipeline. There is a data quality issue 
and you will be asked to implement some Data Quality checks to catch some of these issues.
