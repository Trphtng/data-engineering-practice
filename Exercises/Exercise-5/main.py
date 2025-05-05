import psycopg2
import csv
import os

DATA_DIR = "./data"

def create_tables(cur):
    with open("schema.sql", "r") as f:
        sql = f.read()
        cur.execute(sql)
        print("✅ Đã tạo bảng thành công")

def insert_data_from_csv(cur, table_name, csv_file):
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = [tuple(row) for row in reader]

        placeholders = ','.join(['%s'] * len(headers))
        columns = ','.join(headers)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for row in rows:
            cur.execute(insert_query, row)
        print(f"✅ Đã chèn dữ liệu vào bảng {table_name} từ {csv_file}")

def main():
    host = "postgres"  # Docker Compose sẽ map đúng tên service này
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    create_tables(cur)

    insert_data_from_csv(cur, "accounts", os.path.join(DATA_DIR, "accounts.csv"))
    insert_data_from_csv(cur, "products", os.path.join(DATA_DIR, "products.csv"))
    insert_data_from_csv(cur, "transactions", os.path.join(DATA_DIR, "transactions.csv"))

    conn.commit()
    cur.close()
    conn.close()
    print("🎉 Hoàn thành!")

if __name__ == "__main__":
    main()
