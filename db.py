# db.py
import pymysql
from decimal import Decimal
import logging

# 创建表结构（包含自增主键 id）
create_table_sql = """
CREATE TABLE IF NOT EXISTS backward_adjusted (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- 自增主键
    stock_code VARCHAR(10) COMMENT '股票代码',
    stock_name VARCHAR(50) COMMENT '股票名称',
    date DATE COMMENT '交易日期',
    open DECIMAL(10, 2) COMMENT '开盘价',
    high DECIMAL(10, 2) COMMENT '最高价',
    low DECIMAL(10, 2) COMMENT '最低价',
    close DECIMAL(10, 2) COMMENT '收盘价',
    volume BIGINT COMMENT '成交量（股）',
    turnover DECIMAL(15, 2) COMMENT '成交额（元）'
) COMMENT='股票数据表，包含日线交易数据';
"""

# 创建表的函数
def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)
    print("Table `backward_adjusted` created or already exists.")

# 批量插入数据到数据库
def batch_insert_data(connection, data_to_insert):
    batch_size = 100  # 每批插入100条数据
    total_records = len(data_to_insert)

    with connection.cursor() as cursor:
        sql = """
        INSERT INTO backward_adjusted (
            stock_code, stock_name, date, open, high, low, close, volume, turnover
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # 将数据分批次插入
        for i in range(0, total_records, batch_size):
            batch_data = data_to_insert[i:i + batch_size]
            try:
                cursor.executemany(sql, batch_data)  # 批量插入
                connection.commit()  # 提交事务
            except Exception as e:
                logging.error(f"Error inserting batch {i}-{i+batch_size}: {e}")
                connection.rollback()  # 出现错误时回滚事务
                continue
