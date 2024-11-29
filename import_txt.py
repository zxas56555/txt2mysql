# import_txt.py
import logging
from decimal import Decimal

def import_txt_to_mysql(file_path, connection):
    with open(file_path, 'r', encoding='GBK') as file:
        first_line = file.readline().strip()
        stock_code, stock_name = first_line.split()[:2]  # 提取股票代码和名称

        # 跳过第二行表头
        file.readline()

        # 用于批量插入的列表
        data_to_insert = []

        for line in file:
            line = line.strip()
            if line:  # 跳过空行
                fields = line.split(",")
                if len(fields) < 7:
                    logging.warning(f"Skipping invalid line in file {file_path}: {line}")
                    continue  # 如果字段不足，跳过该行

                try:
                    date = fields[0]
                    open_price = Decimal(fields[1])
                    high_price = Decimal(fields[2])
                    low_price = Decimal(fields[3])
                    close_price = Decimal(fields[4])
                    volume = int(fields[5])
                    turnover = Decimal(fields[6])

                    data_to_insert.append((stock_code, stock_name, date, open_price, high_price, low_price, close_price,
                                           volume, turnover))
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipping invalid data in file {file_path}: {line} ({e})")
                    continue  # 如果发生错误，跳过该行

    # 批量插入数据
    from db import batch_insert_data
    batch_insert_data(connection, data_to_insert)
