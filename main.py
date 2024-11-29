# main.py
import os
import pymysql
from tqdm import tqdm
import logging
from config import db_config
from db import create_table
from import_txt import import_txt_to_mysql

# 遍历文件夹并上传文件
def process_files_in_folder(folder_path, connection, import_function):
    all_files = [f for f in os.listdir(folder_path) if f.endswith(".txt")]
    total_files = len(all_files)  # 文件总数

    # 使用 tqdm 显示文件上传进度
    with tqdm(total=total_files, desc="Uploading Files", ncols=100) as pbar:
        for file_name in all_files:
            file_path = os.path.join(folder_path, file_name)
            import_function(file_path, connection)  # 上传文件
            pbar.update(1)  # 每上传一个文件，进度条更新一次

    print("All files have been processed successfully.")

# 主函数
def main(folder_path):
    connection = pymysql.connect(**db_config)
    try:
        create_table(connection)  # 创建表
        process_files_in_folder(folder_path, connection, import_txt_to_mysql)  # 上传txt文件
    finally:
        connection.close()

# 调用主函数
if __name__ == "__main__":
    folder_path = "backward_adjusted"  # 替换为你的文件夹路径
    main(folder_path)
