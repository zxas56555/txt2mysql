import pandas as pd
import mysql.connector
from mysql.connector import Error

def read_excel(file_path):
    """
    读取Excel文件并返回清洗后的DataFrame。
    """
    try:
        # 指定列类型，确保股票代码列保留原始格式
        df = pd.read_excel(file_path, dtype={"code": str})
        # 映射列名到数据库字段
        df.columns = [
            "code", "name", "change_pct", "current_price", "price_change",
            "buy_price", "sell_price", "total_amount", "current_amount",
            "speed_pct", "turnover_pct", "today_open", "high_price", "low_price",
            "yesterday_close", "pe_ratio", "total_value", "volume_ratio",
            "industry", "region", "amplitude_pct", "average_price",
            "inner_trade", "outer_trade", "inner_outer_ratio"
        ]
        # 确保股票代码为6位，补齐前导零
        df["code"] = df["code"].astype(str).str.zfill(6)
        # 替换空值为None (对应MySQL中的NULL)
        df = df.replace({pd.NA: None, pd.NaT: None, float("nan"): None})
        df = df.where(pd.notnull(df), None)  # 再次确认替换NaN为None
        return df
    except Exception as e:
        print(f"读取Excel文件出错: {e}")
        return None

def insert_into_mysql(df, table_name, db_config):
    """
    将DataFrame中的数据插入到MySQL表中。
    """
    try:
        # 连接数据库
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            print("成功连接到数据库")

            # 构建插入SQL语句
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 插入数据
            successful_inserts = 0
            for _, row in df.iterrows():
                try:
                    # 确保所有值是基础类型
                    cursor.execute(insert_sql, tuple(row))
                    successful_inserts += 1
                except Error as e:
                    print(f"插入数据出错: {e}，跳过该行数据")

            # 提交事务
            connection.commit()
            print(f"成功插入 {successful_inserts} 条记录")
    except Error as e:
        print(f"MySQL错误: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    # Excel文件路径
    excel_file = "全部Ａ股20241123.xlsx"  # 请替换为实际文件路径

    # MySQL数据库配置
    db_config = {
        "host": "192.168.0.195",  # 数据库地址
        "user": "stock_analysis",  # 数据库用户名
        "password": "SDmx@2024",  # 数据库密码
        "database": "stock_analysis",  # 数据库名称
    }

    # 数据库表名称
    table_name = "stock_data"  # 表名

    # 读取Excel文件
    df = read_excel(excel_file)
    if df is not None:
        # 将数据插入到MySQL
        insert_into_mysql(df, table_name, db_config)
