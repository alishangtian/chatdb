import csv
import json
import pymysql
import logging
from datetime import datetime
from pymysql.err import OperationalError
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def validate_date(date_str):
    """验证并转换日期格式"""
    if not date_str or date_str.strip() == '':
        logging.info(f"发现空日期值")
        return None
    try:
        # 尝试解析日期字符串
        date_str = date_str.strip()
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        logging.info(f"成功解析日期: {date_str} -> {parsed_date}")
        return parsed_date
    except ValueError as e:
        logging.warning(f"警告: 无效的日期格式 '{date_str}', 错误: {str(e)}")
        return None

# 尝试连接到MySQL
try:
    connection = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='root@123456',
        database='movies',
        charset='utf8mb4'
    )
    logging.info('连接成功！')
except OperationalError as e:
    logging.error(f'连接失败: {str(e)}')
    exit(1)

# 创建游标
cursor = connection.cursor()

# 创建movies表
create_table_sql = """
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    budget BIGINT,
    genres JSON,
    homepage VARCHAR(255),
    keywords JSON,
    original_language VARCHAR(10),
    original_title VARCHAR(255),
    overview TEXT,
    popularity FLOAT,
    production_companies JSON,
    production_countries JSON,
    release_date DATE,
    revenue BIGINT,
    runtime INT,
    spoken_languages JSON,
    status VARCHAR(50),
    tagline TEXT,
    title VARCHAR(255),
    vote_average FLOAT,
    vote_count INT
)
"""

try:
    cursor.execute(create_table_sql)
    connection.commit()
    logging.info('成功创建表！')
except Exception as e:
    logging.error(f'创建表时出错: {str(e)}')
    connection.close()
    exit(1)

def process_json_field(field):
    """处理JSON字符串字段"""
    try:
        return json.dumps(json.loads(field))  # 确保是有效的JSON字符串
    except:
        return field

# 清空表
try:
    cursor.execute("TRUNCATE TABLE movies")
    connection.commit()
    logging.info('成功清空表！')
except Exception as e:
    logging.error(f'清空表时出错: {str(e)}')
    connection.close()
    exit(1)

logging.info("开始读取CSV文件...")
# 读取CSV文件并处理数据
movies_data = []
with open('tmdb_5000_movies.csv', 'r', encoding='utf-8') as file:
    logging.info("成功打开CSV文件")
    csv_reader = csv.DictReader(file)
    row_count = 0
    for row in csv_reader:
        row_count += 1
        logging.info(f"\n处理第 {row_count} 行数据:")
        # 处理JSON字符串字段
        logging.info(f"原始release_date值: '{row['release_date']}'")
        processed_row = (
            int(row['budget']),
            process_json_field(row['genres']),
            row['homepage'],
            process_json_field(row['keywords']),
            row['original_language'],
            row['original_title'],
            row['overview'],
            float(row['popularity']),
            process_json_field(row['production_companies']),
            process_json_field(row['production_countries']),
            validate_date(row['release_date']),
            int(row['revenue']),
            int(float(row['runtime'])) if row['runtime'] else None,
            process_json_field(row['spoken_languages']),
            row['status'],
            row['tagline'],
            row['title'],
            float(row['vote_average']),
            int(row['vote_count'])
        )
        movies_data.append(processed_row)

# 批量插入数据
insert_sql = """
INSERT INTO movies (
    budget, genres, homepage, keywords, original_language,
    original_title, overview, popularity, production_companies,
    production_countries, release_date, revenue, runtime,
    spoken_languages, status, tagline, title, vote_average, vote_count
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    total = len(movies_data)
    logging.info(f"\n总共读取了 {total} 条数据")
    success = 0
    failed = 0
    
    logging.info("\n开始插入数据...")
    with tqdm(total=total, desc="插入进度", unit="条") as pbar:
        for i, data in enumerate(movies_data, 1):
            try:
                cursor.execute(insert_sql, data)
                success += 1
                if i % 100 == 0:
                    connection.commit()
                    logging.info(f'进度: {i}/{total}, 成功: {success}, 失败: {failed}')
                pbar.update(1)
            except Exception as e:
                failed += 1
                logging.error(f'\n插入第 {i} 条数据时出错:')
                logging.error(f'数据内容: {data}')
                logging.error(f'错误信息: {str(e)}\n')
                connection.rollback()
                pbar.update(1)
                continue
    
    connection.commit()
    logging.info(f'\n数据插入完成')
    logging.info(f'总数: {total}, 成功: {success}, 失败: {failed}')
except Exception as e:
    logging.error(f'执行过程中出错: {str(e)}')
    connection.rollback()

# 关闭连接
cursor.close()
connection.close()

def get_table_structure():
    """获取数据库中所有表的结构"""
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root@123456',
            database='movies',
            charset='utf8mb4'
        )
        cursor = connection.cursor()
        
        # 获取所有表名
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        table_structure = {}
        for table in tables:
            table_name = table[0]
            # 获取表结构
            cursor.execute(f"DESCRIBE {table_name}")
            structure = cursor.fetchall()
            table_structure[table_name] = structure
            
        cursor.close()
        connection.close()
        return table_structure
        
    except Exception as e:
        logging.error(f'获取表结构时出错: {str(e)}')
        return None
