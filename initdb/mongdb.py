import pymongo
import csv
import json
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# 尝试连接到MongoDB
try:
    password = quote_plus('root@123456')
    client = MongoClient(f'mongodb://root:{password}@127.0.0.1:27017/?authSource=admin')
    print('连接成功！')
except ConnectionFailure:
    print('连接失败，请检查MongoDB服务是否运行！')
    exit(1)

# 选择数据库和集合
db = client['movies']
collection = db['movies']

def process_json_field(field):
    """处理JSON字符串字段"""
    try:
        return json.loads(field)
    except:
        return field

# 读取CSV文件并处理数据
movies_data = []
with open('tmdb_5000_movies.csv', 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        # 处理JSON字符串字段
        row['genres'] = process_json_field(row['genres'])
        row['keywords'] = process_json_field(row['keywords'])
        row['production_companies'] = process_json_field(row['production_companies'])
        row['production_countries'] = process_json_field(row['production_countries'])
        row['spoken_languages'] = process_json_field(row['spoken_languages'])
        
        # 转换数值型字段
        row['budget'] = int(row['budget'])
        row['popularity'] = float(row['popularity'])
        row['revenue'] = int(row['revenue'])
        row['runtime'] = int(float(row['runtime'])) if row['runtime'] else None
        row['vote_average'] = float(row['vote_average'])
        row['vote_count'] = int(row['vote_count'])
        
        movies_data.append(row)

# 批量插入数据
try:
    result = collection.insert_many(movies_data)
    print(f'成功插入 {len(result.inserted_ids)} 条电影数据')
except Exception as e:
    print(f'插入数据时出错: {str(e)}')

# 关闭连接
client.close()
