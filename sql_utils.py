import re
from typing import List
import pandas as pd
from database import DatabaseManager

def extract_sql(text: str) -> str:
    """
    从文本中提取SQL语句。
    支持两种格式：
    1. ```sql ... ``` 格式
    2. 直接包含SQL语句的文本
    
    Args:
        text: 包含SQL语句的文本
        
    Returns:
        str: 提取出的SQL语句
    """
    # 首先尝试匹配```sql格式
    sql_block_pattern = r"```sql\n(.*?)\n```"
    sql_block_match = re.search(sql_block_pattern, text, re.DOTALL)
    if sql_block_match:
        return sql_block_match.group(1).strip()
    
    # 如果不是```sql格式，则尝试直接匹配SQL语句
    sql_pattern = r'(?i)(?:select|insert|update|delete|create|alter|drop|truncate|grant|revoke|merge|replace|call|explain).*?(?:;|\Z)'
    sql_match = re.search(sql_pattern, text, re.DOTALL)
    if sql_match:
        return sql_match.group(0).strip()
    
    return text.strip()

def create_table_from_sql(sql: str) -> bool:
    """
    根据模型返回的SQL语句创建MySQL表
    
    Args:
        sql: 模型返回的创建表SQL语句
        
    Returns:
        bool: 是否创建成功
    """
    # 提取表名用于日志记录
    table_name = extract_table_name(sql)
    if table_name:
        print(f"Creating table: {table_name}")
    
    db_manager = DatabaseManager()
    return db_manager.execute_mysql_query(sql)

def extract_table_name(sql: str) -> str:
    """
    从CREATE TABLE语句中提取表名
    
    Args:
        sql: CREATE TABLE SQL语句
        
    Returns:
        str: 提取出的表名
    """
    # 匹配带引号和不带引号的表名
    pattern = r"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:'([^']+)'|([^\s\(]+))"
    match = re.search(pattern, sql)
    if match:
        # 返回带引号或不带引号的表名
        return match.group(1) or match.group(2)
    return None

def add_columns_to_table(table_name: str, columns: List[str], dtypes: List[str]) -> bool:
    """
    向现有表添加新列
    
    Args:
        table_name: 表名
        columns: 要添加的列名列表
        dtypes: 列类型列表
        
    Returns:
        bool: 是否添加成功
    """
    db_manager = DatabaseManager()
    
    # 映射pandas dtype到MySQL类型
    type_mapping = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(255)',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'DATETIME'
    }
    
    # 生成ALTER TABLE语句
    alter_statements = []
    for col, dtype in zip(columns, dtypes):
        mysql_type = type_mapping.get(dtype, 'VARCHAR(255)')
        alter_statements.append(f"ADD COLUMN `{col}` {mysql_type}")
    
    # 如果没有任何列要添加
    if not alter_statements:
        return True
        
    alter_sql = f"""
    ALTER TABLE `{table_name}`
    {', '.join(alter_statements)};
    """
    
    return db_manager.execute_mysql_query(alter_sql)
