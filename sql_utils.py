import re

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
