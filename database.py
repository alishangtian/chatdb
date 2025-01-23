import pymysql
from config import MYSQL_CONFIG
import pandas as pd
from logger import get_logger

logger = get_logger()

class DatabaseManager:
    def __init__(self):
        self.mysql_conn = None
        
    def connect_mysql(self):
        try:
            self.mysql_conn = pymysql.connect(**MYSQL_CONFIG)
            logger.info("Successfully connected to MySQL database")
            return True
        except Exception as e:
            logger.error(f"MySQL connection error: {e}")
            return False
            
    def get_table_names(self):
        """获取数据库中所有表名"""
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            with self.mysql_conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                return [table[0] for table in tables]
        except Exception as e:
            logger.error(f"Error getting table names: {e}")
            return []

    def get_mysql_schema(self):
        """Get MySQL database schema information"""
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            schema = []
            with self.mysql_conn.cursor() as cursor:
                # Get all tables
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %s
                """, (MYSQL_CONFIG['database'],))
                
                tables = cursor.fetchall()
                logger.info(f"Found {len(tables)} tables in MySQL database")
                
                # Get columns for each table
                for table in tables:
                    table_name = table[0]
                    cursor.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    """, (MYSQL_CONFIG['database'], table_name))
                    
                    columns = cursor.fetchall()
                    table_schema = [
                        f"- {col[0]} ({col[1]}"
                        f"{', primary key' if col[2] == 'PRI' else ''}"
                        f"{', nullable' if col[3] == 'YES' else ''})"
                        for col in columns
                    ]
                    
                    schema.append(f"Table: {table_name}")
                    schema.extend(table_schema)
                    schema.append("")  # Empty line between tables
                    
                    logger.info(f"Processed schema for table: {table_name}")
                    
            return "\n".join(schema)
        except Exception as e:
            logger.error(f"Error getting MySQL schema: {e}")
            return ""
            
    def _format_query_results(self, columns, data):
        """Helper function to format query results into text"""
        result_lines = []
        # Add header
        result_lines.append(f"总计 {len(data)} 条记录")
        result_lines.append("")
        # Add column names
        result_lines.append(" | ".join(str(col) for col in columns))
        result_lines.append("-" * (sum(len(str(col)) for col in columns) + 3 * (len(columns) - 1)))
        # Add data rows
        for row in data:
            result_lines.append(" | ".join(str(val) for val in row))
        return "\n".join(result_lines)

    def execute_mysql_query(self, query):
        """执行SQL查询并返回格式化文本结果"""
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            with self.mysql_conn.cursor() as cursor:
                logger.info(f"Executing MySQL query: {query}")
                cursor.execute(query)
                
                # 获取查询结果
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    return self._format_query_results(columns, data)
                else:
                    # 对于非查询语句，返回受影响行数
                    return f"Query executed successfully. Affected rows: {cursor.rowcount}"
        except Exception as e:
            logger.error(f"Error executing query: {query}")
            raise e

    def close_connections(self):
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("Closed MySQL connection")

    def insert_from_df(self, table_name, df):
        """Insert data from DataFrame into MySQL table without specifying column names"""
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            # Clean data - replace NaN with None (NULL in MySQL)
            df = df.where(pd.notnull(df), None)
            logger.info(f"Cleaned data for table {table_name}: {df.head().to_dict()}")
                
            # Create SQL placeholders
            placeholders = ', '.join(['%s'] * len(df.columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.values]
            
            # Execute batch insert
            with self.mysql_conn.cursor() as cursor:
                cursor.executemany(sql, data)
                self.mysql_conn.commit()
                logger.info(f"Inserted {len(data)} rows into {table_name}")
                return {
                    "status": "success",
                    "operation": "insert",
                    "table_name": table_name,
                    "row_count": len(data),
                    "message": f"Successfully inserted {len(data)} rows into {table_name}"
                }
                
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            return {
                "status": "error",
                "operation": "insert",
                "table_name": table_name,
                "message": str(e)
            }
