import pymysql
import pymongo
from pymongo.cursor import Cursor
from config import MYSQL_CONFIG, MONGODB_URI, MONGODB_DATABASE
import pandas as pd
from logger import get_logger

logger = get_logger()

class DatabaseManager:
    def __init__(self):
        self.mysql_conn = None
        self.mongo_client = None
        
    def connect_mysql(self):
        try:
            self.mysql_conn = pymysql.connect(**MYSQL_CONFIG)
            logger.info("Successfully connected to MySQL database")
            return True
        except Exception as e:
            logger.error(f"MySQL connection error: {e}")
            return False
            
    def connect_mongodb(self):
        try:
            self.mongo_client = pymongo.MongoClient(MONGODB_URI)
            logger.info("Successfully connected to MongoDB database")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            return False
            
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
            
    def get_mongodb_schema(self):
        """Get MongoDB database schema by sampling collections"""
        try:
            if not self.mongo_client:
                self.connect_mongodb()
                
            db = self.mongo_client[MONGODB_DATABASE]
            schema = []
            
            # Get all collections
            collections = db.list_collection_names()
            logger.info(f"Found {len(collections)} collections in MongoDB database")
            
            for collection_name in collections:
                collection = db[collection_name]
                # Sample one document to infer schema
                sample_doc = collection.find_one()
                
                if sample_doc:
                    schema.append(f"Collection: {collection_name}")
                    # Recursively process document fields
                    def process_fields(doc, prefix=""):
                        fields = []
                        for key, value in doc.items():
                            if key == '_id':
                                continue
                            field_type = type(value).__name__
                            if isinstance(value, dict):
                                sub_fields = process_fields(value, f"{prefix}{key}.")
                                fields.extend(sub_fields)
                            else:
                                fields.append(f"- {prefix}{key}: {field_type}")
                        return fields
                    
                    schema.extend(process_fields(sample_doc))
                    schema.append("")  # Empty line between collections
                    logger.info(f"Processed schema for collection: {collection_name}")
                    
            return "\n".join(schema)
        except Exception as e:
            logger.error(f"Error getting MongoDB schema: {e}")
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
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            with self.mysql_conn.cursor() as cursor:
                logger.info(f"Executing MySQL query: {query}")
                cursor.execute(query)
                
                # Check query type and return appropriate JSON response
                query_type = "SELECT"
                table_name = None
                
                if query.strip().upper().startswith("CREATE TABLE"):
                    query_type = "CREATE"
                    # Extract table name more reliably
                    table_name = query.split()[2].strip('`').split('(')[0]
                    logger.info(f"Creating new table: {table_name}")
                    return {
                        "status": "success",
                        "operation": "create_table",
                        "table_name": table_name,
                        "message": f"Table {table_name} created successfully"
                    }
                elif query.strip().upper().startswith("LOAD DATA"):
                    query_type = "LOAD"
                    # Extract table name from LOAD DATA INFILE query
                    table_name = query.split("INTO TABLE")[1].split()[0].strip('`')
                    logger.info(f"Loading data into table: {table_name}")
                    return {
                        "status": "success",
                        "operation": "load_data",
                        "table_name": table_name,
                        "message": f"Data loaded successfully into {table_name}"
                    }
                elif "-- 无需创建新表" in query:
                    # Extract table name more reliably
                    table_name = None
                    if "`" in query:
                        table_name = query.split("`")[1]
                    elif "INTO" in query.upper():
                        table_name = query.upper().split("INTO")[1].split()[0].strip('`')
                    logger.info(f"Using existing table: {table_name}")
                    return {
                        "status": "success",
                        "operation": "no_create",
                        "table_name": table_name,
                        "message": f"Using existing table {table_name}"
                    }
                
                # For SELECT queries
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return {
                    "status": "success",
                    "operation": "select",
                    "columns": columns,
                    "data": data,
                    "row_count": len(data)
                }
        except Exception as e:
            return {
                "status": "error",
                "operation": query_type,
                "message": str(e)
            }

    def execute_mongodb_query(self, query_str):
        """Execute a MongoDB query string and return formatted results"""
        try:
            if not self.mongo_client:
                self.connect_mongodb()
                
            db = self.mongo_client[MONGODB_DATABASE]
            logger.info(f"Executing MongoDB query: {query_str}")
            
            # Execute query and format results
            result = eval(f"db.{query_str}")
            if isinstance(result, Cursor):
                data = list(result)
                if not data:
                    return "未找到数据"
                columns = list(data[0].keys())
                rows = [[doc.get(col) for col in columns] for doc in data]
                return self._format_query_results(columns, rows)
            return str(result)
        except Exception as e:
            raise Exception("Error executing MongoDB query: " + str(e))
            
    def close_connections(self):
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("Closed MySQL connection")
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Closed MongoDB connection")

    def insert_from_df(self, table_name, df):
        """Insert data from DataFrame into MySQL table"""
        try:
            if not self.mysql_conn or not self.mysql_conn.open:
                self.connect_mysql()
                
            # Clean data - replace NaN with None (NULL in MySQL)
            df = df.where(pd.notnull(df), None)
            logger.info(f"Cleaned data for table {table_name}: {df.head().to_dict()}")
                
            # Create SQL placeholders
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
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

    def process_csv(self, file_path, collection_name=None):
        """Process CSV file and insert data into MongoDB"""
        try:
            if not self.mongo_client:
                self.connect_mongodb()
            
            db = self.mongo_client[MONGODB_DATABASE]
            
            # Read CSV file
            df = pd.read_csv(file_path)
            headers = df.columns.tolist()
            
            # Determine collection name
            if not collection_name:
                collection_name = os.path.splitext(os.path.basename(file_path))[0]
            
            collection = db[collection_name]
            
            # Check if collection exists
            if collection_name in db.list_collection_names():
                # Get existing fields
                sample_doc = collection.find_one()
                existing_fields = set(sample_doc.keys()) if sample_doc else set()
                
                # Check field compatibility
                new_fields = set(headers)
                if not new_fields.issubset(existing_fields):
                    logger.warning(f"New fields detected: {new_fields - existing_fields}")
                    # Update schema to include new fields
                    collection.update_many(
                        {},
                        {"$set": {field: None for field in new_fields - existing_fields}}
                    )
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Insert data
            result = collection.insert_many(data)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            return False
