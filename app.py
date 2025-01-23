import gradio as gr
import re
from langchain_ollama import ChatOllama
from langchain.prompts import PromptTemplate
from operator import itemgetter
from database import DatabaseManager
from config import OLLAMA_API_URL
from config import OLLAMA_CHAT_MODEL
from config import OLLAMA_CODE_MODEL
from logger import get_logger

logger = get_logger()

# Initialize ChatOllama for chat responses
llm = ChatOllama(
    base_url=OLLAMA_API_URL,
    model=OLLAMA_CHAT_MODEL,
    temperature=0
)
# Initialize ChatOllama for code responses
code_llm = ChatOllama(
    base_url=OLLAMA_API_URL,
    model=OLLAMA_CODE_MODEL,
    temperature=0
)

def format_llm_response(response):
    """Format LLM response to ensure it's a string"""
    if hasattr(response, 'content'):
        return response.content
    elif isinstance(response, dict) and 'content' in response:
        return response['content']
    elif isinstance(response, str):
        return response
    else:
        return str(response)

# Create prompt template for determining if database query is needed
need_db_prompt = PromptTemplate(
    input_variables=["question", "schema"],
    template="""
    你是一个AI助手，需要判断是否需要查询数据库来回答用户的问题。
    分析以下问题和可用的数据库结构，判断是否需要查询数据库来获取数据以提供答案。
    如果需要查询数据库来获取数据，请只回答"true"；
    如果不需要查询数据库就能回答，请只回答"false"。
    
    数据库结构：
    {schema}
    
    问题：{question}
    """
)

# Create prompt template for SQL generation
sql_prompt = PromptTemplate(
    input_variables=["question", "db_type", "schema"],
    template="""
    你是一个SQL专家。请将以下自然语言问题转换为{db_type}查询语句。
    只返回SQL查询语句，不需要任何解释。
    
    数据库结构：
    {schema}
    
    问题：{question}
    SQL查询：
    """
)

# Create prompt template for final answer
answer_prompt = PromptTemplate(
    input_variables=["question", "data"],
    template="""
    基于提供的数据回答以下问题。
    
    问题：{question}
    数据：{data}
    """
)

# Create chains using RunnableSequence
need_db_chain = (
    {
        "question": itemgetter("question"),
        "schema": itemgetter("schema")
    }
    | need_db_prompt
    | llm
)

sql_chain = (
    {
        "question": itemgetter("question"),
        "db_type": itemgetter("db_type"),
        "schema": itemgetter("schema")
    }
    | sql_prompt
    | code_llm
)

answer_chain = (
    {
        "question": itemgetter("question"),
        "data": itemgetter("data")
    }
    | answer_prompt
    | llm
)

# Initialize database manager
db_manager = DatabaseManager()

def get_schema(db_type):
    """Get database schema based on type"""
    return db_manager.get_mysql_schema()

def table_creation_prompt(query):
    """处理表创建提示，返回固定格式的JSON响应"""
    if "-- 无需创建新表" in query:
        # 从查询中提取表名
        table_name = query.split("`")[1] if "`" in query else None
        return {
            "status": "success",
            "operation": "no_create",
            "table_name": table_name,
            "message": f"Using existing table {table_name}"
        }
    else:
        # 从CREATE TABLE语句中提取表名
        table_name = query.split()[2].strip('`')
        return {
            "status": "success",
            "operation": "create_table",
            "table_name": table_name,
            "message": f"Table {table_name} created successfully"
        }

def process_query(question, db_type):
    logger.info(f"Processing query - Question: {question}, DB Type: {db_type}")
    try:
        # Get schema first since we need it for both determination and query
        logger.info("Fetching database schema")
        schema = get_schema(db_type)
        if not schema:
            logger.info("Failed to get database schema")
            yield "需要数据库Schema", "", "Failed to get database schema"
            return

        # First determine if database query is needed
        logger.info("Determining if database query is needed")
        need_db_response = format_llm_response(need_db_chain.invoke({
            "question": question,
            "schema": schema
        }))
        needs_db = need_db_response.strip().lower() == "true"
        need_db_text = "需要查询数据库" if needs_db else "不需要查询数据库"
        
        if not needs_db:
            # If no database query needed, directly answer the question
            logger.info("No database query needed, generating direct answer")
            answer = ""
            # Validate input data before streaming
            if not question:
                raise ValueError("Question cannot be empty")
                
            input_data = {
                "question": question,
                "data": "No database query needed"
            }
            
            if not isinstance(input_data, dict) or "question" not in input_data or "data" not in input_data:
                raise ValueError("Invalid input data format for answer_chain")
                
            for chunk in answer_chain.stream(input_data):
                answer += format_llm_response(chunk)
                yield need_db_text, "无需SQL查询", [(None, answer)]
            return
        
        # If database query is needed, proceed with query generation
        logger.info("Database query needed, proceeding with query generation")
            
        # Generate SQL query
        response = format_llm_response(sql_chain.invoke({
            "question": question,
            "db_type": db_type,
            "schema": schema
        }))

        logger.info(f"SQL generate chain result: {response}")
        
        # Extract SQL query
        from sql_utils import extract_sql
        sql_response = extract_sql(response)
        
        # Initialize retry counter and error message
        retry_count = 0
        last_error = None
        max_retries = 5
        
        while True:
            try:
                # Execute query
                logger.info(f"Executing query (attempt {retry_count + 1})")
                data_str = db_manager.execute_mysql_query(sql_response)
                logger.info(f"Query executed successfully result: {data_str}")
                answer = ""
                # Validate input data before streaming
                if not question or not data_str:
                    raise ValueError("Question and data cannot be empty")
                    
                input_data = {
                    "question": question,
                    "data": data_str
                }
                
                if not isinstance(input_data, dict) or "question" not in input_data or "data" not in input_data:
                    raise ValueError("Invalid input data format for answer_chain")
                    
                for chunk in answer_chain.stream(input_data):
                    answer += format_llm_response(chunk)
                    yield need_db_text, sql_response, [(None, answer)]
                return
                    
            except Exception as e:
                last_error = str(e)
                retry_count += 1
                if retry_count >= max_retries:
                    logger.info(f"Max retries reached. Last error: {last_error}")
                    answer = ""
                    # Validate input data before streaming
                    if not question or not last_error:
                        raise ValueError("Question and error message cannot be empty")
                        
                    input_data = {
                        "question": question,
                        "data": f"Error executing query: {last_error}"
                    }
                    
                    if not isinstance(input_data, dict) or "question" not in input_data or "data" not in input_data:
                        raise ValueError("Invalid input data format for answer_chain")
                        
                    for chunk in answer_chain.stream(input_data):
                        answer += format_llm_response(chunk)
                        yield need_db_text, sql_response, [(None, answer)]
                    return
                
                logger.info(f"Query error (attempt {retry_count}): {last_error}")
                # Regenerate SQL with error context
                response = format_llm_response(sql_chain.invoke({
                    "question": question,
                    "db_type": db_type, 
                    "schema": schema,
                    "error": last_error
                }))
                # Extract SQL query
                sql_response = extract_sql(response)

    except Exception as e:
        logger.info(f"Error processing query: {str(e)}", exc_info=True)
        yield "处理出错", "", [(None, f"Error: {str(e)}")]

# Create prompt template for table creation
table_creation_prompt_template = PromptTemplate(
    input_variables=["schema", "csv_columns", "csv_sample"],
    template="""
    你是一个mysql数据库专家。请根据已存在的表信息判断是否需要创建新表，并生成相应的SQL语句。
    切记：
    -- 当待插入CSV数据的列名和已存在表的字段列名称不一致时，需要创建新表，并返回建表sql，
    -- 切记：字段Pack_Years和Pack Years需要认为是相同的字段，忽略大小写、空格和驼峰格式的差异。
    返回如下
    ```sql
    CREATE TABLE `table_name` (
        column_name data_type [PRIMARY KEY] [UNIQUE],
        ...
    );
    ```
    -- 如果不需要创建新表，返回如下
    ```sql
    无需创建新表，使用表`table_name`
    ```  
    
    现有数据库表结构：
    {schema}
    
    待插入CSV数据的列名：
    {csv_columns}
    
    待插入CSV数据的前两行数据：
    {csv_sample}

    建表语句字段推断规则：
       - 如果值只包含数字：INT
       - 如果值包含数字和小数点：DECIMAL(10,2)
       - 如果值包含日期格式：DATE
       - 如果值长度超过255：TEXT
       - 其他情况：VARCHAR(255)
    """
)

def process_upload(file, table_name=None):
    """Process uploaded CSV file"""
    try:
        if not file:
            return "请选择要上传的文件"
            
        import pandas as pd
        from sql_utils import create_table_from_sql, extract_sql, extract_table_name
        import os
        
        # Read CSV file
        logger.info("开始读取CSV文件")
        
        # If no table name provided, generate from filename
        if not table_name:
            table_name = os.path.splitext(os.path.basename(file.name))[0]
            logger.info(f"从文件名生成初始表名：{table_name}")
        import io
        if hasattr(file, 'read'):  # Handle file-like object
            content = file.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            df = pd.read_csv(io.StringIO(content))
            logger.info(f"从文件对象读取CSV成功，共{len(df)}行")
        elif hasattr(file, 'name'):  # Handle file path
            df = pd.read_csv(file.name)
            logger.info(f"从文件路径读取CSV成功，共{len(df)}行")
        else:
            logger.error("不支持的文件类型")
            raise ValueError("Unsupported file type")
        
        # Get column names and types
        columns = df.columns.tolist()
        dtypes = df.dtypes.astype(str).tolist()
        logger.info(f"获取列信息成功，\n 列名：{columns}，类型：{dtypes}")
        
        # Create table name from filename
        table_name = os.path.splitext(os.path.basename(file.name))[0]
        logger.info(f"从文件名生成初始表名：{table_name}")
        
        # Get database schema
        logger.info("获取数据库schema")
        schema = db_manager.get_mysql_schema()
        
        # Get CSV sample data
        csv_sample = df.head(2).values.tolist()
        logger.info(f"获取CSV样本数据：\n{csv_sample}")
        
        # Ask model to determine table creation
        logger.info("开始生成表创建SQL")
        formated_prompt = table_creation_prompt_template.format(
            schema=schema,
            csv_columns=columns,
            csv_sample=csv_sample
        )
        logger.info(f"生成表创建SQL提示：\n {formated_prompt}")
        response = format_llm_response(llm.invoke(formated_prompt))
        logger.info(f"模型返回结果：\n {response}")
        
        # Extract SQL from response
        sql = extract_sql(response)
        logger.info(f"提取SQL成功：{sql}")
        
        # Execute table creation if needed
        if "CREATE TABLE" in sql:
            table_name = extract_table_name(sql)
            if not table_name:
                logger.error("无法确定表名")
                return f"无法确定表名 sql: {sql}"
            # Create table and get result
            logger.info(f"开始创建表：{table_name}")
            created_table = create_table_from_sql(sql)
            if not created_table:
                logger.error("创建表失败")
                return "创建表失败"
            logger.info(f"表创建成功：{table_name}")
        else:
            # Extract table name from existing schema
            table_name_match = re.search(r"无需创建新表，使用表`([^`]+)`", sql)
            if table_name_match:
                table_name = table_name_match.group(1)
                logger.info(f"使用现有表：{table_name}")
            else:
                logger.error("无法确定表名")
                return "无法确定表名"
                
        logger.info(f"最终使用表：{table_name}")
        
        # Insert data using insert_from_df which explicitly handles DataFrames
        logger.info(f"开始插入数据到表{table_name}")
        result = db_manager.insert_from_df(table_name, df)
        
        if result and result.get("status") == "success":
            logger.info(f"数据插入成功，共插入{len(df)}行")
            return f"文件上传成功，数据已插入表{table_name}"
        else:
            logger.error("文件上传失败")
            return "文件上传失败"
    except TypeError as e:
        return f"上传出错: 类型错误 - {str(e)}"
    except Exception as e:
        return f"上传出错: {type(e).__name__} - {str(e)}"

# Create Gradio interface
with gr.Blocks() as app:
    gr.Markdown("# 自然语言查询助手")
    
    with gr.Tab("数据上传"):
        with gr.Row():
            with gr.Column():
                file_input = gr.File(
                    label="上传CSV文件",
                    file_types=[".csv"]
                )
                # 添加表名下拉选择
                table_name_dropdown = gr.Dropdown(
                    label="选择表名",
                    choices=db_manager.get_table_names(),
                    interactive=True,
                    allow_custom_value=True
                )
                upload_btn = gr.Button("上传")
            with gr.Column():
                upload_output = gr.Textbox(
                    label="上传结果", 
                    lines=2
                )
        upload_btn.click(
            fn=process_upload,
            inputs=[file_input, table_name_dropdown],
            outputs=upload_output
        )
    
    with gr.Row():
        with gr.Column():
            question = gr.Textbox(
                label="请输入您的问题",
                placeholder="例如：显示各产品类别的总销售额"
            )
            
            db_type = gr.Radio(
                choices=["MySQL"],
                label="选择数据库类型",
                value="MySQL"
            )
            
            submit_btn = gr.Button("获取答案")
            
        with gr.Column():
            need_db_output = gr.Textbox(
                label="1. 判断是否需要查询数据库",
                lines=2
            )
            sql_output = gr.Textbox(
                label="2. 生成的SQL查询",
                lines=3
            )
            output_text = gr.Chatbot(
                label="3. 最终答案",
                height=300
            )
            
    submit_btn.click(
        fn=process_query,
        inputs=[question, db_type],
        outputs=[need_db_output, sql_output, output_text],
        api_name="process_query",
        queue=True
    )

if __name__ == "__main__":
    logger.info("Starting NLP2SQL application")
    try:
        app.launch(server_name="0.0.0.0", server_port=8860, share=True)
    except Exception as e:
        logger.info(f"Application error: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down NLP2SQL application")
