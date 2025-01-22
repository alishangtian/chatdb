import gradio as gr
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
    if db_type.lower() == "mysql":
        return db_manager.get_mysql_schema()
    else:
        return db_manager.get_mongodb_schema()

def process_query(question, db_type):
    logger.info(f"Processing query - Question: {question}, DB Type: {db_type}")
    try:
        # Get schema first since we need it for both determination and query
        logger.info("Fetching database schema")
        schema = get_schema(db_type)
        if not schema:
            logger.error("Failed to get database schema")
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
                data_str = (db_manager.execute_mysql_query(sql_response) 
                      if db_type.lower() == "mysql" 
                      else db_manager.execute_mongodb_query(sql_response))
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
                    logger.error(f"Max retries reached. Last error: {last_error}")
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
                
                logger.error(f"Query error (attempt {retry_count}): {last_error}")
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
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
        yield "处理出错", "", [(None, f"Error: {str(e)}")]

# Create Gradio interface
with gr.Blocks() as app:
    gr.Markdown("# 自然语言查询助手")
    
    with gr.Row():
        with gr.Column():
            question = gr.Textbox(
                label="请输入您的问题",
                placeholder="例如：显示各产品类别的总销售额"
            )
            
            db_type = gr.Radio(
                choices=["MySQL", "MongoDB"],
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
        app.launch(server_name="0.0.0.0", server_port=7860, share=True)
    except Exception as e:
        logger.error(f"Application error: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down NLP2SQL application")
