import asyncio
import json

from typing import Any, List
from pydantic import BaseModel, Field
from pydantic import ValidationError
from datetime import datetime

from langchain_core.tools import tool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.memory import InMemorySaver

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from langchain_mcp_adapters.client import MultiServerMCPClient

from src.model import AppConfig
from src.gitwork import GitWorker
from src.prompts import main_prompt
from src.prompts import dag_prompt, task_prompt, ddl_prompt, doc_prompt

from langfuse import Langfuse, get_client
from langfuse.langchain import CallbackHandler

lf  = Langfuse(
    public_key="pk-lf-2acd524f-a2f1-48cb-b163-8f5f2b36bc5b",
    secret_key="sk-lf-f5a1610e-5034-480b-b398-7ec491247572",
    host="https://langfuse.it-brew-lct2025.ru"
)

_config: dict = None

def get_config():
    global _config
    if _config:
        return _config
    
    try:
        # This will automatically read from environment variables
        # or .env file
        config = AppConfig()
        return config
    
    except ValidationError as e:
        print("Configuration error:")
        print(e.errors())
        # Exit or handle error appropriately
        raise


class LLMAgent():

    def __init__(self, 
                 api_key: str, 
                 base_url: str, 
                 folder: str, 
                 model: str, 
                 tools: list, 
                 checkpointer, 
                 langfuse, 
                 langfuse_handler
        ): 
        self.api_key = api_key
        self.base_url = base_url
        self.folder = folder
        self.model = model
        self.tools = tools
        self.checkpointer = checkpointer
        self.langfuse = langfuse
        self.langfuse_handler = langfuse_handler
        
        self.llm = ChatOpenAI(api_key=self.api_key, base_url=self.base_url, model=self.model, temperature=0.1)
        self.agent = create_react_agent(self.llm, tools=self.tools, prompt=main_prompt, checkpointer=self.checkpointer)

    @classmethod
    async def create(cls, api_key: str, base_url: str, folder: str, model: str, mcp_configs: dict, pg_url: str): 
        
        client = MultiServerMCPClient(mcp_configs)
        all_tools = await client.get_tools() 
        tools = (
            [tool for tool in all_tools if tool.name.startswith('gitlab_')] 
            + 
            [tool for tool in all_tools if tool.name in ['get_db_table_ddl', 'get_db_table_sample']] 
            +
            [tool for tool in all_tools if tool.name in ['test_kafka_connection', 'get_s3_bucket_parquet_schema', 'get_s3_bucket_object_sample', 'get_s3_bucket_object_list', 'get_link_sample']] 
            +
            [
                generate_task_file, generate_dag_file, 
                generate_ddl_file, generate_doc_file, 
                commit_file, create_branch
            ]
        )

        lf_client = get_client()
        langfuse_handler = CallbackHandler()

        if pg_url:
            aconn = await AsyncConnection.connect(pg_url, autocommit=True, row_factory=dict_row)
            checkpointer = AsyncPostgresSaver(aconn)
            await checkpointer.setup()
        else:
            checkpointer = InMemorySaver()

        agent = cls(
            api_key=api_key, 
            base_url=base_url, 
            folder=folder, 
            model=model, 
            tools=tools, 
            checkpointer=checkpointer, 
            langfuse=lf_client, 
            langfuse_handler=langfuse_handler)
        
        return agent
        
    
    async def ainvoke(self, message, idx: int):
        """
        """
        config = {
            "configurable": {
                "thread_id": str(idx)
            }, 
            "recursion_limit": 50, 
            "callbacks": [self.langfuse_handler],
        }

        message_input = {"messages": [
            {"role": "user", "content": message}
            ]}

        chunks = []
        async for chunk in self.agent.astream(message_input, config=config, durability="async"):
            print(chunk)
            chunks.append(chunk)

        return chunks


# Agent dependencies    
_agent = None    

async def build_agent():
    global _agent
    conf = get_config()
    _agent = await LLMAgent.create(
        api_key=conf.API_KEY, 
        base_url=conf.BASE_URL, 
        folder=conf.FOLDER, 
        model=conf.MODEL_NAME,
        mcp_configs=json.load(open(conf.MCP_CONFIG,"r")),
        pg_url=conf.POSTGRESQL_URL
        )


def get_agent():
    return _agent

# Model dependencies
_model = None

async def build_model():
    global _model

    conf = get_config()
    api_key=conf.API_KEY
    base_url=conf.BASE_URL
    model=conf.MODEL_NAME

    _model = ChatOpenAI(api_key=api_key, base_url=base_url, model=model)

async def get_model():
    return _model

# Git dependencies
_git = None

async def build_git():
    global _git
    conf = get_config()
    _git = GitWorker(gitlab_url=conf.GITLAB_URL, gitlab_token=conf.GITLAB_TOKEN, project_path=conf.PROJECT_PATH)

def get_git():
    return _git



# Tools
class FileOutput(BaseModel):
    filename:str = Field(..., description="File name with path")
    description:str = Field(..., description="File description for git commit message")
    content:str = Field(..., description="File content")
    commit_message: str = Field(..., description="Git commit message")

# Global parser instance
parser = PydanticOutputParser(pydantic_object=FileOutput)

@tool
async def generate_task_file(
    file_name:str = Field(..., description="Apache Spark application file name"),
    task_requirements:str = Field(..., description="Task requirements to generate file"),
    task_template:str = Field(..., description="Proper pyspark template and usefull snippets")
    ) -> FileOutput:
    """
    This tool is for generating Apache Spark applications for ETL processes
    """
    prompt = PromptTemplate(
        template=task_prompt,
        input_variables=['file_name', 'task_requirements', 'task_template'],
        partial_variables={"format_instructions": parser.get_format_instructions()},
        )
    
    model = await get_model()
    
    chain = prompt | model | parser

    return await chain.ainvoke({"file_name": file_name, "task_requirements": task_requirements, "task_template": task_template})


@tool
async def generate_dag_file(
    dag_id:str = Field(..., description="Airflow DAG id and DAG file name."),
    dag_requirements:str = Field("Airflow DAG requirements specification"), 
    dag_template:str = Field("Airflow DAG template to provide same code generation"),
    generation_instructions:str = Field("Generate instructions")                     
    ) -> FileOutput:
    """
    This tool is for generating Airflow DAG file based on requirements and generation instructions.
    """

    prompt = PromptTemplate(
        template=dag_prompt,
        input_variables=['dag_id', 'dag_requirements', 'dag_template', 'generation_instructions'],
        partial_variables={"format_instructions": parser.get_format_instructions()},
        )
    
    model = await get_model()
    
    chain = prompt | model | parser

    return await chain.ainvoke({"dag_id": dag_id, "dag_requirements": dag_requirements, "dag_template": dag_template, "generation_instructions": generation_instructions})


@tool
async def generate_ddl_file(
    file_name:str = Field(..., description="DDL file name."),
    source_ddl:str = Field("Source ddl specification"), 
    ddl_template:str = Field("Apache spark ddl template"),
    additional_instructions:str = Field("Generate instructions")                     
    ) -> FileOutput:
    """
    This tool is for generating Apache Spark DDL file based on source ddl, ddl template and additional instructions.
    """
    parser = PydanticOutputParser(pydantic_object=FileOutput)

    prompt = PromptTemplate(
        template=ddl_prompt,
        input_variables=['file_name', 'source_ddl', 'ddl_template', 'additional_instructions'],
        partial_variables={"format_instructions": parser.get_format_instructions()},
        )
    
    model = await get_model()
    
    chain = prompt | model | parser

    return await chain.ainvoke({"filename": file_name, "source_ddl": source_ddl, "ddl_template": ddl_template, "additional_instructions": additional_instructions})


@tool
async def generate_doc_file(
    file_name:str = Field(..., description="Documentation file name."),
    file_context:str = Field(..., description="Documentatiln context"),
    additional_instructions:str = Field(..., description="Generate instructions"),
    dag_file:str = Field(..., description="Generated DAG file")                 
    ) -> FileOutput:
    """
    This tool is for generating documentation file based on provided context, Airflow DAG file and additional instructions.
    Use this tool to generate SRS (Software requirements Spce.), README or any another text documents.
    """
    parser = PydanticOutputParser(pydantic_object=FileOutput)

    prompt = PromptTemplate(
        template=doc_prompt,
        input_variables=['file_name', 'file_context', 'additional_instructions', 'dag_file'],
        partial_variables={"format_instructions": parser.get_format_instructions()},
        )
    
    model = await get_model()
    
    chain = prompt | model | parser

    return await chain.ainvoke({"file_name": file_name, "file_context": file_context, "additional_instructions": additional_instructions, "dag_file": dag_file})


@tool
async def create_branch(
    issue_id:int = Field(..., description="Gitlab issue id"),
    issue_title:str = Field(..., description="Gitlab issue title")) -> str:
    """
    This tool is for creating a new branch in the local github project
    """
    git = get_git()

    branch_name = git.gitlab_create_branch(issue_id=issue_id, task_title=issue_title)

    return branch_name


@tool
async def commit_file(
    branch_name:str = Field(..., description="Gitlab branch name"),
    filename:str = Field(..., description="File name with path"),
    filecontent:str = Field(..., description="File content"),
    task_title:str = Field(..., description="Task title for commit message")
    ) -> dict[str, Any]:
    """
    This tool is for executing `git commit` action in provided branch
    """
    git = get_git()

    res = git.gitlab_commit_file(branch_name=branch_name, filename=filename, filecontent=filecontent, task=task_title)

    return res

