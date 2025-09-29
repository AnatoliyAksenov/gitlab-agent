import json
from retry import retry

from pydantic import BaseModel, Field

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser

from src.gitwork import GitWorker


async def process_issue_task(data, agent, git:GitWorker):
    print('Starting processing issue')
    issue_id = data.get('object_attributes', {}).get('id')
    description = data.get('object_attributes', {}).get('description')
    title = data.get('object_attributes', {}).get('title')

    data = f"""You have to solve a task:
    - issue_id: {issue_id}
    - issue_title: `{title}`
    - description: 
    ```
    {description}
    ```
    """
    print(f"Starting processing {issue_id}: {title}")

    res = await agent.ainvoke(json.dumps(data, ensure_ascii=False), issue_id)

    total_tokens = sum([x.get('agent').get('messages')[0].usage_metadata.get('total_tokens') for x in res if x.get('agent')])
    input_tokens = sum([x.get('agent').get('messages')[0].usage_metadata.get('input_tokens') for x in res if x.get('agent')])
    output_tokens = sum([x.get('agent').get('messages')[0].usage_metadata.get('output_tokens') for x in res if x.get('agent')])

    git.add_notes(issue_id, f"Processing finished. Total tokens: {total_tokens} were used (input tokens: {input_tokens}, output tokens: {output_tokens})")
    git.create_merge_request(issue_id=issue_id)
    git.close_issue(issue_id=issue_id)

    print(f"Issue id: {issue_id} is done.")

    





