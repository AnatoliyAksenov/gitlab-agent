from uuid import uuid1
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks
from fastapi import Depends, Request, Response

from src.utils import get_model
from src.utils import get_agent
from src.utils import get_git

from src.tasks import process_issue_task

router = APIRouter()

@router.post("/process_issue")
async def read_users_me(request: Request, background_tasks: BackgroundTasks, agent=Depends(get_agent), git=Depends(get_git)):
    data = await request.json()

    background_tasks.add_task(process_issue_task, data, agent, git)

    return Response("Issue in process", 200)
