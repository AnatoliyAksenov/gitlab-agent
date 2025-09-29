
from datetime import datetime, timedelta
from fastapi import FastAPI


from src.api.hooks   import router as HooksRouter 
from src.api.health import router as HealthRouter
from contextlib import asynccontextmanager


from src.utils import build_agent, build_git, build_model

@asynccontextmanager
async def lifespan(app: FastAPI):
    # raise the sails
    await build_agent()
    await build_model()
    await build_git()
    
    print('Application ready to work.')
    yield
    # Finish line
    print('Work finished. Thanks')
    pass

app = FastAPI(
    title="Back-end server for DE chat application",
    lifespan=lifespan
)

app.include_router(HooksRouter)
app.include_router(HealthRouter)


if __name__ == '__main__':
    """
    Only for debug and local run.
    For production run use: `uvicorn app:app --host 0.0.0.0 --port 8000`
    """

    import uvicorn

    uvicorn.run('app:app', host="0.0.0.0", port=8080)