from fastapi import FastAPI
from core.configs import settings
from api.V1.api import api_router

app: FastAPI = FastAPI(title='Teste- fastAPI SQL Model')
app.include_router(api_router, prefix=settings.API_V1_STR)