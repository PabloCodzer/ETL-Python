from fastapi import APIRouter
from api.V1.endpoists import curso

api_router = APIRouter()
api_router.include_router(curso.router, prefix='/cursos', tags=['cursos'])