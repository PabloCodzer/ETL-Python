from typing import List

from fastapi import APIRouter
from fastapi import status
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from sqlalchemy.ext.asyncio import AsyncSession # type: ignore
from sqlmodel import select # type: ignore
from core.deps import get_session
from models.Curso_Model import Curso_model
from sqlmodel.sql.expression import Select, SelectOfScalar # type: ignore

SelectOfScalar.inherit_cache =  True
Select.inherit_cache = True

router = APIRouter()

@router.get('/', response_model=List[Curso_model])
async def get_cursos(db: AsyncSession = Depends(get_session)):
    async with db as session:
        query = select(Curso_model)
        result =  await session.execute(query)
        cursos: List[Curso_model] = result.scalars().all()
        return cursos
    
@router.get('/{curso_id}', status_code=status.HTTP_202_ACCEPTED, response_model=Curso_model)
async def get_curso_by_id(curso_id: int, db: AsyncSession = Depends(get_session)):
    async with db as session:
       
        query = select(Curso_model).filter(Curso_model.id == curso_id)
        result = await session.execute(query)
        curso = result.scalar_one_or_none()
            
        if curso is not None:
            return curso
        else:
            raise HTTPException(detail="Curso não encontrado", status_code=status.HTTP_404_NOT_FOUND)
            

@router.post('/', status_code=status.HTTP_201_CREATED, response_model=Curso_model)
async def post_curso(db: AsyncSession = Depends(get_session), curso:Curso_model=None):
    novo_curso = Curso_model(
        titulo=curso.titulo,
        aulas=curso.aulas,
        horas=curso.horas
    )
    db.add(novo_curso)
    await db.commit()
    return novo_curso