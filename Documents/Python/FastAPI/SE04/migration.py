from sqlmodel import SQLModel # type: ignore
from core.database import engine
import asyncio

async def create_tables() -> None:
    import models.__all_models
    print("Criando as tabelas....")

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)
        
    print("Tabelas Criadas")

if __name__ == "__main__":
    asyncio.run(create_tables())