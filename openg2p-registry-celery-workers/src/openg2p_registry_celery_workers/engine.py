from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from .config import Settings

_config = Settings.get_config()

class Engine:
    '''
    Engine for sync database connection.
    Engine for async connection is managed by openg2p_fastapi_common.context dbengine.
    '''
    @staticmethod
    def get_engine():
        try:
            engine = create_engine(Engine.construct_db_datasource())
            return engine
        except Exception as e:
            raise ValueError(f"Invalid DB datasource: {Engine.construct_db_datasource()} | ERROR: {e}")
    
    @staticmethod
    def get_async_engine():
        try:
            engine = create_async_engine(Engine.construct_async_db_datasource())
            return engine
        except Exception as e:
            raise ValueError(f"Invalid DB datasource: {Engine.construct_async_db_datasource()} | ERROR: {e}")

    def construct_db_datasource():
        return f"postgresql://{_config.db_username}:{_config.db_password}@{_config.db_hostname}:{_config.db_port}/{_config.db_dbname}"
    
    def construct_async_db_datasource():
        return f"postgresql+asyncpg://{_config.db_username}:{_config.db_password}@{_config.db_hostname}:{_config.db_port}/{_config.db_dbname}"