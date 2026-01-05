from sqlalchemy import create_engine

from .config import Settings

_config = Settings.get_config()

class Engine():

    def get_engine():
        try:
            engine = create_engine(_config.db_datasource)
            return engine
        except Exception as e:
            raise ValueError(f"Invalid DB datasource: {_config.db_datasource} | ERROR: {e}")