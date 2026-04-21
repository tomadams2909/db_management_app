from enum import Enum
from config.settings import get_env

class Database(str, Enum):
    LOCAL = "LOCAL_DB_URL"
    INSURANCE = "INSURANCE_DB_URL"

    @property
    def url(self) -> str:
        return get_env(self.value)