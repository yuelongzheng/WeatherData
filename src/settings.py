from pydantic_settings import BaseSettings

class LocationDetails(BaseSettings):
    longdeg : str
    longmin : str
    latdeg : str
    latmin : str
    location : str
    longhemi : str
    lathemi : str
    timezone : str
    loc : str
    Event : str

    class Config:
        env_file= ".env"
        env_file_encoding = "utf-8"
