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
    station : str

    class Config:
        env_file= ".locationdetails.env"
        env_file_encoding = "utf-8"

class scrape(BaseSettings):
    station_observation_url : str
    daily_observation_directory : str
    station_name : str

    class Config:
        env_file='.scrape.env'
        env_file_encoding = 'utf-8'