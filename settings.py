from pydantic import BaseSettings

class Settings(BaseSettings):
    MONGO_CONNECTION: str
    FITS_BUCKET: str
    NORMALIZATION_URL: str
    YEAR: int
    MONTH: int
    USER_ID: str