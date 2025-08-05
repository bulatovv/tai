from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration settings"""

    model_config = SettingsConfigDict(env_file='.env')  # pyright: ignore[reportUnannotatedClassAttribute]

    log_level: str = 'INFO'
    log_json_format: bool = False
    log_include_stack: bool = False

    training_host: str = 'samp.training-server.com'
    training_port: int = 7777


settings = Settings()
