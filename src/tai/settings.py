from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration settings"""

    model_config = SettingsConfigDict(env_file='.env')  # pyright: ignore[reportUnannotatedClassAttribute]

    log_level: str = 'INFO'
    log_json_format: bool = False
    log_include_stack: bool = False
    log_exception_include_locals: bool = False

    training_host: str = 'samp.training-server.com'
    training_port: int = 7777

    training_api_base_url: str = 'https://training-server.com/api'

    chrono_login: str
    chrono_token: str
    chrono_api_base_url: str = 'https://chrono.czo.ooo/api'

    telegram_bot_token: str
    telegram_channel_id: str


settings = Settings()
