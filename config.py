# config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="C:\\projects\\1с_tj_daemon_app\\1c_tj_daemon\\.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    # 🔧 ClickHouse — плоские поля
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_database: str = "NetrebinAA"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_table_name: str = "test_tj_events"
    
    #  Parser — плоские поля
    parser_root_directory: str = "Y:\\Monitor\\Happywear_New"
    parser_poll_interval: int = 60
    parser_batch_size: int = 5000
    parser_state_db_path: str = "C:\\projects\\1с_tj_daemon_app\\1c_tj_daemon\\data\\state.db"
    parser_log_file: str = "C:\\projects\\1с_tj_daemon_app\\1c_tj_daemon\\logs\\daemon.log"
    parser_log_level: str = "INFO"
    parser_excluded_dirs: str = "ed4562a8-f93a-4d27-80cb-b5f6d8595faa,.tmp,archive,ad7049e9-fdab-46d6-837a-138bb65d8864,256c8005-0b82-4140-89dc-f1f639fa6345,Querying"
    
    @property
    def watch_directories(self) -> List[str]:
        return [
            "SQL_Locks", "1C_Culprits", "Trans", "1C_Victims",
            "47044381-d7a4-4e94-af24-fe6fc38b754a",
             "a76a488a-ceaf-4640-acf8-25fcf0fd68d3",
        ]
    
    @property
    def excluded_dirs_list(self) -> List[str]:
        return [d.strip() for d in self.parser_excluded_dirs.split(',') if d.strip()]


settings = Settings()
