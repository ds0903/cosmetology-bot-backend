from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Dict, Any
import os
from app.utils.prompt_loader import get_prompt, get_all_prompts


class Settings(BaseSettings):
    """Main application settings loaded from environment variables"""
    
    # Database
    database_url: str = Field(default="postgresql://user:password@localhost/bot_db")
    redis_url: str = Field(default="redis://localhost:6379")
    
    # Claude AI
    claude_api_key_1: str = Field(default="")
    claude_api_key_2: str = Field(default="")
    claude_model: str = Field(default="claude-sonnet-4-20250514")
    
    # Google Sheets
    google_credentials_file: str = Field(default="credentials.json")
    google_sheets_credentials_file: str = Field(default="credentials.json")
    google_sheet_id: str = Field(default="")
    google_sheet_make_id: str = Field(default="")
    google_sheets_scopes: List[str] = Field(default=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    
    # SendPulse
    sendpulse_webhook_secret: str = Field(default="")
    sendpulse_api_url: str = Field(default="https://api.sendpulse.com/your-endpoint")
    sendpulse_api_token: str = Field(default="")
    
    # Application
    debug: bool = Field(default=True)
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    log_level: str = Field(default="INFO")
    secret_key: str = Field(default="your_secret_key_here")
    
    # Business Hours
    default_work_start_time: str = Field(default="09:00")
    default_work_end_time: str = Field(default="18:00")
    slot_duration_minutes: int = Field(default=30)
    
    # Message Queue
    max_queue_size: int = Field(default=1000)
    message_retry_attempts: int = Field(default=3)
    message_processing_timeout: int = Field(default=30)
    
    # Dialogue Archiving
    dialogue_archive_hours: int = Field(default=24)
    dialogue_archive_after_hours: int = Field(default=24)
    archive_compression_enabled: bool = Field(default=True)
    
    # Rate Limiting
    max_messages_per_minute: int = Field(default=60)
    flood_protection_threshold: int = Field(default=10)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


class ProjectConfig:
    """Configuration for each individual project/client"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.database_table_name = f"bookings_{project_id}"
        self.google_sheet_id = settings.google_sheet_id
        self.google_sheet_make_id = getattr(settings, "google_sheet_make_id", "")
        self.google_drive_folder_id = settings.google_drive_folder_id
        self.claude_prompts = get_all_prompts()
        self.services = {}  # service_name -> duration_in_slots
        self.specialists = []
        self.work_hours = {
            "start": settings.default_work_start_time,
            "end": settings.default_work_end_time
        }
    
    def update_prompt(self, prompt_type: str, new_prompt: str) -> None:
        """Update a specific Claude prompt"""
        if prompt_type in self.claude_prompts:
            self.claude_prompts[prompt_type] = new_prompt
        else:
            raise ValueError(f"Unknown prompt type: {prompt_type}. Available: {list(self.claude_prompts.keys())}")
    
    def get_prompt(self, prompt_type: str) -> str:
        """Get a specific Claude prompt"""
        return get_prompt(prompt_type)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert ProjectConfig to dictionary"""
        return {
            "project_id": self.project_id,
            "database_table_name": self.database_table_name,
            "google_sheet_id": self.google_sheet_id,
            "google_drive_folder_id": self.google_drive_folder_id,
            "claude_prompts": self.claude_prompts,
            "services": self.services,
            "specialists": self.specialists,
            "work_hours": self.work_hours
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProjectConfig":
        """Create ProjectConfig from dictionary"""
        config = cls(data["project_id"])
        config.database_table_name = data.get("database_table_name", f"bookings_{data['project_id']}")
        config.google_sheet_id = data.get("google_sheet_id", settings.google_sheet_id)
        config.google_drive_folder_id = data.get("google_drive_folder_id", "")
        config.claude_prompts = data.get("claude_prompts", get_all_prompts())
        config.services = data.get("services", {})
        config.specialists = data.get("specialists", [])
        config.work_hours = data.get("work_hours", {
            "start": settings.default_work_start_time,
            "end": settings.default_work_end_time
        })
        return config


# Initialize global settings instance
settings = Settings() 
