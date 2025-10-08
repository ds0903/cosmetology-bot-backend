"""
API ендпоінти для динамічного перемикання моделей
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Literal
import logging

from app.services.provider_switcher import (
    get_current_provider,
    set_current_provider,
    get_provider_history,
    reset_provider_history
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/ai", tags=["AI Model Management"])

AIProvider = Literal["claude", "o3", "gemini", "grok"]


class SwitchProviderRequest(BaseModel):
    """Запит на зміну моделі"""
    provider: AIProvider
    reason: str = ""  # Опціональна причина зміни


@router.get("/current-model")
async def get_current_model():
    """
    Отримати поточну AI модель
    
    **Приклад:**
    ```bash
    curl http://localhost:8000/admin/ai/current-model
    ```
    """
    current = get_current_provider()
    return {
        "success": True,
        "current_provider": current,
        "available_providers": ["claude", "o3", "gemini", "grok"]
    }


@router.post("/switch-model")
async def switch_model(request: SwitchProviderRequest):
    """
    Змінити AI модель БЕЗ перезапуску сервера
    
    **Приклад:**
    ```bash
    curl -X POST http://localhost:8000/admin/ai/switch-model \
      -H "Content-Type: application/json" \
      -d '{"provider": "gemini", "reason": "testing"}'
    ```
    
    **Доступні моделі:**
    - claude
    - o3
    - gemini
    - grok
    """
    try:
        result = set_current_provider(request.provider)
        
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        
        logger.info(f"Model switched to {request.provider}. Reason: {request.reason or 'not specified'}")
        
        return {
            "success": True,
            "old_provider": result["old_provider"],
            "new_provider": result["new_provider"],
            "message": result["message"],
            "reason": request.reason
        }
    except Exception as e:
        logger.error(f"Error switching model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-history")
async def get_model_history():
    """
    Отримати історію змін моделей
    
    **Приклад:**
    ```bash
    curl http://localhost:8000/admin/ai/model-history
    ```
    """
    history = get_provider_history()
    return {
        "success": True,
        "total_switches": len(history),
        "history": history
    }


@router.post("/reset-history")
async def reset_history():
    """
    Очистити історію змін моделей
    
    **Приклад:**
    ```bash
    curl -X POST http://localhost:8000/admin/ai/reset-history
    ```
    """
    reset_provider_history()
    return {
        "success": True,
        "message": "History cleared"
    }


@router.get("/quick-switch/{provider}")
async def quick_switch(provider: AIProvider):
    """
    Швидка зміна моделі через URL
    
    **Приклади:**
    ```bash
    curl http://localhost:8000/admin/ai/quick-switch/gemini
    curl http://localhost:8000/admin/ai/quick-switch/o3
    curl http://localhost:8000/admin/ai/quick-switch/grok
    curl http://localhost:8000/admin/ai/quick-switch/claude
    ```
    """
    result = set_current_provider(provider)
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return {
        "success": True,
        "message": f"✅ Switched to {provider}",
        "old_provider": result["old_provider"],
        "new_provider": result["new_provider"]
    }
