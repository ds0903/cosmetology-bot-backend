"""
Тестові ендпоінти для роботи з Multi-AI сервісом
Використовуйте цей файл для тестування всіх AI моделей
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import logging

from app.services.multi_ai_service import get_multi_ai_service, AIProvider

logger = logging.getLogger(__name__)

# Створюємо роутер
router = APIRouter(prefix="/api/ai", tags=["AI Testing"])


class TestMessageRequest(BaseModel):
    """Запит для тестування AI моделі"""
    provider: AIProvider
    system_prompt: str = "You are a helpful assistant."
    user_message: str
    max_tokens: int = 500
    temperature: float = 0.7


class CompareRequest(BaseModel):
    """Запит для порівняння всіх моделей"""
    system_prompt: str = "You are a helpful assistant."
    user_message: str
    max_tokens: int = 500


@router.post("/test")
async def test_ai_model(request: TestMessageRequest):
    """
    Тестування конкретної AI моделі
    
    **Приклад запиту:**
    ```json
    {
        "provider": "claude",
        "system_prompt": "You are a helpful assistant.",
        "user_message": "Hello, how are you?",
        "max_tokens": 500,
        "temperature": 0.7
    }
    ```
    
    **Доступні провайдери:**
    - claude
    - gpt-o3
    - gemini
    - grok
    """
    try:
        service = get_multi_ai_service()
        
        result = await service.send_message(
            provider=request.provider,
            system_prompt=request.system_prompt,
            user_message=request.user_message,
            max_tokens=request.max_tokens,
            temperature=request.temperature
        )
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        logger.error(f"Error testing AI model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compare")
async def compare_all_models(request: CompareRequest):
    """
    Порівняти відповіді всіх доступних AI моделей
    
    **Приклад запиту:**
    ```json
    {
        "system_prompt": "You are a helpful assistant.",
        "user_message": "Explain quantum computing in one sentence.",
        "max_tokens": 100
    }
    ```
    
    Поверне відповіді від усіх ініціалізованих моделей з інформацією про:
    - Текст відповіді
    - Використані токени
    - Приблизну вартість
    """
    try:
        service = get_multi_ai_service()
        
        results = await service.compare_all(
            system_prompt=request.system_prompt,
            user_message=request.user_message,
            max_tokens=request.max_tokens
        )
        
        return {
            "success": True,
            "data": results
        }
    except Exception as e:
        logger.error(f"Error comparing AI models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def check_ai_status():
    """
    Перевірити статус всіх AI провайдерів
    
    Показує які моделі ініціалізовані та готові до використання
    """
    service = get_multi_ai_service()
    
    status = {
        "claude": service.claude_client is not None,
        "gpt-o3": service.openai_client is not None,
        "gemini": service.gemini_client is not None,
        "grok": service.grok_client is not None
    }
    
    available = [k for k, v in status.items() if v]
    
    return {
        "success": True,
        "status": status,
        "available_providers": available,
        "total_available": len(available)
    }


@router.post("/quick-test")
async def quick_test():
    """
    Швидкий тест всіх доступних моделей з простим запитанням
    
    Використовується для перевірки що всі API ключі працюють
    """
    try:
        service = get_multi_ai_service()
        
        # Простий тестовий запит
        system_prompt = "You are a helpful assistant. Answer in one short sentence."
        user_message = "What is 2+2?"
        
        results = await service.compare_all(
            system_prompt=system_prompt,
            user_message=user_message,
            max_tokens=50
        )
        
        # Форматуємо результат для легкого читання
        summary = {}
        for provider, result in results.items():
            if provider == "summary":
                continue
            
            if "error" in result:
                summary[provider] = {
                    "status": "❌ Failed",
                    "error": result["error"]
                }
            else:
                summary[provider] = {
                    "status": "✅ Working",
                    "response": result.get("response", "")[:100],
                    "cost": f"${result.get('cost_estimate', 0):.6f}"
                }
        
        summary["total_cost"] = f"${results.get('summary', {}).get('total_cost_estimate', 0):.6f}"
        
        return {
            "success": True,
            "test_question": user_message,
            "results": summary
        }
    except Exception as e:
        logger.error(f"Error in quick test: {e}")
        raise HTTPException(status_code=500, detail=str(e))
