"""
Динамічне перемикання AI моделей через API
Дозволяє змінювати модель БЕЗ перезапуску сервера
"""

from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Глобальна змінна для поточного провайдера
_current_provider = "claude"  # За замовчуванням
_provider_history = []

def get_current_provider() -> str:
    """Отримати поточну модель"""
    return _current_provider

def set_current_provider(provider: str) -> dict:
    """
    Змінити поточну модель
    
    Args:
        provider: "claude", "o3", "gemini", "grok"
    
    Returns:
        dict з результатом
    """
    global _current_provider, _provider_history
    
    valid_providers = ["claude", "o3", "gemini", "grok"]
    
    if provider not in valid_providers:
        return {
            "success": False,
            "error": f"Invalid provider. Valid: {valid_providers}"
        }
    
    old_provider = _current_provider
    _current_provider = provider
    
    # Зберігаємо історію
    _provider_history.append({
        "from": old_provider,
        "to": provider,
        "timestamp": datetime.utcnow().isoformat()
    })
    
    logger.info(f"🔄 AI Provider changed: {old_provider} → {provider}")
    
    return {
        "success": True,
        "old_provider": old_provider,
        "new_provider": provider,
        "message": f"Successfully switched from {old_provider} to {provider}"
    }

def get_provider_history() -> list:
    """Отримати історію змін моделей"""
    return _provider_history

def reset_provider_history():
    """Очистити історію"""
    global _provider_history
    _provider_history = []
