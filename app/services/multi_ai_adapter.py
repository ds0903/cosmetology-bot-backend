"""
Адаптер ClaudeService для роботи з MultiAIService
Дозволяє використовувати будь-яку AI модель замість Claude
"""

import json
import logging
from typing import Dict, Any, Optional, Literal
from datetime import datetime

from .multi_ai_service import get_multi_ai_service, AIProvider
from ..config import settings, ProjectConfig
from ..utils.prompt_loader import get_prompt
from ..models import (
    IntentDetectionResult, 
    ServiceIdentificationResult, 
    ClaudeMainResponse
)

logger = logging.getLogger(__name__)


class MultiAIAdapter:
    """
    Адаптер який замінює ClaudeService і дозволяє використовувати різні AI моделі
    
    Приклад використання:
        # Замість ClaudeService
        ai_service = MultiAIAdapter(db, provider="claude")
        
        # Або динамічно
        ai_service = MultiAIAdapter(db)
        ai_service.set_provider("gpt-o3")
    """
    
    def __init__(self, db, provider: AIProvider = None):
        """
        Ініціалізація адаптера
        
        Args:
            db: Database session
            provider: AI модель ("claude", "gpt-o3", "gemini", "grok")
                     Якщо None - береться з settings.default_ai_provider
        """
        self.db = db
        self.multi_ai = get_multi_ai_service()
        
        # Визначаємо провайдера
        if provider:
            self.current_provider = provider
        elif hasattr(settings, 'default_ai_provider'):
            self.current_provider = settings.default_ai_provider
        else:
            self.current_provider = "claude"  # Default fallback
        
        logger.info(f"MultiAIAdapter initialized with provider: {self.current_provider}")
        
        # Лічильник запитів для статистики
        self.request_count = {
            "claude": 0,
            "gpt-o3": 0,
            "gemini": 0,
            "grok": 0
        }
        self.total_cost = 0.0
    
    def set_provider(self, provider: AIProvider):
        """Змінити AI модель"""
        old_provider = self.current_provider
        self.current_provider = provider
        logger.info(f"Provider changed: {old_provider} -> {provider}")
    
    def get_provider(self) -> str:
        """Отримати поточну AI модель"""
        return self.current_provider
    
    def get_stats(self) -> Dict[str, Any]:
        """Отримати статистику використання"""
        return {
            "current_provider": self.current_provider,
            "requests_by_provider": self.request_count,
            "total_requests": sum(self.request_count.values()),
            "total_cost_estimate": round(self.total_cost, 6)
        }
    
    async def _call_ai(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        max_tokens: int = 2000,
        provider: Optional[AIProvider] = None
    ) -> Dict[str, Any]:
        """Внутрішній метод виклику AI"""
        # Використовуємо вказаний провайдер або поточний
        provider_to_use = provider or self.current_provider
        
        # Виклик AI
        result = await self.multi_ai.send_message(
            provider=provider_to_use,
            system_prompt=system_prompt,
            user_message=user_prompt,
            max_tokens=max_tokens
        )
        
        # Оновлюємо статистику
        self.request_count[provider_to_use] += 1
        self.total_cost += result.get("cost_estimate", 0)
        
        return result
    
    async def detect_intent(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        current_date: str,
        day_of_week: str,
        message_id: str,
        zip_history: Optional[str] = None
    ) -> IntentDetectionResult:
        """
        Module 1: Intent detection
        Визначає намір користувача
        """
        logger.info(f"Message ID: {message_id} - Intent detection using {self.current_provider}")
        
        # Отримуємо промпт
        system_prompt = get_prompt("intent_detection")
        
        # Формуємо user prompt
        user_prompt_parts = [
            f"current_date: {current_date}",
            f"day_of_week: {day_of_week}",
            f"dialogue_history: {dialogue_history}"
        ]
        
        if zip_history:
            user_prompt_parts.append(f"zip_history: {zip_history}")
        
        user_prompt_parts.append(f"current_message: {current_message}")
        user_prompt = "\n".join(user_prompt_parts)
        
        try:
            # Виклик AI
            result = await self._call_ai(system_prompt, user_prompt, max_tokens=1000)
            
            # Парсинг відповіді
            raw_response = result["response"]
            logger.info(f"Message ID: {message_id} - Raw response: {raw_response[:200]}")
            
            # Парсимо JSON відповідь
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                intent_result = IntentDetectionResult(**parsed)
                logger.info(f"Message ID: {message_id} - Intent: waiting={intent_result.waiting}")
                return intent_result
            else:
                logger.warning(f"Message ID: {message_id} - Failed to parse, using default")
                return IntentDetectionResult(waiting=1)
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in intent detection: {e}")
            return IntentDetectionResult(waiting=1)
    
    async def identify_service(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        message_id: str
    ) -> ServiceIdentificationResult:
        """
        Module 2: Service identification
        Визначає послугу яку хоче клієнт
        """
        logger.info(f"Message ID: {message_id} - Service identification using {self.current_provider}")
        
        # Отримуємо промпт
        base_prompt = get_prompt("service_identification")
        services_json = json.dumps(project_config.services, ensure_ascii=False, indent=2)
        system_prompt = f"{base_prompt}\n\nДоступні послуги:\n{services_json}"
        
        user_prompt = f"dialogue_history: {dialogue_history}\ncurrent_message: {current_message}"
        
        try:
            result = await self._call_ai(system_prompt, user_prompt, max_tokens=500)
            raw_response = result["response"]
            
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                service_result = ServiceIdentificationResult(**parsed)
                logger.info(f"Message ID: {message_id} - Service: {service_result.service_name}, duration: {service_result.time_fraction}")
                return service_result
            else:
                return ServiceIdentificationResult(time_fraction=1, service_name="unknown")
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in service identification: {e}")
            return ServiceIdentificationResult(time_fraction=1, service_name="unknown")
    
    async def generate_main_response(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        current_date: str,
        day_of_week: str,
        available_slots: Dict[str, Any],
        reserved_slots: Dict[str, Any],
        rows_of_owner: str,
        message_id: str,
        slots_target_date: Optional[str] = None,
        zip_history: Optional[str] = None,
        record_error: Optional[str] = None,
        newbie_status: int = 1
    ) -> ClaudeMainResponse:
        """
        Module 3: Main response generation
        Генерує головну відповідь клієнту
        """
        logger.info(f"Message ID: {message_id} - Main response using {self.current_provider}")
        
        # Отримуємо промпт
        base_prompt = get_prompt("main_response")
        specialists = ', '.join(project_config.specialists)
        system_prompt = f"{base_prompt}\n\nСпеціалісти: {specialists}"
        
        # Формуємо user prompt
        user_prompt_parts = [
            f"current_date: {current_date}",
            f"day_of_week: {day_of_week}",
            f"newbie_massage: {newbie_status}",
            f"dialogue_history: {dialogue_history}",
            f"current_message: {current_message}",
            f"available_slots: {json.dumps(available_slots, ensure_ascii=False)}",
            f"reserved_slots: {json.dumps(reserved_slots, ensure_ascii=False)}",
            f"rows_of_owner: {rows_of_owner}"
        ]
        
        if zip_history:
            user_prompt_parts.append(f"zip_history: {zip_history}")
        if record_error:
            user_prompt_parts.append(f"record_error: {record_error}")
        if slots_target_date:
            user_prompt_parts.append(f"slots_target_date: {slots_target_date}")
        
        user_prompt = "\n".join(user_prompt_parts)
        
        try:
            result = await self._call_ai(system_prompt, user_prompt, max_tokens=2000)
            raw_response = result["response"]
            
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                # CRITICAL FIX: Обробляємо різні назви полів від різних моделей
                # Деякі моделі повертають client_response замість gpt_response (з прикладів у промпті)
                if 'gpt_response' not in parsed and 'client_response' in parsed:
                    logger.info(f"Message ID: {message_id} - Converting client_response to gpt_response")
                    parsed['gpt_response'] = parsed['client_response']
                
                # Якщо немає ні gpt_response, ні client_response - fallback
                if 'gpt_response' not in parsed:
                    logger.error(f"Message ID: {message_id} - No gpt_response or client_response in parsed result: {list(parsed.keys())}")
                    parsed['gpt_response'] = "Вибачте, сталася помилка. Спробуйте ще раз."
                
                try:
                    main_response = ClaudeMainResponse(**parsed)
                    logger.info(f"Message ID: {message_id} - Response generated, cost: ${result.get('cost_estimate', 0):.6f}")
                    return main_response
                except Exception as validation_error:
                    logger.error(f"Message ID: {message_id} - Validation error: {validation_error}")
                    logger.error(f"Message ID: {message_id} - Parsed data: {parsed}")
                    return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
            else:
                return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in main response: {e}")
            return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
    
    def _parse_json_response(self, raw_response: str, message_id: str) -> Optional[dict]:
        """Парсинг JSON відповіді від будь-якої AI моделі"""
        try:
            # Очищаємо відповідь
            content = raw_response.strip()
            
            # Видаляємо markdown блоки
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            # Шукаємо JSON в тексті
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            
            # Парсимо JSON
            result = json.loads(content)
            
            if not isinstance(result, dict):
                raise ValueError(f"Response is not a dictionary: {type(result)}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Message ID: {message_id} - JSON parsing error: {e}")
            logger.error(f"Raw response: {raw_response[:500]}")
            return None
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Parsing error: {e}")
            return None
