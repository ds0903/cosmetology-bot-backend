"""
Адаптер ClaudeService для роботи з MultiAIService
Дозволяє використовувати будь-яку AI модель замість Claude
"""

import json
import logging
import re
import base64
import httpx
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
    """
    
    def __init__(self, db, provider: AIProvider = None):
        """
        Ініціалізація адаптера
        
        Args:
            db: Database session
            provider: AI модель ("claude", "o3", "gemini", "grok")
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
            self.current_provider = "claude"
        
        logger.info(f"MultiAIAdapter initialized with provider: {self.current_provider}")
        
        # Лічильник запитів для статистики
        self.request_count = {
            "claude": 0,
            "o3": 0,
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
    
    async def _download_image_as_base64(self, image_url: str, message_id: str = None) -> Optional[dict]:
        """Download image from URL and convert to base64 for AI models"""
        try:
            logger.info(f"Message ID: {message_id} - Downloading image from {image_url}")
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(image_url, headers=headers)
                response.raise_for_status()
                
                content_type = response.headers.get('content-type', 'image/jpeg')
                image_base64 = base64.b64encode(response.content).decode('utf-8')
                
                logger.info(f"Message ID: {message_id} - Image downloaded successfully, size: {len(response.content)} bytes")
                
                return {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": content_type,
                        "data": image_base64
                    }
                }
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Failed to download image: {e}")
            return None
    
    def _enhance_prompt_for_provider(self, base_prompt: str, provider: str) -> str:
        """Адаптує промпт під конкретну модель"""
        
        # Додаємо спеціалізовані інструкції для non-Claude моделей
        if provider != "claude":
            enhancement = """
КРИТИЧНО ВАЖЛИВО ДЛЯ JSON ВІДПОВІДІ:
1. Відповідай ТІЛЬКИ чистим JSON без будь-яких додаткових текстів, markdown або пояснень
2. НЕ використовуй ```json``` обгортки - тільки чистий JSON
3. Переконайся що всі ключі точно відповідають тим, що вказані в інструкціях
4. Всі булеві значення передавай як true/false (малими літерами)
5. Всі строкові значення беруть в подвійні лапки
6. НЕ додавай ніяких коментарів після JSON
7. Перевір що JSON валідний перед відповіддю

ФОРМАТ ВІДПОВІДІ МАЄ БУТИ ТОЧНО ТАКИМ:
{"key1": "value1", "key2": true, "key3": null}

"""
            base_prompt = enhancement + base_prompt
        
        return base_prompt
    
    async def _call_ai(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        max_tokens: int = 2000,
        provider: Optional[AIProvider] = None
    ) -> Dict[str, Any]:
        """Внутрішній метод виклику AI"""
        provider_to_use = provider or self.current_provider
        
        # Адаптуємо промпт під провайдера
        enhanced_system_prompt = self._enhance_prompt_for_provider(system_prompt, provider_to_use)
        
        # Виклик AI
        result = await self.multi_ai.send_message(
            provider=provider_to_use,
            system_prompt=enhanced_system_prompt,
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
            result = await self._call_ai(system_prompt, user_prompt, max_tokens=1000)
            
            raw_response = result.get("response", result.get("text", ""))
            logger.info(f"Message ID: {message_id} - Raw intent response ({self.current_provider}): {raw_response[:200]}")
            
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                normalized = self._normalize_intent_fields(parsed, message_id)
                intent_result = IntentDetectionResult(**normalized)
                logger.info(f"Message ID: {message_id} - Intent ({self.current_provider}): waiting={intent_result.waiting}, date={intent_result.date_order}")
                return intent_result
            else:
                logger.warning(f"Message ID: {message_id} - Failed to parse intent from {self.current_provider}, using default")
                return IntentDetectionResult(waiting=1)
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in intent detection ({self.current_provider}): {e}")
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
        
        base_prompt = get_prompt("service_identification")
        services_json = json.dumps(project_config.services, ensure_ascii=False, indent=2)
        system_prompt = f"{base_prompt}\n\nДоступні послуги:\n{services_json}"
        
        user_prompt = f"dialogue_history: {dialogue_history}\ncurrent_message: {current_message}"
        
        try:
            result = await self._call_ai(system_prompt, user_prompt, max_tokens=500)
            raw_response = result.get("response", result.get("text", ""))
            
            logger.info(f"Message ID: {message_id} - Raw service response ({self.current_provider}): {raw_response[:200]}")
            
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                normalized = self._normalize_service_fields(parsed, message_id)
                service_result = ServiceIdentificationResult(**normalized)
                logger.info(f"Message ID: {message_id} - Service ({self.current_provider}): {service_result.service_name}, duration: {service_result.time_fraction}")
                return service_result
            else:
                return ServiceIdentificationResult(time_fraction=1, service_name="unknown")
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in service identification ({self.current_provider}): {e}")
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
        newbie_status: int = 1,
        image_url: Optional[str] = None  # NEW PARAMETER for image support
    ) -> ClaudeMainResponse:
        """
        Module 3: Main response generation
        Генерує головну відповідь клієнту
        """
        logger.info(f"Message ID: {message_id} - Main response using {self.current_provider}")
        
        base_prompt = get_prompt("main_response")
        specialists = ', '.join(project_config.specialists)
        system_prompt = f"{base_prompt}\n\nСпеціалісти: {specialists}"
        
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
        
        # Handle image if provided
        image_content = None
        if image_url:
            logger.info(f"Message ID: {message_id} - Processing image: {image_url}")
            image_content = await self._download_image_as_base64(image_url, message_id)
            if image_content:
                user_prompt_parts.append("⚠️ Користувач надіслав зображення. Проаналізуй його.")
                logger.info(f"Message ID: {message_id} - Image content prepared for AI")
        
        user_prompt = "\n".join(user_prompt_parts)
        
        try:
            # Use multi-modal for Claude with images
            if image_content and self.current_provider == "claude":
                # For Claude, use content blocks
                user_content = [image_content, {"type": "text", "text": user_prompt}]
                result = await self.multi_ai.send_message(
                    provider=self.current_provider,
                    system_prompt=self._enhance_prompt_for_provider(system_prompt, self.current_provider),
                    user_message=user_content,
                    max_tokens=2000
                )
                logger.info(f"Message ID: {message_id} - Claude Vision API called with image")
            else:
                # For other models or no image
                if image_content:
                    logger.warning(f"Message ID: {message_id} - Image provided but {self.current_provider} may not support Vision API")
                    user_prompt += "\n\n[Користувач надіслав зображення, але поточна модель може не підтримувати Vision API]"
                
                result = await self._call_ai(system_prompt, user_prompt, max_tokens=2000)
            raw_response = result.get("response", result.get("text", ""))
            
            logger.info(f"Message ID: {message_id} - Raw main response ({self.current_provider}): {raw_response[:200]}")
            
            parsed = self._parse_json_response(raw_response, message_id)
            
            if parsed:
                normalized = self._normalize_main_response_fields(parsed, message_id)
                
                try:
                    main_response = ClaudeMainResponse(**normalized)
                    logger.info(f"Message ID: {message_id} - Response generated by {self.current_provider}, cost: ${result.get('cost_estimate', 0):.6f}")
                    logger.info(f"Message ID: {message_id} - Response text length: {len(normalized.get('gpt_response', ''))} chars")
                    logger.info(f"Message ID: {message_id} - Booking actions: activate={normalized.get('activate_booking')}, reject={normalized.get('reject_order')}, change={normalized.get('change_order')}")
                    return main_response
                except Exception as validation_error:
                    logger.error(f"Message ID: {message_id} - Validation error ({self.current_provider}): {validation_error}")
                    logger.error(f"Message ID: {message_id} - Normalized data: {normalized}")
                    return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
            else:
                logger.warning(f"Message ID: {message_id} - Failed to parse main response from {self.current_provider}")
                return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in main response ({self.current_provider}): {e}", exc_info=True)
            return ClaudeMainResponse(gpt_response="Вибачте, сталася помилка. Спробуйте ще раз.")
    
    def _parse_json_response(self, raw_response: str, message_id: str) -> Optional[dict]:
        """Парсинг JSON відповіді від будь-якої AI моделі з покращеною обробкою"""
        try:
            content = raw_response.strip()
            
            # Видаляємо різні префікси
            prefixes = ["json", "JSON", "```json", "```JSON"]
            for prefix in prefixes:
                if content.startswith(prefix):
                    content = content[len(prefix):].strip()
            
            # Видаляємо markdown блоки
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            # Шукаємо JSON в тексті методом пошуку відкриваючої і закриваючої дужок
            if not content.startswith("{"):
                start = content.find("{")
                if start != -1:
                    # Шукаємо відповідну закриваючу дужку
                    brace_count = 0
                    end = -1
                    in_string = False
                    escape_next = False
                    
                    for i in range(start, len(content)):
                        char = content[i]
                        
                        if escape_next:
                            escape_next = False
                            continue
                        
                        if char == '\\':
                            escape_next = True
                            continue
                        
                        if char == '"' and not escape_next:
                            in_string = not in_string
                        
                        if not in_string:
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    end = i + 1
                                    break
                    
                    if end > start:
                        content = content[start:end]
            
            # Видаляємо control characters
            content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', content)
            
            # Парсимо JSON
            result = json.loads(content)
            
            if not isinstance(result, dict):
                raise ValueError(f"Response is not a dictionary: {type(result)}")
            
            # Логуємо thinking якщо є
            if 'thinking' in result:
                logger.debug(f"Message ID: {message_id} - AI thinking ({self.current_provider}): {result['thinking'][:200]}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Message ID: {message_id} - JSON parsing error ({self.current_provider}): {e}")
            logger.error(f"Raw response: {raw_response[:500]}")
            return None
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Parsing error ({self.current_provider}): {e}")
            return None
    
    def _normalize_intent_fields(self, parsed: dict, message_id: str) -> dict:
        """Нормалізація полів для IntentDetectionResult з підтримкою різних форматів"""
        
        # Обробка waiting
        waiting = parsed.get('waiting', 0)
        if isinstance(waiting, str):
            waiting = 1 if waiting.lower() in ('1', 'true', 'yes', 'так') else 0
        elif isinstance(waiting, bool):
            waiting = 1 if waiting else 0
        else:
            waiting = int(waiting) if isinstance(waiting, (int, float)) else 0
        
        # Обробка date_order - різні варіанти назв
        date_order = (parsed.get('date_order') or parsed.get('date') or 
                     parsed.get('desired_date') or parsed.get('appointment_date'))
        
        # Обробка desire_time з різними варіантами назв
        desire_time0 = (parsed.get('desire_time_0') or parsed.get('desire_time0') or 
                       parsed.get('time_start') or parsed.get('desired_time_start') or
                       parsed.get('time_from'))
        desire_time1 = (parsed.get('desire_time_1') or parsed.get('desire_time1') or 
                       parsed.get('time_end') or parsed.get('desired_time_end') or
                       parsed.get('time_to'))
        
        # Якщо є тільки одне поле desire_time
        if not desire_time0 and parsed.get('desire_time'):
            desire_time0 = parsed.get('desire_time')
        
        # Обробка інших полів
        name = parsed.get('name') or parsed.get('client_name') or parsed.get('customer_name')
        procedure = (parsed.get('procedure') or parsed.get('service') or 
                    parsed.get('service_name') or parsed.get('treatment'))
        cosmetolog = (parsed.get('cosmetolog') or parsed.get('specialist') or 
                     parsed.get('master') or parsed.get('doctor'))
        desire_date = parsed.get('desire_date') or parsed.get('preferred_date')
        
        normalized = {
            'waiting': waiting,
            'date_order': date_order,
            'desire_time0': desire_time0,
            'desire_time1': desire_time1,
            'name': name,
            'procedure': procedure,
            'cosmetolog': cosmetolog,
            'desire_date': desire_date
        }
        
        logger.debug(f"Message ID: {message_id} - Normalized intent: waiting={waiting}, date={date_order}, time={desire_time0}-{desire_time1}")
        return normalized
    
    def _normalize_service_fields(self, parsed: dict, message_id: str) -> dict:
        """Нормалізація полів для ServiceIdentificationResult"""
        
        # Обробка time_fraction - різні варіанти назв
        time_fraction = (parsed.get('time_fractions') or parsed.get('time_fraction') or 
                        parsed.get('duration') or parsed.get('slots') or 
                        parsed.get('duration_slots') or 1)
        
        # Якщо це словник (старий формат) - беремо перше значення
        if isinstance(time_fraction, dict):
            time_fraction = list(time_fraction.values())[0] if time_fraction else 1
        
        # Конвертуємо в число
        if isinstance(time_fraction, str):
            time_fraction = int(time_fraction) if time_fraction.isdigit() else 1
        elif not isinstance(time_fraction, int):
            time_fraction = int(time_fraction) if time_fraction else 1
        
        # Обробка service_name - різні варіанти назв
        service_name = (parsed.get('service_name') or parsed.get('service') or 
                       parsed.get('procedure') or parsed.get('treatment') or "unknown")
        
        normalized = {
            'time_fraction': time_fraction,
            'service_name': service_name
        }
        
        logger.debug(f"Message ID: {message_id} - Normalized service: {service_name}, duration: {time_fraction} slots")
        return normalized
    
    def _normalize_main_response_fields(self, parsed: dict, message_id: str) -> dict:
        """Нормалізація полів для ClaudeMainResponse з підтримкою всіх можливих варіантів"""
        
        # КРИТИЧНО: Детальне логування того що прийшло
        logger.info(f"Message ID: {message_id} - RAW parsed dict keys: {list(parsed.keys())}")
        logger.info(f"Message ID: {message_id} - RAW parsed dict (full): {json.dumps(parsed, ensure_ascii=False, indent=2)}")
        
        # КРИТИЧНО: Обробка gpt_response з максимальною кількістю варіантів
        gpt_response = (parsed.get('gpt_response') or parsed.get('client_response') or 
                       parsed.get('response') or parsed.get('message') or 
                       parsed.get('answer') or parsed.get('reply') or
                       parsed.get('text') or parsed.get('bot_response') or
                       parsed.get('assistant_response'))
        
        logger.info(f"Message ID: {message_id} - Extracted gpt_response: {gpt_response}")
        
        # Якщо нічого не знайдено, створюємо дефолтну відповідь
        if not gpt_response:
            logger.warning(f"Message ID: {message_id} - No response text found in any standard fields. Keys: {list(parsed.keys())}")
            # Шукаємо будь-яке текстове поле
            for key, value in parsed.items():
                if isinstance(value, str) and len(value) > 20 and key != 'thinking':
                    gpt_response = value
                    logger.info(f"Message ID: {message_id} - Using field '{key}' as response text")
                    break
            
            if not gpt_response:
                gpt_response = "Вибачте, сталася помилка. Спробуйте ще раз."
        
        # Функція конвертації в boolean
        def to_bool(value):
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'так', 'да')
            if isinstance(value, int):
                return value == 1
            return bool(value)
        
        # Обробка команд бронювання - максимальна кількість варіантів назв
        activate_booking = to_bool(parsed.get('activate_booking') or parsed.get('create_booking') or 
                                   parsed.get('make_booking') or parsed.get('book') or
                                   parsed.get('confirm_booking'))
        
        reject_order = to_bool(parsed.get('reject_order') or parsed.get('cancel_booking') or 
                              parsed.get('cancel_order') or parsed.get('delete_booking'))
        
        change_order = to_bool(parsed.get('change_order') or parsed.get('modify_booking') or 
                              parsed.get('update_booking') or parsed.get('reschedule'))
        
        booking_confirmed = to_bool(parsed.get('booking_confirmed') or parsed.get('confirmed'))
        booking_declined = to_bool(parsed.get('booking_declined') or parsed.get('declined'))
        
        # Обробка double_booking
        double_booking = to_bool(parsed.get('double_booking') or parsed.get('multiple_booking') or
                                parsed.get('two_bookings'))
        
        # Обробка списків для double booking
        specialists_list = (parsed.get('specialists_list') or parsed.get('specialists') or
                           parsed.get('masters_list') or parsed.get('doctors_list'))
        times_set_up_list = (parsed.get('times_set_up_list') or parsed.get('times') or
                            parsed.get('booking_times') or parsed.get('appointment_times'))
        times_reject_list = (parsed.get('times_reject_list') or parsed.get('cancel_times') or
                            parsed.get('delete_times'))
        procedures_list = (parsed.get('procedures_list') or parsed.get('procedures') or 
                          parsed.get('services') or parsed.get('services_list'))
        
        # Обробка основних полів з альтернативними назвами
        date_order = (parsed.get('date_order') or parsed.get('booking_date') or 
                     parsed.get('date') or parsed.get('appointment_date'))
        
        time_set_up = (parsed.get('time_set_up') or parsed.get('booking_time') or 
                      parsed.get('time') or parsed.get('appointment_time'))
        
        date_reject = (parsed.get('date_reject') or parsed.get('cancel_date') or
                      parsed.get('cancellation_date'))
        
        time_reject = (parsed.get('time_reject') or parsed.get('cancel_time') or
                      parsed.get('cancellation_time'))
        
        cosmetolog = (parsed.get('cosmetolog') or parsed.get('specialist') or 
                     parsed.get('master') or parsed.get('doctor') or
                     parsed.get('specialist_name'))
        
        procedure = (parsed.get('procedure') or parsed.get('service') or 
                    parsed.get('service_name') or parsed.get('treatment'))
        
        phone = parsed.get('phone') or parsed.get('telephone') or parsed.get('phone_number')
        name = parsed.get('name') or parsed.get('client_name') or parsed.get('customer_name')
        
        normalized = {
            'gpt_response': gpt_response,
            'pic': parsed.get('pic'),
            'activate_booking': activate_booking,
            'reject_order': reject_order,
            'change_order': change_order,
            'booking_confirmed': booking_confirmed,
            'booking_declined': booking_declined,
            'cosmetolog': cosmetolog,
            'time_set_up': time_set_up,
            'date_order': date_order,
            'time_reject': time_reject,
            'date_reject': date_reject,
            'procedure': procedure,
            'phone': phone,
            'name': name,
            'feedback': parsed.get('feedback'),
            'double_booking': double_booking,
            'specialists_list': specialists_list,
            'times_set_up_list': times_set_up_list,
            'times_reject_list': times_reject_list,
            'procedures_list': procedures_list
        }
        
        # Детальне логування
        logger.info(f"Message ID: {message_id} - Normalized main response from {self.current_provider}:")
        logger.info(f"  - Response text: {len(gpt_response)} chars")
        logger.info(f"  - activate_booking: {activate_booking}")
        logger.info(f"  - reject_order: {reject_order}")
        logger.info(f"  - change_order: {change_order}")
        logger.info(f"  - double_booking: {double_booking}")
        
        if activate_booking:
            logger.info(f"  - Booking details: {cosmetolog} at {date_order} {time_set_up} for {procedure}")
        
        if double_booking:
            logger.info(f"  - Double booking: specialists={specialists_list}, times={times_set_up_list}")
        
        return normalized
