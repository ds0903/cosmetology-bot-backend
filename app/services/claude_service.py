import json
import re
import asyncio
import random
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta
from anthropic import AsyncAnthropic
from anthropic import InternalServerError, RateLimitError, APIConnectionError
from sqlalchemy.orm import Session
import logging
from pytz import timezone

from ..config import settings, ProjectConfig
from ..utils.prompt_loader import get_prompt
from ..models import (
    IntentDetectionResult, 
    ServiceIdentificationResult, 
    ClaudeMainResponse
)


logger = logging.getLogger(__name__)


class ClaudeService:
    """Service for handling Claude AI interactions with improved error handling and retry logic"""
    
    def __init__(self, db: Session):
        self.db = db
        try:
            self.client1 = AsyncAnthropic(api_key=settings.claude_api_key_1)
            self.client2 = AsyncAnthropic(api_key=settings.claude_api_key_2)
            logger.debug("ClaudeService initialized with two async API clients")
            
            # Circuit breaker state for each client
            self.client1_failures = 0
            self.client2_failures = 0
            self.client1_last_failure = None
            self.client2_last_failure = None
            self.max_failures = 3
            self.circuit_timeout = 300  # 5 minutes
            
            # Simple counter for load balancing
            self.request_counter = 0
            
        except Exception as e:
            logger.error(f"Failed to initialize Claude clients: {e}")
            raise
    
    def _increment_counter(self) -> int:
        """Increment and return request counter for load balancing"""
        self.request_counter += 1
        return self.request_counter
    

    
    def _truncate_dialogue_for_logging(self, dialogue_history: str) -> str:
        """Truncate dialogue history to last 3 messages for logging purposes"""
        if not dialogue_history.strip():
            return "No dialogue history"
        
        lines = dialogue_history.strip().split('\n')
        # Take last 3 lines (should be Client-Claude-Client pattern)
        last_lines = lines[-3:] if len(lines) >= 3 else lines
        
        if len(lines) > 3:
            return f"...{len(lines)-3} earlier messages...\n" + '\n'.join(last_lines)
        else:
            return '\n'.join(last_lines)
    
    def _is_client_circuit_open(self, client_num: int) -> bool:
        """Check if circuit breaker is open for a client"""
        if client_num == 1:
            failures = self.client1_failures
            last_failure = self.client1_last_failure
        else:
            failures = self.client2_failures
            last_failure = self.client2_last_failure
            
        if failures >= self.max_failures and last_failure:
            time_since_failure = (datetime.utcnow() - last_failure).total_seconds()
            return time_since_failure < self.circuit_timeout
        return False
    
    def _record_client_failure(self, client_num: int, message_id: str = None):
        """Record a failure for circuit breaker"""
        if client_num == 1:
            self.client1_failures += 1
            self.client1_last_failure = datetime.utcnow()
            logger.warning(f"Message ID: {message_id} - Client 1 failure recorded: {self.client1_failures}/{self.max_failures}")
        else:
            self.client2_failures += 1
            self.client2_last_failure = datetime.utcnow()
            logger.warning(f"Message ID: {message_id} - Client 2 failure recorded: {self.client2_failures}/{self.max_failures}")
    
    def _record_client_success(self, client_num: int, message_id: str = None):
        """Record a success for circuit breaker (reset failures)"""
        if client_num == 1:
            if self.client1_failures > 0:
                logger.info(f"Message ID: {message_id} - Client 1 success - resetting failure count from {self.client1_failures}")
                self.client1_failures = 0
                self.client1_last_failure = None
        else:
            if self.client2_failures > 0:
                logger.info(f"Message ID: {message_id} - Client 2 success - resetting failure count from {self.client2_failures}")
                self.client2_failures = 0
                self.client2_last_failure = None
    
    async def _cached_claude_request(
        self,
        client: AsyncAnthropic,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
        use_hour_cache: bool = True,
        message_id: str = None
    ):
        """
        Make a Claude API request with 1-hour prompt caching support.
        """
        try:
            logger.debug(f"System prompt length: {len(system_prompt)}, first 500 chars: {system_prompt[:500]}")
            # Добавим хэш системного промпта для отладки кэширования
            import hashlib
            system_hash = hashlib.md5(system_prompt.encode()).hexdigest()[:8]
            logger.debug(f"Message ID: {message_id} - System prompt hash: {system_hash}, length: {len(system_prompt)}")
            
            # Build messages
            messages = [{"role": "user", "content": user_prompt}]
            
            # Build messages
            messages = [{"role": "user", "content": user_prompt}]
            
            # Build kwargs
            kwargs = {
                "model": settings.claude_model,
                "max_tokens": max_tokens,
                "messages": messages
            }
            
            # Add system prompt with caching
            if system_prompt and use_hour_cache:
                kwargs["system"] = [
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral", "ttl": "1h"}
                    }
                ]
                kwargs["extra_headers"] = {"anthropic-beta": "extended-cache-ttl-2025-04-11"}
                logger.info(f"Message ID: {message_id} - Using 1-hour cached system prompt ({len(system_prompt)} chars)")
            
            # Make the request
            response = await client.messages.create(**kwargs)
            # Логируем точные данные от API
            if hasattr(response, 'usage'):
                usage = response.usage
                logger.info(f"Message ID: {message_id} - EXACT API tokens: cache_read={getattr(usage, 'cache_read_input_tokens', 0)}, regular_input={getattr(usage, 'input_tokens', 0)}, output={getattr(usage, 'output_tokens', 0)}")
            
            # Детальное логирование cache usage
            if hasattr(response, 'usage'):
                usage = response.usage
                cache_creation = getattr(usage, 'cache_creation_input_tokens', 0)
                cache_read = getattr(usage, 'cache_read_input_tokens', 0)
                regular_input = getattr(usage, 'input_tokens', 0)
                
                if cache_creation > 0:
                    logger.info(f"Message ID: {message_id} - Cache CREATED: {cache_creation} tokens (cost: ~${cache_creation * 0.000006:.4f})")
                if cache_read > 0:
                    logger.info(f"Message ID: {message_id} - Cache HIT: {cache_read} tokens (saved: ~${(cache_read * 0.000003 - cache_read * 0.0000003):.4f})")
                
                logger.debug(f"Message ID: {message_id} - Token usage: cache_create={cache_creation}, cache_read={cache_read}, regular={regular_input}")
            
            return response
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Cached request failed: {e}")
            raise

    def _get_available_claude_client(self, counter: int, message_id: str = None) -> tuple[AsyncAnthropic, int]:
        """Get available Claude client with circuit breaker logic"""
        # Try preferred client first
        preferred_client_num = 1 if counter % 2 == 0 else 2
        
        if not self._is_client_circuit_open(preferred_client_num):
            client = self.client1 if preferred_client_num == 1 else self.client2
            logger.debug(f"Message ID: {message_id} - Using preferred client {preferred_client_num}")
            return client, preferred_client_num
        
        # Try alternative client
        alternative_client_num = 2 if preferred_client_num == 1 else 1
        if not self._is_client_circuit_open(alternative_client_num):
            client = self.client2 if alternative_client_num == 2 else self.client1
            logger.warning(f"Message ID: {message_id} - Client {preferred_client_num} circuit open, using client {alternative_client_num}")
            return client, alternative_client_num
        
        # Both circuits open - use preferred anyway with warning
        client = self.client1 if preferred_client_num == 1 else self.client2
        logger.error(f"Message ID: {message_id} - Both clients have circuit breakers open, using client {preferred_client_num} anyway")
        return client, preferred_client_num
    
    async def _retry_claude_request(self, request_func, max_retries: int = 3, message_id: str = None):
        """Retry Claude request with exponential backoff"""
        base_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                counter = self._increment_counter()
                client, client_num = self._get_available_claude_client(counter, message_id)
                
                logger.debug(f"Message ID: {message_id} - Attempt {attempt + 1}/{max_retries + 1} using client {client_num}")
                
                # Execute the request
                result = await request_func(client)
                
                # Record success
                self._record_client_success(client_num, message_id)
                logger.debug(f"Message ID: {message_id} - Request successful on attempt {attempt + 1}")
                return result
                
            except InternalServerError as e:
                # Handle 529 overloaded and other 5xx errors
                if hasattr(e, 'status_code') and e.status_code == 529:
                    logger.warning(f"Message ID: {message_id} - Claude API overloaded (529) on attempt {attempt + 1}")
                else:
                    logger.warning(f"Message ID: {message_id} - Claude internal server error on attempt {attempt + 1}: {e}")
                
                # Record failure for circuit breaker
                if 'client_num' in locals():
                    self._record_client_failure(client_num, message_id)
                
                if attempt < max_retries:
                    # Calculate delay with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Message ID: {message_id} - Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Message ID: {message_id} - All retry attempts exhausted for Claude request")
                    raise
                    
            except RateLimitError as e:
                logger.warning(f"Message ID: {message_id} - Claude rate limit exceeded on attempt {attempt + 1}: {e}")
                
                if 'client_num' in locals():
                    self._record_client_failure(client_num, message_id)
                
                if attempt < max_retries:
                    # Longer delay for rate limits
                    delay = base_delay * (3 ** attempt) + random.uniform(0, 2)
                    logger.info(f"Message ID: {message_id} - Rate limited, retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise
                    
            except APIConnectionError as e:
                logger.warning(f"Message ID: {message_id} - Claude API connection error on attempt {attempt + 1}: {e}")
                
                if 'client_num' in locals():
                    self._record_client_failure(client_num, message_id)
                
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Message ID: {message_id} - Connection error, retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise
                    
            except Exception as e:
                logger.error(f"Message ID: {message_id} - Unexpected error in Claude request: {e}")
                if 'client_num' in locals():
                    self._record_client_failure(client_num, message_id)
                raise
    
    async def detect_intent(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        current_date: str,  # Добавлено
   	day_of_week: str,   # Добавлено
        message_id: str,
        zip_history: Optional[str] = None
    ) -> IntentDetectionResult:
        """
        Module 1: Intent detection with proper caching separation
        """
        logger.info(f"Message ID: {message_id} - Starting intent detection for project {project_config.project_id}")
        
        # STATIC system prompt (кэшируется на час)
        system_prompt = get_prompt("intent_detection")
        
        # DYNAMIC user prompt (все переменные данные)
        berlin_tz = timezone('Europe/Berlin')
        current_date = datetime.now(berlin_tz).strftime("%d.%m.%Y %H:%M")
        
        user_prompt_parts = [
            f"current_date: {current_date}",
            f"day_of_week: {day_of_week}",  # Добавляем эту строку
            f"dialogue_history: {dialogue_history}"
        ]
        
        if zip_history:
            user_prompt_parts.append(f"zip_history: {zip_history}")
            
        user_prompt_parts.append(f"current_message: {current_message}")
        user_prompt = "\n".join(user_prompt_parts)
        
        logger.info(f"Message ID: {message_id} - Prompts: system={len(system_prompt)} chars (static), user={len(user_prompt)} chars (dynamic)")

        # Use retry mechanism with caching
        try:
            response = await self._retry_claude_request(
                lambda client: self._cached_claude_request(
                    client=client,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=1000,
                    use_hour_cache=True,
                    message_id=message_id
                ),
                max_retries=3,
                message_id=message_id
            )
            
            raw_response = response.content[0].text
            logger.info(f"Message ID: {message_id} - Claude raw response for intent detection: {raw_response[:500]}")
            logger.info(f"Message ID: {message_id} - Intent detection response length: {len(raw_response)} chars")
            
            # Parse and validate response
            result = self._parse_and_validate_intent_response(raw_response, message_id, 1, 1)
            
            if result:
                intent_result = IntentDetectionResult(**result)
                logger.info(f"Message ID: {message_id} - Claude thinking: {result.get('thinking', 'not provided')}")
                logger.info(f"Message ID: {message_id} - Intent detection completed: waiting={intent_result.waiting}, "
                          f"date_order={intent_result.date_order}, time_range={intent_result.desire_time0}-{intent_result.desire_time1}")
                return intent_result
            else:
                logger.warning(f"Message ID: {message_id} - Intent detection failed after attempts, returning default (waiting=1)")
                return IntentDetectionResult(waiting=1)
                
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in intent detection: {e}", exc_info=True)
            logger.warning(f"Message ID: {message_id} - Falling back to default intent result (waiting=1)")
            return IntentDetectionResult(waiting=1)

    def _parse_and_validate_intent_response(self, raw_response: str, message_id: str, attempt: int, max_attempts: int) -> Optional[dict]:
        """
        Парсит и валидирует ответ от Claude в JSON формат
        Проверяет наличие критических полей согласно промпту
        """
        try:
            # Очищаем ответ от лишних символов
            content = raw_response.strip()
        
            # Удаляем markdown блоки кода если есть
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
        
            # Пытаемся найти JSON в тексте (на случай если есть текст до или после JSON)
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
        
            # Парсим JSON
            result = json.loads(content)
        
            # Проверяем что это словарь
            if not isinstance(result, dict):
                raise ValueError(f"Response is not a dictionary: {type(result)}")
        
            # Логируем thinking если есть
            if 'thinking' in result:
                logger.debug(f"Message ID: {message_id} - Claude thinking: {result['thinking'][:200]}")
        
            # Проверяем критические поля согласно промпту
            # Должен быть либо waiting, либо date_order, либо пара desire_time_0/desire_time_1
            has_waiting = 'waiting' in result
            has_date_order = 'date_order' in result
            has_time_pair = 'desire_time_0' in result and 'desire_time_1' in result
        
            if not (has_waiting or has_date_order or has_time_pair):
                logger.warning(f"Message ID: {message_id} - JSON missing critical fields. Has: {list(result.keys())}")
                if attempt < max_attempts:
                    return None  # Триггерим retry
        
            # Преобразуем формат для совместимости с IntentDetectionResult
            # desire_time_0 и desire_time_1 -> desire_time (берем начало интервала)
            if has_time_pair:
                result['desire_time'] = result.get('desire_time_0')
                # Сохраняем оба значения для логирования
                logger.info(f"Message ID: {message_id} - Time interval detected: {result['desire_time_0']} - {result['desire_time_1']}")
        
            # Убеждаемся что waiting - это число
            if has_waiting:
                result['waiting'] = int(result.get('waiting', 0))
            else:
                result['waiting'] = 0
        
            # Добавляем недостающие поля со значениями None для IntentDetectionResult
            for field in ['name', 'procedure', 'cosmetolog', 'desire_date']:
                if field not in result:
                    result[field] = None
        
            return result
        
        except json.JSONDecodeError as e:
            logger.error(f"Message ID: {message_id} - Failed to parse JSON (attempt {attempt}): {e}")
            logger.error(f"Message ID: {message_id} - Raw content that failed: {raw_response[:500]}")
            return None
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Validation error (attempt {attempt}): {e}")
            return None
    
    async def identify_service(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        message_id: str
    ) -> ServiceIdentificationResult:
        """
        Module 2: Service identification with proper caching
        """
        logger.info(f"Message ID: {message_id} - Starting service identification")
        
        # STATIC system prompt (включает список услуг - редко меняется)
        base_prompt = get_prompt("service_identification")
        services_json = json.dumps(project_config.services, ensure_ascii=False, indent=2)
        system_prompt = f"{base_prompt}\n\nДоступные услуги:\n{services_json}"
        
        # DYNAMIC user prompt
        user_prompt = f"""dialogue_history: {dialogue_history}
current_message: {current_message}"""
        
        logger.info(f"Message ID: {message_id} - Prompts: system={len(system_prompt)} chars (static), user={len(user_prompt)} chars (dynamic)")
        
        try:
            response = await self._cached_claude_request(
                    client=self.client1,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=500,
                    use_hour_cache=True,
                    message_id=message_id
                )
            
            # Use retry mechanism
            
            raw_response = response.content[0].text
            logger.info(f"Message ID: {message_id} - Claude raw response for service identification: {raw_response}")
            logger.info(f"Message ID: {message_id} - Service identification response length: {len(raw_response)} chars")
            
            if not raw_response.strip():
                logger.warning(f"Message ID: {message_id} - Service identification received empty response from Claude")
                return ServiceIdentificationResult(time_fraction=1, service_name="unknown")
            
            result = self._parse_service_response(raw_response, message_id)
            service_result = ServiceIdentificationResult(**result)
            
            logger.info(f"Message ID: {message_id} - Claude thinking parsed result: {result}")
            duration_minutes = service_result.time_fraction * 30
            logger.info(f"Message ID: {message_id} - Service identification completed: service='{service_result.service_name}', duration={service_result.time_fraction} slots ({duration_minutes} minutes)")
            return service_result
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in service identification: {e}", exc_info=True)
            logger.warning(f"Message ID: {message_id} - Falling back to default service result (unknown, 1 slot)")
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
        newbie_status: int = 1  # ДОБАВЛЯЕМ ПАРАМЕТР
    ) -> ClaudeMainResponse:
        """
        Module 3: Main response with proper caching separation
        """
        logger.info(f"Message ID: {message_id} - Starting main response generation")
        
        # STATIC system prompt (только базовый промпт и статические данные)
        base_prompt = get_prompt("main_response")
        specialists = ', '.join(project_config.specialists)
        system_prompt = f"{base_prompt}\n\nСпециалисты: {specialists}"
        
        # DYNAMIC user prompt (все переменные данные)
        user_prompt_parts = [
            f"current_date: {current_date}",
            f"newbie_massage: {newbie_status} (1=новичок в массаже, 0=уже был на массаже)"
            f"dialogue_history: {dialogue_history}",
            f"current_message: {current_message}",
            f"day_of_week: {day_of_week}",  # Добавляем эту строку
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
        
        logger.info(f"Message ID: {message_id} - Prompts: system={len(system_prompt)} chars (static), user={len(user_prompt)} chars (dynamic)")
        
        
        try:
            truncated_history = self._truncate_dialogue_for_logging(dialogue_history)
            logger.info(f"Message ID: {message_id} - Sending async request to Claude for main response generation. Dialogue history: {truncated_history}, current_message: '{current_message[:100]}...'")
            
            # Define the request function for retry mechanism
            async def make_request(client):
                return await self._cached_claude_request(
                    client=client,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=2000,
                    use_hour_cache=True,
                    message_id=message_id
                )
            # Use retry mechanism
            response = await self._retry_claude_request(make_request, max_retries=3, message_id=message_id)
            
            raw_response = response.content[0].text
            logger.info(f"Message ID: {message_id} - Claude raw response for main response: {raw_response}")
            logger.info(f"Message ID: {message_id} - Main response length: {len(raw_response)} chars")
            
            if not raw_response.strip():
                logger.warning(f"Message ID: {message_id} - Main response received empty response from Claude")
                return ClaudeMainResponse(gpt_response="Извините, произошла ошибка. Попробуйте еще раз позже.")
            
            result = self._parse_main_response(raw_response, message_id)
            main_response = ClaudeMainResponse(**result)
            
            logger.info(f"Message ID: {message_id} - Claude thinking parsed result: {result}")
            logger.info(f"Message ID: {message_id} - Main response generated successfully: {len(main_response.gpt_response)} chars, booking actions: activate={main_response.activate_booking}, reject={main_response.reject_order}, change={main_response.change_order}")
            
            if main_response.activate_booking:
                logger.info(f"Message ID: {message_id} - Booking activation requested: specialist={main_response.cosmetolog}, date={main_response.date_order}, time={main_response.time_set_up}")
            elif main_response.reject_order:
                logger.info(f"Message ID: {message_id} - Booking rejection requested: date={main_response.date_reject}, time={main_response.time_reject}")
            elif main_response.change_order:
                logger.info(f"Message ID: {message_id} - Booking change requested: new specialist={main_response.cosmetolog}, new date={main_response.date_order}")
            
            return main_response
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in generate_main_response: {e}", exc_info=True)
            logger.warning(f"Message ID: {message_id} - Falling back to default error response")
            return ClaudeMainResponse(
                gpt_response="Извините, произошла ошибка. Попробуйте еще раз позже."
            )
    
    async def compress_dialogue(
        self,
        project_config: ProjectConfig,
        dialogue_history: str
    ) -> str:
        """
        Module 4: Dialogue compression
        Compresses old dialogue to save tokens
        """
        prompt = self._build_compression_prompt(project_config, dialogue_history)
        
        try:
            # Define the request function for retry mechanism
            async def make_request(client):
                return await client.messages.create(
                    model=settings.claude_model,
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}]
                )
            
            # Use retry mechanism
            response = await self._retry_claude_request(make_request, max_retries=2, message_id=None)
            return response.content[0].text.strip()
            
        except Exception as e:
            logger.warning(f"Dialogue compression failed: {e}, using fallback")
            return dialogue_history[:500] + "..."
    
    def _build_intent_detection_prompt(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        zip_history: Optional[str] = None
    ) -> str:
        """Build prompt for intent detection"""
        base_prompt = get_prompt("intent_detection")
        berlin_tz = timezone('Europe/Berlin')
        current_date = datetime.now(berlin_tz).strftime("%d.%m.%Y %H:%M")
        
        zip_history_section = f"\nzip_history: {zip_history}" if zip_history else ""
        
        return f"""
        {base_prompt}
        
        current_date: {current_date}
        dialogue_history: {dialogue_history}{zip_history_section}
        current_message: {current_message}
        """
    
    def _build_service_identification_prompt(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        zip_history: Optional[str] = None
    ) -> str:
        """Build prompt for service identification"""
        base_prompt = get_prompt("service_identification")
        
        # Replace the hardcoded services dictionary with project-specific services
        services_dict = json.dumps(project_config.services, ensure_ascii=False, indent=4)
        logger.debug(f"Service identification using services: {list(project_config.services.keys())}")
        
        # Replace the hardcoded dictionary in the prompt
        if "СЛОВАРЬ УСЛУГ:" in base_prompt:
            # Find and replace the services dictionary in the prompt
            import re
            pattern = r'СЛОВАРЬ УСЛУГ:\s*\{[^}]*(?:\{[^}]*\}[^}]*)*\}'
            base_prompt = re.sub(pattern, f'СЛОВАРЬ УСЛУГ:\n{services_dict}', base_prompt, flags=re.DOTALL)
            logger.debug("Replaced hardcoded services with project-specific services in service identification prompt")
        
        zip_history_section = f"\nzip_history: {zip_history}" if zip_history else ""
        
        return f"""
        {base_prompt}
        
        dialogue_history: {dialogue_history}{zip_history_section}
        current_message: {current_message}
        """
    
    def _build_service_normalization_prompt(
        self,
        project_config: ProjectConfig,
        service_name: str
    ) -> str:
        """Build prompt for service name normalization"""
        services_dict_str = ""
        for service, duration in project_config.services.items():
            services_dict_str += f"- {service}\n"
        
        prompt = f"""Ты - ассистент для нормализации названий услуг косметологической клиники.

ЗАДАЧА: Найти наиболее точное соответствие входящего названия услуги с эталонным словарем.

СЛОВАРЬ ЭТАЛОННЫХ УСЛУГ:
{services_dict_str}

ПРАВИЛА:
- Ищи семантически близкие соответствия
- Игнорируй различия в регистре, пунктуации и пробелах
- Учитывай сокращения и опечатки
- Если входящее название содержит дополнительные уточнения (цены, время), сопоставляй с основной услугой
- Если точное соответствие не найдено, выбери наиболее близкое по смыслу

ФОРМАТ ВЫВОДА:
Выводи ТОЛЬКО точное название услуги из словаря. Никаких дополнительных слов, объяснений или форматирования.

ПРИМЕРЫ:
Вход: "чистка лица ультразвуком"
Выход: УЗ чистка

Вход: "стрижка волос женщинам"
Выход: Женская стрижка

Вход: "маникюр с гелем"
Выход: Маникюр с покр. гель

Вход: "покраска волос"
Выход: Окрашивание волос

Входящее название услуги: {service_name}
Выход:"""

        return prompt
    
    async def normalize_service_name(
        self,
        project_config: ProjectConfig,
        service_name: str,
        message_id: str
    ) -> str:
        """Normalize service name using Claude to find exact match from services dictionary"""
        logger.info(f"Message ID: {message_id} - Normalizing service name: '{service_name}'")
        
        try:
            # Build normalization prompt
            prompt = self._build_service_normalization_prompt(project_config, service_name)
            
            # Call Claude for service normalization
            response = await self._retry_claude_request(
                lambda client: client.messages.create(
                    model=settings.claude_model,
                    max_tokens=150,  # Short response expected
                    messages=[
                        {
                            "role": "user", 
                            "content": prompt
                        }
                    ]
                ),
                message_id=message_id
            )
            
            # Extract normalized service name
            normalized_service = response.content[0].text.strip()
            
            logger.info(f"Message ID: {message_id} - Service normalization result: '{service_name}' -> '{normalized_service}'")
            
            # Verify the normalized service exists in the dictionary
            if normalized_service in project_config.services:
                logger.info(f"Message ID: {message_id} - Normalized service '{normalized_service}' found in services dictionary")
                return normalized_service
            else:
                logger.warning(f"Message ID: {message_id} - Normalized service '{normalized_service}' not found in services dictionary. Available: {list(project_config.services.keys())}")
                return service_name  # Return original if normalization failed
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in service normalization: {e}")
            return service_name  # Return original service name on error
    
    def _build_main_response_prompt(
        self,
        project_config: ProjectConfig,
        dialogue_history: str,
        current_message: str,
        current_date: str,
        available_slots: Dict[str, Any],
        reserved_slots: Dict[str, Any],
        rows_of_owner: str,
        zip_history: Optional[str],
        record_error: Optional[str],
        slots_target_date: Optional[str] = None
    ) -> str:
        """Build prompt for main response generation"""
        base_prompt = get_prompt("main_response")
        weekday = datetime.now().strftime("%A")
        record_status = record_error if record_error else "-"
        
        # Add project-specific information
        specialists_list = json.dumps(project_config.specialists, ensure_ascii=False)
        services_dict = json.dumps(project_config.services, ensure_ascii=False, indent=2)
        logger.debug(f"Main response using specialists: {project_config.specialists}")
        logger.debug(f"Main response using services: {list(project_config.services.keys())}")
        
        return f"""
        {base_prompt}
        
        СПЕЦИАЛИСТЫ САЛОНА: {specialists_list}
        УСЛУГИ САЛОНА (название: длительность в слотах): {services_dict}
        
        current_date: {current_date}
        weekday: {weekday}
        available_slots: {json.dumps(available_slots, ensure_ascii=False)}
        slots_target_date: {slots_target_date or "unknown"}
        reserved_slots: {json.dumps(reserved_slots, ensure_ascii=False)}
        rows_of_owner: {rows_of_owner}
        zip_history: {zip_history or ""}
        record_status: {record_status}
        dialogue_history: {dialogue_history}
        current_message: {current_message}
        """
    
    def _build_compression_prompt(
        self,
        project_config: ProjectConfig,
        dialogue_history: str
    ) -> str:
        """Build prompt for dialogue compression"""
        base_prompt = get_prompt("dialogue_compression")
        
        return f"""
        {base_prompt}
        
        Диалог для сжатия:
        {dialogue_history}
        """
    
    def _parse_intent_response(self, response: str, message_id: str) -> Dict[str, Any]:
        """Parse intent detection response"""
        try:
            logger.info(f"Message ID: {message_id} - RAW INTENT RESPONSE: '{response}'")
            
            # Handle "json{...}" prefix that Claude sometimes adds
            clean_response = response.strip()
            if clean_response.startswith("json"):
                clean_response = clean_response[4:].strip()
            
            # Strip ```json ... ``` wrapper
            if clean_response.startswith("```"):
                import re
                clean_response = re.sub(r"^```[a-zA-Z]*\s*", "", clean_response)
                clean_response = re.sub(r"\s*```$", "", clean_response)
            
            logger.info(f"Message ID: {message_id} - CLEANED RESPONSE: '{clean_response}'")
            
            result = json.loads(clean_response)
            # Handle both underscore and non-underscore formats for desire_time
            desire_time_0 = result.get("desire_time_0") or result.get("desire_time0")
            desire_time_1 = result.get("desire_time_1") or result.get("desire_time1")
            
            # Priority logic: if we have date_order or desire_time, ignore explicit waiting
            has_date_or_time = result.get("date_order") or desire_time_0
            waiting_value = 0 if has_date_or_time else int(result.get("waiting", 1))
            
            parsed_result = {
                "waiting": waiting_value,
                "date_order": result.get("date_order"),
                "desire_time0": desire_time_0,
                "desire_time1": desire_time_1
            }
            
            logger.info(f"Message ID: {message_id} - PARSING LOGIC: has_date_or_time={has_date_or_time}, claude_waiting={result.get('waiting')}, final_waiting={waiting_value}")
            logger.info(f"Message ID: {message_id} - PARSING LOGIC: desire_time_0='{desire_time_0}', desire_time_1='{desire_time_1}'")
            logger.debug(f"Message ID: {message_id} - Intent response parsed successfully: {parsed_result}")
            return parsed_result
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Failed to parse intent response JSON: {e}")
            logger.warning(f"Message ID: {message_id} - Raw response was: '{response[:300]}'")
            return {"waiting": 1}
    
    def _parse_service_response(self, response: str, message_id: str) -> Dict[str, Any]:
        """Parse service identification response"""
        try:
            logger.debug(f"Message ID: {message_id} - Parsing service response: {response[:200]}...")
            
            import re
            clean_response = response.strip()
            
            # Handle "json{...}" prefix that Claude sometimes adds
            if clean_response.startswith("json"):
                clean_response = clean_response[4:].strip()
            
            # Попытка 1: Извлечь JSON из блока ```json ... ```
            json_match = re.search(r'```json\s*(.*?)\s*```', clean_response, re.DOTALL)
            if json_match:
                clean_response = json_match.group(1).strip()
            else:
                # Попытка 2: Найти первый валидный JSON объект в тексте
                # Ищем начало JSON объекта
                json_start = clean_response.find('{')
                if json_start > 0:  # Если есть текст перед JSON
                    # Пытаемся найти конец JSON, подсчитывая скобки
                    brace_count = 0
                    in_string = False
                    escape_next = False
                    json_end = json_start
                    
                    for i in range(json_start, len(clean_response)):
                        char = clean_response[i]
                        
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
                                    json_end = i + 1
                                    break
                    
                    if json_end > json_start:
                        clean_response = clean_response[json_start:json_end]
            
            # Strip any remaining markdown code blocks
            if clean_response.startswith("```"):
                clean_response = re.sub(r"^```[a-zA-Z]*\s*", "", clean_response)
                clean_response = re.sub(r"\s*```$", "", clean_response)
            
            logger.debug(f"Message ID: {message_id} - Cleaned service response for JSON parsing: {clean_response[:200]}...")
            
            result = json.loads(clean_response)
            parsed_result = {
                "time_fraction": result.get("time_fractions", result.get("time_fraction", 1)),
                "service_name": result.get("service_name", "unknown")
            }
            
            # Если time_fractions - пустой словарь, используем дефолт
            if isinstance(parsed_result["time_fraction"], dict) and not parsed_result["time_fraction"]:
                parsed_result["time_fraction"] = 1
                
            logger.debug(f"Message ID: {message_id} - Service response parsed successfully: {parsed_result}")
            return parsed_result
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Failed to parse service response JSON: {e}")
            logger.warning(f"Message ID: {message_id} - Raw response was: '{response[:100]}...'")
            return {"time_fraction": 1, "service_name": "unknown"}
    
    def _parse_main_response(self, response: str, message_id: str) -> Dict[str, Any]:
        """Parse main response"""
        try:
            logger.debug(f"Message ID: {message_id} - Parsing main response: {response[:200]}...")
            # Handle "json{...}" prefix and clean control characters
            import re
            clean_response = response.strip()
            if clean_response.startswith("json"):
                clean_response = clean_response[4:].strip()
            
            # Strip ```json ... ``` wrapper
            if clean_response.startswith("```"):
                clean_response = re.sub(r"^```[a-zA-Z]*\s*", "", clean_response)
                clean_response = re.sub(r"\s*```$", "", clean_response)
            
            # Remove control characters that can break JSON parsing
            clean_response = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', clean_response)
            
            logger.debug(f"Message ID: {message_id} - Cleaned response for JSON parsing: {clean_response[:200]}...")
            result = json.loads(clean_response)
            parsed_result = {
                "gpt_response": result.get("client_response") or result.get("response") or result.get("message") or result.get("gpt_response", ""),
                "pic": result.get("pic"),
                "activate_booking": result.get("activate_booking"),
                "reject_order": result.get("reject_order"),
                "change_order": result.get("change_order"),
                "booking_confirmed": result.get("booking_confirmed"),
                "booking_declined": result.get("booking_declined"),
                "cosmetolog": result.get("cosmetolog"),
                "time_set_up": result.get("time_set_up"),
                "date_order": result.get("date_order"),
                "time_reject": result.get("time_reject"),
                "date_reject": result.get("date_reject"),
                "procedure": result.get("procedure"),
                "phone": result.get("phone"),
                "name": result.get("name"),
                "feedback": result.get("feedback"),
                "double_booking": result.get("double_booking"),
                "specialists_list": result.get("specialists_list"),
                "times_set_up_list": result.get("times_set_up_list"),
                "procedures_list": result.get("procedures_list")
            }
            

            if parsed_result.get("double_booking") or parsed_result.get("specialists_list"):
                logger.info(f"Message ID: {message_id} - 🔧 DOUBLE BOOKING FIELDS DETECTED:")
                logger.info(f"Message ID: {message_id} - double_booking={parsed_result.get('double_booking')}")
                logger.info(f"Message ID: {message_id} - specialists_list={parsed_result.get('specialists_list')}")
                logger.info(f"Message ID: {message_id} - times_set_up_list={parsed_result.get('times_set_up_list')}")
                logger.info(f"Message ID: {message_id} - procedures_list={parsed_result.get('procedures_list')}")
                logger.info(f"Message ID: {message_id} - activate_booking={parsed_result.get('activate_booking')}")
            
            logger.debug(f"Message ID: {message_id} - Main response parsed successfully: {parsed_result}")
            return parsed_result
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Failed to parse main response JSON: {e}")
            logger.warning(f"Message ID: {message_id} - Raw response was: '{response[:200]}...'")
            return {
                "gpt_response": "Извините, произошла ошибка. Попробуйте еще раз."
            } 
