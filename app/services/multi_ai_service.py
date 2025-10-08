"""
Multi-AI Service для роботи з різними AI провайдерами:
- OpenAI GPT o3
- Google Gemini 2.5 Pro
- xAI Grok 3
- Anthropic Claude (існуючий)
"""

import json
import logging
from typing import Dict, Any, Optional, Literal
from datetime import datetime

# Імпорти клієнтів
import openai
from openai import AsyncOpenAI
import google.generativeai as genai
from anthropic import AsyncAnthropic
import httpx

from ..config import settings

logger = logging.getLogger(__name__)

AIProvider = Literal["claude", "gpt-o3", "gemini", "grok"]


class MultiAIService:
    """Універсальний сервіс для роботи з різними AI моделями"""
    
    def __init__(self):
        """Ініціалізація всіх AI клієнтів"""
        # Claude (вже існує)
        self.claude_client = None
        if settings.claude_api_key_1:
            self.claude_client = AsyncAnthropic(api_key=settings.claude_api_key_1)
            logger.info("Claude client initialized")
        
        # OpenAI GPT
        self.openai_client = None
        if hasattr(settings, 'openai_api_key') and settings.openai_api_key:
            self.openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
            # Зберігаємо назву моделі з .env
            self.openai_model = getattr(settings, 'openai_model', 'gpt-4o')
            logger.info(f"OpenAI client initialized with {self.openai_model}")
        
        # Google Gemini
        self.gemini_client = None
        if hasattr(settings, 'gemini_api_key') and settings.gemini_api_key:
            genai.configure(api_key=settings.gemini_api_key)
            # Зберігаємо назву моделі з .env
            self.gemini_model = getattr(settings, 'gemini_model', 'gemini-1.5-pro-latest')
            self.gemini_client = genai.GenerativeModel(self.gemini_model)
            logger.info(f"Gemini client initialized with {self.gemini_model}")
        
        # xAI Grok (використовує OpenAI-сумісний API)
        self.grok_client = None
        if hasattr(settings, 'grok_api_key') and settings.grok_api_key:
            self.grok_client = AsyncOpenAI(
                api_key=settings.grok_api_key,
                base_url="https://api.x.ai/v1"
            )
            # Зберігаємо назву моделі з .env
            self.grok_model = getattr(settings, 'grok_model', 'grok-beta')
            logger.info(f"Grok client initialized with {self.grok_model}")
    
    async def send_message(
        self,
        provider: AIProvider,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 2000,
        temperature: float = 0.7
    ) -> Dict[str, Any]:
        """
        Універсальний метод для відправки повідомлення до будь-якого провайдера
        
        Returns:
            Dict з полями:
            - response: str - текст відповіді
            - provider: str - використаний провайдер
            - model: str - використана модель
            - tokens_used: dict - використані токени (якщо доступно)
            - cost_estimate: float - приблизна вартість (якщо доступно)
        """
        try:
            if provider == "claude":
                return await self._call_claude(system_prompt, user_message, max_tokens, temperature)
            elif provider == "gpt-o3":
                return await self._call_gpt_o3(system_prompt, user_message, max_tokens, temperature)
            elif provider == "gemini":
                return await self._call_gemini(system_prompt, user_message, max_tokens, temperature)
            elif provider == "grok":
                return await self._call_grok(system_prompt, user_message, max_tokens, temperature)
            else:
                raise ValueError(f"Unknown provider: {provider}")
        except Exception as e:
            logger.error(f"Error calling {provider}: {e}")
            return {
                "response": f"Error: {str(e)}",
                "provider": provider,
                "model": "error",
                "error": str(e)
            }
    
    async def _call_claude(
        self, 
        system_prompt: str, 
        user_message: str, 
        max_tokens: int, 
        temperature: float
    ) -> Dict[str, Any]:
        """Виклик Claude (Anthropic)"""
        if not self.claude_client:
            raise ValueError("Claude client not initialized. Check CLAUDE_API_KEY_1")
        
        response = await self.claude_client.messages.create(
            model=settings.claude_model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
        )
        
        # Парсинг відповіді
        text = response.content[0].text
        
        # Розрахунок вартості для Claude Sonnet 4.5
        # Ціни: $2/M input, $8/M output
        input_tokens = getattr(response.usage, 'input_tokens', 0)
        output_tokens = getattr(response.usage, 'output_tokens', 0)
        cost = (input_tokens * 0.000002) + (output_tokens * 0.000008)
        
        return {
            "response": text,
            "provider": "claude",
            "model": settings.claude_model,
            "tokens_used": {
                "input": input_tokens,
                "output": output_tokens,
                "total": input_tokens + output_tokens
            },
            "cost_estimate": round(cost, 6)
        }

    import httpx
    from typing import Dict, Any

    async def _call_gpt_o3(self, system_prompt: str, user_message: str, max_tokens: int, temperature: float) -> Dict[
        str, Any]:
        if not self.openai_client:
            raise ValueError("OpenAI client not initialized. Check OPENAI_API_KEY")

        url = "https://api.openai.com/v1/responses"
        headers = {
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        }

        # ВАЖНО: Responses API => используем "input" и type="input_text"
        payload = {
            "model": "gpt-o3",
            "instructions": system_prompt,  # вместо отдельного system-сообщения
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": user_message}
                    ],
                }
            ],
            # В Responses API лимит токенов — это max_output_tokens
            "max_output_tokens": max_tokens,
            "temperature": temperature,
            # опц. для reasoning-моделей:
            # "reasoning": {"effort": "medium"},
            # опц. если хочешь принудительно только текст:
            # "modalities": ["text"],
        }

        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=headers, json=payload)

        # Если 4xx/5xx — кинем понятную ошибку с телом ответа
        try:
            data = resp.json()
        except Exception:
            resp.raise_for_status()  # поднимет HTTPError, если не 2xx
            # если 2xx, но тело не JSON — тоже ошибка
            raise RuntimeError(f"Unexpected non-JSON response: {resp.text[:500]}")

        if resp.status_code >= 400:
            # вытащим понятный фрагмент из ошибки OpenAI
            err = data.get("error", {})
            raise RuntimeError(
                f"OpenAI API error {resp.status_code}: "
                f"{err.get('message')} (param={err.get('param')}, code={err.get('code')})"
            )

        # ---------- ПАРСИНГ ОТВЕТА Responses API ----------
        # 1) Самый простой путь: готовая склеенная строка
        if "output_text" in data and data["output_text"]:
            text = data["output_text"]
        else:
            # 2) Универсальный парсинг по фрагментам
            chunks = []
            for item in data.get("output", []):
                for c in item.get("content", []):
                    if c.get("type") == "output_text":
                        chunks.append(c.get("text", ""))
            text = "".join(chunks).strip()

        return {
            "text": text,
            "raw": data,  # опционально для отладки
        }

    async def _call_gemini(
        self, 
        system_prompt: str, 
        user_message: str, 
        max_tokens: int, 
        temperature: float
    ) -> Dict[str, Any]:
        """Виклик Gemini 2.5 Pro (Google)"""
        if not self.gemini_client:
            raise ValueError("Gemini client not initialized. Check GEMINI_API_KEY")
        
        # Gemini об'єднує system і user промпти
        full_prompt = f"{system_prompt}\n\nUser: {user_message}"
        
        # Виклик моделі (синхронний, тому використовуємо asyncio)
        import asyncio
        response = await asyncio.to_thread(
            self.gemini_client.generate_content,
            full_prompt,
            generation_config=genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            )
        )
        
        text = response.text
        
        # Розрахунок вартості для Gemini 2.5 Pro
        # Ціни: $1.25/M input (<200k), $10/M output
        tokens_in = getattr(response, 'prompt_token_count', 0) or len(full_prompt.split())
        tokens_out = getattr(response, 'candidates_token_count', 0) or len(text.split())
        cost = (tokens_in * 0.00000125) + (tokens_out * 0.00001)
        
        return {
            "response": text,
            "provider": "gemini",
            "model": self.gemini_model,  # Повертаємо реальну назву моделі
            "tokens_used": {
                "input": tokens_in,
                "output": tokens_out,
                "total": tokens_in + tokens_out
            },
            "cost_estimate": round(cost, 6)
        }
    
    async def _call_grok(
        self, 
        system_prompt: str, 
        user_message: str, 
        max_tokens: int, 
        temperature: float
    ) -> Dict[str, Any]:
        """Виклик Grok 3 (xAI) - OpenAI-сумісний API"""
        if not self.grok_client:
            raise ValueError("Grok client not initialized. Check GROK_API_KEY")
        
        # Використовуємо стандартний OpenAI chat completions формат
        response = await self.grok_client.chat.completions.create(
            model=self.grok_model,  # Використовуємо модель з .env
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        # Парсинг відповіді (OpenAI формат)
        text = response.choices[0].message.content
        
        # Розрахунок вартості для Grok 3
        # Ціни: $3/M input, $15/M output
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        cost = (input_tokens * 0.000003) + (output_tokens * 0.000015)
        
        return {
            "response": text,
            "provider": "grok",
            "model": self.grok_model,  # Повертаємо реальну назву моделі
            "tokens_used": {
                "input": input_tokens,
                "output": output_tokens,
                "total": input_tokens + output_tokens
            },
            "cost_estimate": round(cost, 6)
        }
    
    async def compare_all(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 500
    ) -> Dict[str, Dict[str, Any]]:
        """
        Порівняльний тест всіх доступних моделей
        
        Returns:
            Dict з результатами від кожної моделі
        """
        import asyncio
        
        providers = []
        if self.claude_client:
            providers.append("claude")
        if self.openai_client:
            providers.append("gpt-o3")
        if self.gemini_client:
            providers.append("gemini")
        if self.grok_client:
            providers.append("grok")
        
        if not providers:
            return {"error": "No AI providers initialized"}
        
        logger.info(f"Running comparison test with {len(providers)} providers: {providers}")
        
        # Запускаємо всі запити паралельно
        tasks = [
            self.send_message(provider, system_prompt, user_message, max_tokens)
            for provider in providers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Формуємо результат
        comparison = {}
        total_cost = 0
        
        for provider, result in zip(providers, results):
            if isinstance(result, Exception):
                comparison[provider] = {
                    "error": str(result),
                    "status": "failed"
                }
            else:
                comparison[provider] = result
                total_cost += result.get("cost_estimate", 0)
        
        comparison["summary"] = {
            "total_providers_tested": len(providers),
            "successful": sum(1 for r in comparison.values() if isinstance(r, dict) and "error" not in r),
            "total_cost_estimate": round(total_cost, 6),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return comparison


# Глобальний інстанс сервісу
_multi_ai_service = None

def get_multi_ai_service() -> MultiAIService:
    """Отримати глобальний інстанс MultiAIService"""
    global _multi_ai_service
    if _multi_ai_service is None:
        _multi_ai_service = MultiAIService()
    return _multi_ai_service
