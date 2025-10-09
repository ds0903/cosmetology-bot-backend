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

AIProvider = Literal["claude", "o3", "gemini", "grok"]


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
            self.openai_model = getattr(settings, 'openai_model', 'gpt-4o')
            logger.info(f"OpenAI client initialized with {self.openai_model}")
        
        # Google Gemini
        self.gemini_client = None
        if hasattr(settings, 'gemini_api_key') and settings.gemini_api_key:
            genai.configure(api_key=settings.gemini_api_key)
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
            elif provider == "o3":
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
        
        text = response.content[0].text
        
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

    # multi_ai_service.py
    async def _call_gpt_o3(
            self,
            system_prompt: str,
            user_message: str,
            max_tokens: int,
            temperature: float
    ) -> Dict[str, Any]:
        if not self.openai_client:
            raise ValueError("OpenAI client not initialized. Check OPENAI_API_KEY")

        model = self.openai_model

        # 1) Підсилення вимоги щодо JSON — як у Gemini/Grok
        enhanced_system = (
                system_prompt
                + "\n\nВАЖЛИВО: Відповідай ТІЛЬКИ валідним JSON (без markdown/пояснень/бектиків)."
        )

        # 2) Універсальний витягач тексту з Responses API (на випадок різних версій SDK)
        def _extract_responses_text(resp_obj) -> str:
            # нові SDK мають зручне поле
            text = getattr(resp_obj, "output_text", None)
            if isinstance(text, str) and text.strip():
                return text.strip()

            # fallback: зібрати вручну з resp.output
            out = []
            for item in getattr(resp_obj, "output", []) or []:
                for c in getattr(item, "content", []) or []:
                    # message.content[i].text або .type=="output_text"
                    t = getattr(c, "text", None)
                    if isinstance(t, str):
                        out.append(t)
                    else:
                        # іноді це dict/obj із полем 'text'
                        maybe = getattr(c, "value", None)
                        if isinstance(maybe, str):
                            out.append(maybe)
            return "".join(out).strip()

        try:
            # 3) Якщо це reasoning-модель o3 — Responses API БЕЗ response_format і temperature
            if str(model).lower().startswith("o3"):
                resp = await self.openai_client.responses.create(
                    model=model,
                    # для o3 важливий responses API; 'messages' не використовуємо
                    input=[
                        {"role": "system", "content": enhanced_system},
                        {"role": "user", "content": user_message},
                    ],
                    # temperature НЕ підтримується для o3!
                    # response_format НЕ підтримується для Responses API!
                    # JSON контролюється через промпт (enhanced_system)
                    max_output_tokens=max_tokens,
                )
                text = _extract_responses_text(resp)
                usage = getattr(resp, "usage", None)
                input_tokens = getattr(usage, "input_tokens", 0) or 0
                output_tokens = getattr(usage, "output_tokens", 0) or 0

            else:
                # 4) Для не-o3 (наприклад gpt-4o) — Chat Completions із json_object
                resp = await self.openai_client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": enhanced_system},
                        {"role": "user", "content": user_message},
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={"type": "json_object"},
                )
                text = (resp.choices[0].message.content or "").strip()
                input_tokens = getattr(resp.usage, "prompt_tokens", 0) or 0
                output_tokens = getattr(resp.usage, "completion_tokens", 0) or 0

            if not text:
                # контрольований JSON, щоб адаптер не падав на парсингу
                text = '{"gpt_response":"Вибачте, сталася помилка при отриманні відповіді від o3."}'

            # 5) Невибаглива оцінка вартості (як і раніше)
            cost = (input_tokens * 0.0000025) + (output_tokens * 0.00001)

            logger.info(f"GPT(o3) response length: {len(text)} chars")

            return {
                "response": text,
                "provider": "o3",
                "model": model,
                "tokens_used": {
                    "input": input_tokens,
                    "output": output_tokens,
                    "total": input_tokens + output_tokens
                },
                "cost_estimate": round(cost, 6)
            }

        except Exception as e:
            logger.error(f"OpenAI o3 API error: {e}", exc_info=True)
            # повертаємо валідний JSON-рядок, щоб подальший пайплайн не падав
            return {
                "response": '{"gpt_response":"Вибачте, сталася внутрішня помилка o3. Спробуйте ще раз."}',
                "provider": "o3",
                "model": model,
                "tokens_used": {"input": 0, "output": 0, "total": 0},
                "cost_estimate": 0.0,
            }

    # multi_ai_service.py
    async def _call_gemini(
            self,
            system_prompt: str,
            user_message: str,
            max_tokens: int,
            temperature: float
    ) -> Dict[str, Any]:
        if not self.gemini_client:
            return {
                "response": '{"gpt_response":"Gemini client not initialized. Check GEMINI_API_KEY"}',
                "provider": "gemini",
                "model": getattr(self, "gemini_model", "unknown"),
                "tokens_used": {"input": 0, "output": 0, "total": 0},
                "cost_estimate": 0.0,
            }

        import asyncio
        from typing import Any

        def _extract_text(resp: Any) -> str:
            # Витягуємо ТІЛЬКИ з candidates -> content.parts[].text (без resp.text)
            text = ""
            cands = getattr(resp, "candidates", None) or []
            if cands:
                cand = cands[0]
                content = getattr(cand, "content", None)
                if content and getattr(content, "parts", None):
                    parts = content.parts
                    text = "".join(getattr(p, "text", "") for p in parts if getattr(p, "text", None))
                elif content and getattr(content, "text", None):
                    text = content.text
            return (text or "").strip()

        def _finish_reason(resp: Any) -> str:
            cands = getattr(resp, "candidates", None) or []
            if cands:
                fr = getattr(cands[0], "finish_reason", None)
                # у різних SDK буває Enum або str/int — нормалізуємо до UPPER str
                return str(fr).upper() if fr is not None else ""
            return ""

        try:
            # Жорстка інструкція на JSON
            model = genai.GenerativeModel(
                self.gemini_model,
                system_instruction=(
                        system_prompt
                        + "\n\nКРИТИЧНО: Відповідай ТІЛЬКИ валідним JSON (без пояснень/markdown)."
                ),
            )

            gen_cfg = genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
                response_mime_type="application/json",
            )

            # --- ПЕРШИЙ ВИКЛИК ---
            resp = await asyncio.to_thread(
                model.generate_content,
                [{"role": "user", "parts": [user_message]}],
                generation_config=gen_cfg,
            )
            text = _extract_text(resp)
            fr = _finish_reason(resp)

            # Якщо обрізано/заблоковано/порожньо — робимо одноразовий м’який ретрай
            if (not text) or (fr and fr != "STOP"):
                try:
                    relaxed_safety = [
                        genai.types.SafetySetting(
                            category=genai.types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                            threshold=genai.types.BlockThreshold.BLOCK_NONE
                        ),
                        genai.types.SafetySetting(
                            category=genai.types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                            threshold=genai.types.BlockThreshold.BLOCK_NONE
                        ),
                        genai.types.SafetySetting(
                            category=genai.types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                            threshold=genai.types.BlockThreshold.BLOCK_NONE
                        ),
                        genai.types.SafetySetting(
                            category=genai.types.HarmCategory.HARM_CATEGORY_SEXUAL_CONTENT,
                            threshold=genai.types.BlockThreshold.BLOCK_NONE
                        ),
                    ]
                    resp = await asyncio.to_thread(
                        model.generate_content,
                        [{"role": "user", "parts": [user_message]}],
                        generation_config=gen_cfg,
                        safety_settings=relaxed_safety,
                    )
                    text = _extract_text(resp)
                    fr = _finish_reason(resp)
                except Exception as retry_err:
                    logger.warning(f"Gemini soft-retry failed: {retry_err}")

            # Якщо все ще немає валідної відповіді — контрольований fallback JSON
            if not text:
                text = '{"gpt_response":"Вибачте, сталася помилка при отриманні відповіді від Gemini. Спробуйте ще раз."}'

            # Токени/вартість — м’які дефолти, якщо usage_metadata немає
            tokens_in = getattr(getattr(resp, "usage_metadata", None), "prompt_token_count", 0) or 0
            tokens_out = getattr(getattr(resp, "usage_metadata", None), "candidates_token_count", 0) or 0
            if tokens_in == 0:
                tokens_in = max(1, len(user_message.split()))
            if tokens_out == 0:
                tokens_out = max(1, len(text.split()))

            cost = (tokens_in * 0.00000125) + (tokens_out * 0.00001)

            logger.info(
                f"Gemini response length: {len(text)} chars; "
                f"finish_reason={fr or 'UNKNOWN'}; tokens_in={tokens_in}; tokens_out={tokens_out}"
            )

            return {
                "response": text,
                "provider": "gemini",
                "model": self.gemini_model,
                "tokens_used": {
                    "input": tokens_in,
                    "output": tokens_out,
                    "total": tokens_in + tokens_out
                },
                "cost_estimate": round(cost, 6)
            }

        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            # Повертаємо валідний JSON-рядок, аби адаптер не падав на парсингу
            return {
                "response": '{"gpt_response":"Вибачте, сталася внутрішня помилка Gemini. Спробуйте ще раз."}',
                "provider": "gemini",
                "model": getattr(self, "gemini_model", "unknown"),
                "tokens_used": {"input": 0, "output": 0, "total": 0},
                "cost_estimate": 0.0,
            }

    async def _call_grok(
        self, 
        system_prompt: str, 
        user_message: str, 
        max_tokens: int, 
        temperature: float
    ) -> Dict[str, Any]:
        """
        Виклик Grok 3 (xAI) - OpenAI-сумісний API
        Адаптовано для роботи 1-в-1 як Claude
        """
        if not self.grok_client:
            raise ValueError("Grok client not initialized. Check GROK_API_KEY")
        
        # Додаємо спеціальну інструкцію для Grok
        enhanced_system = system_prompt + "\n\nВАЖЛИВО: Відповідай ТІЛЬКИ валідним JSON без будь-яких додаткових текстів чи пояснень."
        
        response = await self.grok_client.chat.completions.create(
            model=self.grok_model,
            messages=[
                {"role": "system", "content": enhanced_system},
                {"role": "user", "content": user_message}
            ],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        text = response.choices[0].message.content
        
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        cost = (input_tokens * 0.000003) + (output_tokens * 0.000015)
        
        logger.info(f"Grok response length: {len(text)} chars")
        
        return {
            "response": text,
            "provider": "grok",
            "model": self.grok_model,
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
            providers.append("o3")
        if self.gemini_client:
            providers.append("gemini")
        if self.grok_client:
            providers.append("grok")
        
        if not providers:
            return {"error": "No AI providers initialized"}
        
        logger.info(f"Running comparison test with {len(providers)} providers: {providers}")
        
        tasks = [
            self.send_message(provider, system_prompt, user_message, max_tokens)
            for provider in providers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
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
