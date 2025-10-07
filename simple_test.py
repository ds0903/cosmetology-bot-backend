"""
Простий тестовий скрипт для перевірки одної моделі
Використання: python simple_test.py <provider> <message>
Приклад: python simple_test.py claude "Привіт! Як справи?"
"""

import asyncio
import sys
from dotenv import load_dotenv

load_dotenv()

from app.services.multi_ai_service import MultiAIService


async def simple_test(provider: str, message: str):
    """Простий тест однієї моделі"""
    
    service = MultiAIService()
    
    print(f"\n{'='*60}")
    print(f"🤖 Тестуємо: {provider.upper()}")
    print(f"💬 Повідомлення: {message}")
    print(f"{'='*60}\n")
    
    try:
        result = await service.send_message(
            provider=provider,
            system_prompt="You are a helpful assistant.",
            user_message=message,
            max_tokens=500
        )
        
        print(f"✅ Відповідь отримано!\n")
        print(f"📝 Модель: {result['model']}")
        print(f"💬 Відповідь:\n{'-'*60}")
        print(result['response'])
        print(f"{'-'*60}\n")
        print(f"📊 Токени: {result.get('tokens_used', {})}")
        print(f"💰 Вартість: ${result.get('cost_estimate', 0):.6f}")
        
    except Exception as e:
        print(f"❌ Помилка: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("""
Використання:
  python simple_test.py <provider> [message]

Провайдери:
  claude    - Anthropic Claude Sonnet 4.5
  gpt-o3    - OpenAI GPT o3
  gemini    - Google Gemini 2.5 Pro
  grok      - xAI Grok 3

Приклади:
  python simple_test.py claude "Привіт!"
  python simple_test.py gpt-o3 "What is 2+2?"
  python simple_test.py gemini "Розкажи про квантові комп'ютери"
        """)
        sys.exit(1)
    
    provider = sys.argv[1]
    message = sys.argv[2] if len(sys.argv) > 2 else "Hello! How are you?"
    
    if provider not in ["claude", "gpt-o3", "gemini", "grok"]:
        print(f"❌ Невірний провайдер: {provider}")
        print("Доступні: claude, gpt-o3, gemini, grok")
        sys.exit(1)
    
    asyncio.run(simple_test(provider, message))
