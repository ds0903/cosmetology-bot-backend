import asyncio
import os
from datetime import datetime

async def test():
    # Используем реальный email
    os.environ['SALON_OWNER_EMAILS'] = 'elwing4289@gmail.com'
    os.environ['SALON_FOLDER_NAME'] = 'Beauty_Digital_8012_Test'
    
    from app.services.dialogue_export import DialogueExporter
    
    print("Инициализируем экспортер...")
    exporter = DialogueExporter()
    
    if exporter.folder_id:
        print(f"✅ Папка создана/найдена: {exporter.folder_id}")
        print(f"📧 Доступ выдан для: elwing4289@gmail.com")
        print(f"📁 Название папки: Beauty_Digital_8012_Test")
        
        # Создаем тестовый файл
        test_dialogue = [
            {'timestamp': datetime.now(), 'role': 'user', 'message': 'Здравствуйте, хочу записаться'},
            {'timestamp': datetime.now(), 'role': 'assistant', 'message': 'Добрый день! Конечно, помогу вам записаться.'},
            {'timestamp': datetime.now(), 'role': 'user', 'message': 'На маникюр можно?'},
            {'timestamp': datetime.now(), 'role': 'assistant', 'message': 'Да, конечно. Когда вам удобно?'},
        ]
        
        booking_info = {
            'date': '22.08.2025',
            'time': '14:00',
            'service': 'Маникюр',
            'specialist': 'Мастер Анна'
        }
        
        file_id = await exporter.save_dialogue_to_drive(
            'test_client_001',
            'Тестовый Клиент',
            booking_info,
            test_dialogue
        )
        
        if file_id:
            print(f"✅ Тестовый файл создан: {file_id}")
            print("\n📌 Проверьте Google Drive:")
            print("1. Зайдите на drive.google.com под elwing4289@gmail.com")
            print("2. Откройте раздел 'Доступные мне'")
            print("3. Найдите папку 'Beauty_Digital_8012_Test'")
            print("4. В папке должен быть файл с диалогом")
        else:
            print("❌ Не удалось создать файл")
    else:
        print("❌ Папка не создана")

asyncio.run(test())
