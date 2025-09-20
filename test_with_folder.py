import asyncio
import os
from datetime import datetime

async def test():
    # Используем ID вашей папки из .env
    os.environ['GOOGLE_DRIVE_FOLDER_ID'] = '17gHvokdqdIzsR8tLzTWcC6H1SWNJJwoM'
    
    from app.services.dialogue_export import DialogueExporter
    
    print("Инициализируем экспортер с вашей папкой...")
    exporter = DialogueExporter()
    
    if not exporter.folder_id:
        print("❌ Папка не установлена")
        return
    
    print(f"✅ Используем папку: {exporter.folder_id}")
    
    # Создаем тестовый диалог
    test_dialogue = [
        {'timestamp': datetime.now(), 'role': 'user', 'message': 'Здравствуйте, хочу записаться на маникюр'},
        {'timestamp': datetime.now(), 'role': 'assistant', 'message': 'Добрый день! Конечно, помогу записать вас на маникюр.'},
        {'timestamp': datetime.now(), 'role': 'user', 'message': 'Можно на завтра в 15:00?'},
        {'timestamp': datetime.now(), 'role': 'assistant', 'message': 'Да, время свободно. Записала вас на завтра на 15:00.'},
    ]
    
    booking_info = {
        'date': '22.08.2025',
        'time': '15:00',
        'service': 'Маникюр',
        'specialist': 'Мастер Анна'
    }
    
    print("Сохраняем диалог в Google Drive...")
    file_id = await exporter.save_dialogue_to_drive(
        'test_client_002',
        'Тестовый Клиент',
        booking_info,
        test_dialogue
    )
    
    if file_id:
        print(f"✅ Успешно! Файл создан с ID: {file_id}")
        print("\n📌 Проверьте папку на Google Drive")
        print("Файл должен появиться в папке с ID: 17gHvokdqdIzsR8tLzTWcC6H1SWNJJwoM")
    else:
        print("❌ Не удалось создать файл")

asyncio.run(test())
