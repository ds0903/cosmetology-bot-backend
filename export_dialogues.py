#!/usr/bin/env python3
"""
Standalone скрипт для экспорта диалогов на Google Drive
Запускается отдельно от основного сервиса
"""
import os
import sys
import asyncio
from datetime import datetime

# Настройки (измените на реальные)
SALON_EMAILS = ["real_email1@gmail.com", "real_email2@gmail.com"]  # Замените на реальные email
FOLDER_NAME = "Beauty_Digital_8012_Dialogues"

async def export_dialogue(client_id: str, client_name: str):
    """Экспортирует диалог клиента после записи"""
    
    # Устанавливаем переменные окружения
    os.environ['SALON_OWNER_EMAILS'] = ",".join(SALON_EMAILS)
    os.environ['SALON_FOLDER_NAME'] = FOLDER_NAME
    
    try:
        from app.services.dialogue_export import DialogueExporter
        from app.database import SessionLocal, Dialogue
        
        # Получаем диалоги из БД
        db = SessionLocal()
        try:
            dialogues = db.query(Dialogue).filter(
                Dialogue.client_id == client_id
            ).order_by(Dialogue.timestamp.asc()).all()
            
            dialogue_history = [
                {'timestamp': d.timestamp, 'role': d.role, 'message': d.message}
                for d in dialogues
            ]
        finally:
            db.close()
        
        # Создаем тестовые данные записи (в реальности берите из БД)
        booking_data = {
            'date': datetime.now().strftime("%d.%m.%Y"),
            'time': datetime.now().strftime("%H:%M"),
            'service': 'Услуга',
            'specialist': 'Мастер'
        }
        
        # Экспортируем
        exporter = DialogueExporter()
        if exporter.folder_id:
            result = await exporter.save_dialogue_to_drive(
                client_id,
                client_name,
                booking_data,
                dialogue_history
            )
            
            if result:
                print(f"✅ Диалог экспортирован для {client_name}")
                return True
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return False
    
    return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python3 export_dialogues.py CLIENT_ID CLIENT_NAME")
        sys.exit(1)
    
    client_id = sys.argv[1]
    client_name = sys.argv[2]
    
    asyncio.run(export_dialogue(client_id, client_name))
