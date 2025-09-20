# Откройте app/services/booking_service.py и добавьте:

# 1. В начало файла (после других импортов, строка ~11):
try:
    from app.services.dialogue_export import DialogueExporter
    DIALOGUE_EXPORT_ENABLED = True
except Exception as e:
    DIALOGUE_EXPORT_ENABLED = False
    print(f"Dialogue export disabled: {e}")

# 2. В метод __init__ (после self.sheets_service, строка ~23):
        if DIALOGUE_EXPORT_ENABLED:
            try:
                self.dialogue_exporter = DialogueExporter()
            except Exception as e:
                self.dialogue_exporter = None
                logger.warning(f"Failed to init DialogueExporter: {e}")
        else:
            self.dialogue_exporter = None

# 3. После успешной записи (после logger.info о создании записи, строка ~229):
        # Экспортируем диалог если включено
        if self.dialogue_exporter:
            try:
                from app.database import SessionLocal, Dialogue
                db = SessionLocal()
                try:
                    dialogues = db.query(Dialogue).filter(
                        Dialogue.client_id == client_id,
                        Dialogue.project_id == self.project_id
                    ).order_by(Dialogue.timestamp.asc()).all()
                    
                    dialogue_history = [
                        {'timestamp': d.timestamp, 'role': d.role, 'message': d.message}
                        for d in dialogues
                    ]
                finally:
                    db.close()
                
                booking_data = {
                    'date': booking_date.strftime("%d.%m.%Y"),
                    'time': booking_time.strftime("%H:%M"),
                    'service': response.procedure,
                    'specialist': response.cosmetolog
                }
                
                await self.dialogue_exporter.save_dialogue_to_drive(
                    client_id, 
                    response.name or "Клиент",
                    booking_data,
                    dialogue_history
                )
                logger.info(f"Message ID: {message_id} - Dialogue exported to Google Drive")
            except Exception as e:
                logger.error(f"Message ID: {message_id} - Failed to export dialogue: {e}")
