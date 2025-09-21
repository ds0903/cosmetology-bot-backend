from fastapi import FastAPI, HTTPException, Depends, Request  
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from typing import Dict, Any, Optional
from datetime import datetime, date, time, timedelta
import asyncio
import json
import logging
import sys
import random
import string
import os
import locale
import pytz
from pytz import timezone

from app.database import get_db, create_tables, SessionLocal, Dialogue  
from app.config import settings, ProjectConfig
from app.models import (
    SendPulseMessage, 
    WebhookResponse, 
    ProjectStats,
    MessageStatus
)
from app.services.message_queue import MessageQueueService
from app.services.claude_service import ClaudeService
from app.services.google_sheets import GoogleSheetsService
from app.services.booking_service import BookingService

# Кеш для хранения данных pending подтверждений
# Формат: {client_id: {"date": "...", "time": "...", "specialist": "...", "service": "...", "timestamp": ...}}
pending_confirmations = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


def generate_message_id() -> str:
    """Generate a unique 10-character alphanumeric message ID"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))


def load_local_config() -> Dict[str, Any]:
    """Load local configuration from local_config.json"""
    config_file = "local_config.json"
    
    if not os.path.exists(config_file):
        logger.warning(f"Local config file '{config_file}' not found. Using default configuration.")
        return {
            "default": {
                "specialists": ["Арина", "Эдуард", "Инна", "Жанна"],
                "services": {
                    "Чистка лица": 3,
                    "Уход за кожей лица": 3,
                    "Пилинг": 2,
                    "Карбоновый пилинг": 2,
                    "Микродермабразия": 2,
                    "Коагуляция": 1,
                    "Мезотерапия": 2,
                    "Биоревитализация": 2,
                    "Контурная пластика": 3,
                    "Алмазная микродермабразия": 2,
                    "Безинъекционная мезотерапия": 2,
                    "Плазмолифтинг": 3,
                    "Карбоновый пилинг — 900": 2,
                    "BB glow": 4,
                    "УЗ чистка": 3,
                    "Гидропилинг": 2,
                    "RF лифтинг": 2,
                    "Фонофорез": 2,
                    "Смас лифтинг лица": 4,
                    "Чистка лица и уход за лицом": 4,
                    "Пилинг — 600": 2,
                    "Массаж лица": 2,
                    "Пирсинг ушей": 1,
                    "Лечение гипергидроза": 2,
                    "Липолитики": 2,
                    "Нитевой лифтинг": 3,
                    "Коррекция мимических морщин": 2,
                    "Гиалуронидаза": 2,
                    "Увеличение и коррекция губ": 2,
                    "Бланчтерапия": 2,
                    "Склеротерапия": 2,
                    "Миостимуляция тела": 1,
                    "Консультация подолога": 2,
                    "Установка скоб": 1,
                    "Медицинский педикюр": 3,
                    "Лазерное лечение онихомикоза": 2,
                    "Изготовление ортопедических стелек": 3,
                    "Общий массаж": 2,
                    "Массаж спины": 2,
                    "Антицеллюлитный массаж": 3,
                    "Вакуумный": 2,
                    "Стоун терапия": 3,
                    "Шоколадный массаж": 3,
                    "Массаж со скрабом": 2,
                    "Шоколадное обертывание": 2,
                    "Грязевые обертывания": 2,
                    "Лимфодренажный массаж": 3,
                    "Прессотерапия": 2,
                    "Коллагенарий": 1,
                    "Солярий горизонтальный": 1,
                    "Солярий вертикальный": 1,
                    "Турбо солярий": 1,
                    "Коррекция формы бровей": 2,
                    "Окрашивание бровей": 2,
                    "Ламинирование бровей": 3,
                    "Окрашивание ресниц": 1,
                    "Ламинирование ресниц": 3,
                    "Маникюр без покрытия": 2,
                    "Маникюр с покр. гель": 4,
                    "Покрытие гель": 2,
                    "Наращивание, коррекция с маникюром": 5,
                    "Педикюр без покрытия": 3,
                    "Педикюр с покрытием гель": 4,
                    "Чистка пальцев ног": 2,
                    "SPA для ног": 3,
                    "Женская стрижка": 3,
                    "Мужская стрижка": 2,
                    "Детская стрижка": 2,
                    "Окрашивание волос": 8,
                    "Уход за волосами": 4,
                    "Укладка волос": 2,
                    "Прически": 4,
                    "Плетение кос": 2,
                    "Кератиновое насыщение волос": 8,
                    "Наращивание волос 1 прядь": 10
                }
            }
        }
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            logger.info(f"Loaded local configuration from '{config_file}'")
            return config
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing local config file '{config_file}': {e}")
        logger.warning("Using default configuration due to parsing error")
        return load_local_config.__defaults__[0] if hasattr(load_local_config, '__defaults__') else {}
    except Exception as e:
        logger.error(f"Error loading local config file '{config_file}': {e}")
        logger.warning("Using default configuration due to loading error")
        return {}





project_configs = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database
    create_tables()
    
    # Create database session for initialization
    db = SessionLocal()
    try:
        # Load local configuration
        local_config = load_local_config()
        
        # Load project configurations
        default_config = ProjectConfig("default")
        
        # Apply configuration from local_config.json if available
        if "default" in local_config:
            default_project_config = local_config["default"]
            default_config.specialists = default_project_config.get("specialists", ["Арина", "Эдуард", "Инна", "Жанна"])
            default_config.services = default_project_config.get("services", {})
            default_config.work_hours = default_project_config.get("work_hours", {
                "start": settings.default_work_start_time,
                "end": settings.default_work_end_time
            })
        else:
            logger.warning("No 'default' configuration found in local config, using hardcoded defaults")
            default_config.specialists = ["Арина", "Эдуард", "Инна", "Жанна"]
            default_config.services = {}
        
        project_configs["default"] = default_config
        
        # Load configurations for other projects if they exist in local_config.json
        for project_id, project_data in local_config.items():
            if project_id != "default":
                project_config = ProjectConfig(project_id)
                project_config.specialists = project_data.get("specialists", default_config.specialists)
                project_config.services = project_data.get("services", default_config.services)
                project_config.work_hours = project_data.get("work_hours", default_config.work_hours)
                project_configs[project_id] = project_config
                logger.info(f"Loaded configuration for project '{project_id}'")
        
        # Create database record for default project if it doesn't exist
        from app.database import Project
        existing_project = db.query(Project).filter(Project.project_id == "default").first()
        if not existing_project:
            db_project = Project(
                project_id="default",
                name="Default Project",
                configuration=default_config.to_dict(),
                is_active=True
            )
            db.add(db_project)
            db.commit()
            logger.info("Created default project in database")
        else:
            logger.info("Default project already exists in database")
        
        # Start dialogue compression background task
        from app.services.dialogue_archiving import run_dialogue_compression_task
        compression_task = asyncio.create_task(run_dialogue_compression_task(project_configs))
        logger.info("Started dialogue compression background task")
        
    finally:
        db.close()
    
    yield
    
    # Cleanup
    logger.info("Shutting down dialogue compression task...")
    if 'compression_task' in locals():
        compression_task.cancel()
        try:
            await compression_task
        except asyncio.CancelledError:
            logger.info("Dialogue compression task cancelled successfully")
    
    project_configs.clear()


app = FastAPI(
    title="Telegram Bot Backend",
    description="FastAPI backend for SendPulse Telegram bot with AI management",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Telegram Bot Backend is running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow()}

@app.post("/make/add-template-message")
async def add_template_message(request: Request):
    """
    Endpoint for Make.com to add template message to dialogue history
    Expected JSON:
    {
        "client_id": "123456789",
        "template_text": "Текст шаблона напоминания",
        "project_id": "default"
    }
    """
    try:
        # Очищаем записи старше 48 часов из кеша
        current_time = datetime.utcnow()
        expired_clients = []
        for cid, data in pending_confirmations.items():
            if (current_time - data['timestamp']).total_seconds() > 172800:  # 48 часов
                expired_clients.append(cid)
        for cid in expired_clients:
            del pending_confirmations[cid]
            logger.debug(f"Removed expired pending confirmation for client {cid}")
        data = await request.json()
        client_id = data.get("client_id")
        template_text = data.get("template_text")
        message_type = data.get("message_type", "reminder")  # Новая строка
        date = data.get("date")
        time = data.get("time")
        specialist = data.get("specialist")
        service = data.get("service")
        project_id = data.get("project_id", "default")
        
        if not client_id or not template_text:
            return JSONResponse(
                status_code=400,
                content={"error": "Missing required fields: client_id or template_text"}
            )
       
        # Add message to dialogue history with timestamp prefix

        # Сохраняем данные в кеш если это запрос подтверждения
        if message_type == "confirmation_request" and date and time:
            pending_confirmations[client_id] = {
                "date": date,
                "time": time,
                "specialist": specialist,
                "service": service,
                "timestamp": datetime.utcnow()
            }
            logger.info(f"Cached pending confirmation for client {client_id}: {date} {time}")
        
        with SessionLocal() as db:
            dialogue_entry = Dialogue(
                project_id=project_id,
                client_id=client_id,
                role="claude: booking_confirmation_pending" if message_type == "confirmation_request" else ("claude: feedback" if message_type == "feedback" else "claude"),
                message=template_text,
                timestamp=datetime.utcnow()
    	    )
            db.add(dialogue_entry)
            db.commit()
            
            logger.info(f"Added {message_type} message to dialogue for client_id={client_id} with role: {dialogue_entry.role}")
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "Template message added to dialogue history",
                    "client_id": client_id
                }
            )
            
    except Exception as e:
        logger.error(f"Error adding template message: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/make/update-booking-status")
async def update_booking_status(request: Request):
    """
    Endpoint for updating booking confirmation status
    Expected JSON:
    {
        "client_id": "123456789",
        "date": "28.08.2025",
        "time": "14:00",
        "action": "booking_confirmed" or "booking_declined",
        "project_id": "default"
    }
    """
    try:
        data = await request.json()
        client_id = data.get('client_id')
        date = data.get('date')
        time = data.get('time')
        action = data.get('action')
        project_id = data.get('project_id', 'default')
        
        if not all([client_id, date, time, action]):
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Missing required parameters"}
            )
        
        # Determine status based on action
        status = 'approved' if action == 'booking_confirmed' else 'cancelled'
        
        # Update status in Google Sheets
        # Get project configuration
        project_config = project_configs.get(project_id, project_configs.get("default"))
        
        # Update status in Google Sheets
        sheets_service = GoogleSheetsService(project_config)
        success = await sheets_service.update_booking_status_in_make_table(
            client_id, date, time, status
        )
        
        # Также пытаемся обновить в основной таблице, если есть специалист в запросе
        specialist = data.get('specialist')
        if specialist:
            main_success = await sheets_service.update_booking_status_in_main_table(
                specialist, date, time, status
            )
            if main_success:
                logger.info(f"Updated main table status to {status} for specialist {specialist}")
        
        
        if success:
            logger.info(f"Booking status updated to {status} for client {client_id}")
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": f"Status updated to {status}",
                    "client_id": client_id,
                    "status": status
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Booking not found or update failed"}
            )
            
    except Exception as e:
        logger.error(f"Error updating booking status: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )


@app.post("/webhook/sendpulse", response_model=WebhookResponse)
async def sendpulse_webhook(
    message: SendPulseMessage,
    db: Session = Depends(get_db)
):
    """
    Main webhook endpoint for SendPulse messages
    Processes incoming messages according to the technical specification
    """
    # Generate unique message ID for tracking
    message_id = generate_message_id()
    client_id = message.tg_id
    contact_send_id = getattr(message, "contact_send_id", None) or message.tg_id
    logger.info(f"Message ID: {message_id} - Using contact_send_id={contact_send_id} for Make.com")
    logger.info(f"Message ID: {message_id} - DEBUG: message has contact_send_id={getattr(message, "contact_send_id", "NOT_FOUND")}")
    
    # Log message receipt with unique ID
    logger.info(f"Message {message.response} get UUID: {message_id}")
    logger.info(f"Message ID: {message_id} - Webhook received: project_id={message.project_id}, client_id={client_id}, count={message.count}, retry={message.retry}")
    logger.debug(f"Message ID: {message_id} - Message content: '{message.response[:200]}...'")
    
    error_count = 0
    
    try:
        # Get project configuration
        project_config = project_configs.get(message.project_id, project_configs.get("default"))
        if not project_config:
            error_count += 1
            logger.error(f"Message ID: {message_id} - Project configuration not found for project_id={message.project_id}")
            return WebhookResponse(
                send_status="FALSE",
                count=f"{error_count}",
                gpt_response="Project configuration not found",
                pic="",
                status="500",
                user_message=message.response
            )
        
        # Ensure project exists in database (create if doesn't exist)
        from app.database import Project
        existing_project = db.query(Project).filter(Project.project_id == message.project_id).first()
        if not existing_project:
            logger.info(f"Message ID: {message_id} - Creating new project in database: {message.project_id}")
            db_project = Project(
                project_id=message.project_id,
                name=f"Project {message.project_id}",
                configuration=project_config.to_dict(),
                is_active=True
            )
            db.add(db_project)
            db.commit()
            logger.info(f"Message ID: {message_id} - Created project {message.project_id} in database")
        
        # Initialize services
        logger.debug(f"Message ID: {message_id} - Initializing queue service for webhook request from client_id={client_id}")
        queue_service = MessageQueueService(db)
        
        # Process incoming message
        logger.info(f"Message ID: {message_id} - Processing incoming message {message.response[:100]} through queue service for client_id={client_id}")
        queue_result = queue_service.process_incoming_message(message, message_id)
        
        if "error" in queue_result:
            error_count += 1
            logger.error(f"Message ID: {message_id} - Queue processing error for client_id={client_id}: {queue_result['error']}")
            return WebhookResponse(
                send_status="FALSE",
                count=f"{error_count}",
                gpt_response=f"Error: {queue_result['error']}",
                pic="",
                status="500",
                user_message=message.response
            )
        
        # Check if this message should be skipped due to retry logic
        if queue_result.get("send_status") == "FALSE":
            logger.info(f"Message ID: {message_id} - Message skipped due to retry logic for client_id={client_id}")
            return WebhookResponse(
                send_status="FALSE",
                count="1",
                gpt_response="",  # Empty response for skipped messages to prevent delivery
                pic="",
                status="200",
                user_message=message.response
            )
        
        # Process the message directly and wait for response
        logger.info(f"Message ID: {message_id} - Processing message directly for client_id={client_id}")
        response_data = await process_message_async(
            message.project_id,
            client_id,
            queue_result["queue_item_id"],
            message_id, contact_send_id
        )
        
        if not response_data:
            error_count += 1
            logger.error(f"Message ID: {message_id} - No response received from processing for client_id={client_id}")
            return WebhookResponse(
                send_status="FALSE",
                count=f"{error_count}",
                gpt_response="",  # Empty response for errors to prevent delivery
                pic="",
                status="200",
                user_message=message.response
            )
        
        # Check for processing errors in response_data
        if response_data.get("error"):
            error_count += response_data.get("error_count", 1)
            logger.error(f"Message ID: {message_id} - Processing errors occurred for client_id={client_id}: {response_data['error']}")
            return WebhookResponse(
                send_status="FALSE",
                count=f"{error_count}",
                gpt_response="",  # Empty response for errors to prevent delivery
                pic="",
                status="200",
                user_message=message.response
            )
        
        # CRITICAL: Use atomic winner claiming to determine send_status
        # This prevents race conditions where multiple messages get TRUE or no messages get TRUE
        queue_service = MessageQueueService(db)
        is_winner = queue_service.try_claim_as_winner(
            message.project_id, 
            client_id, 
            queue_result["queue_item_id"], 
            message_id
        )
        
        # Determine send_status and count based on winner status
        if is_winner:
            # This message won - it's the latest and should return TRUE
            send_status = "TRUE"
            count = "0"  # Successful completion, no errors
            final_gpt_response = response_data["gpt_response"]  # Send the actual AI response
            final_pic = response_data.get("pic", "")
            logger.info(f"Message ID: {message_id} - Message {queue_result['queue_item_id']} won winner claim for client_id={client_id}, returning send_status=TRUE, count=0")
        else:
            # This message was superseded by a newer message - don't send AI response to user
            send_status = "FALSE"
            count = None  # No errors, just superseded by newer message
            final_gpt_response = ""  # CRITICAL: Empty response for FALSE status to prevent duplicate delivery
            final_pic = ""  # No picture for superseded messages
            logger.info(f"Message ID: {message_id} - Message {queue_result['queue_item_id']} lost winner claim for client_id={client_id}, returning send_status=FALSE, count=None")
        
        logger.info(f"Message ID: {message_id} - SENDING WebhookResponse: send_status={send_status}, gpt_response length={len(final_gpt_response) if final_gpt_response else 0}, first 100 chars: {final_gpt_response[:100] if final_gpt_response else "EMPTY"}")
        # Return the final response
        return WebhookResponse(
            send_status=send_status,
            count=count,
            gpt_response=final_gpt_response.replace('\\n', '\n'),
            pic=final_pic,
            status="200",
            user_message=message.response
        )
        
    except Exception as e:
        error_count += 1
        logger.error(f"Message ID: {message_id} - Webhook error: {e}", exc_info=True)
        return WebhookResponse(
            send_status="FALSE",
            count=f"{error_count}",
            gpt_response="",  # Empty response for errors to prevent delivery
            pic="",
            status="500",
            user_message=message.response
        )


async def process_message_async(project_id: str, client_id: str, queue_item_id: str, message_id: str, contact_send_id: str = None) -> dict:
    """
    Process message with AI and return response data
    This implements the full processing pipeline from the technical specification
    """
    logger.info(f"Message ID: {message_id} - Starting message processing for project_id={project_id}, client_id={client_id}, queue_item_id={queue_item_id}")
    
    error_count = 0
    
    try:
        # Get new database session for processing
        from app.database import SessionLocal
        db = SessionLocal()
        
        try:
            # Get project configuration
            project_config = project_configs.get(project_id, project_configs.get("default"))
            if not project_config:
                error_count += 1
                logger.error(f"Message ID: {message_id} - Project configuration not found for project_id={project_id}")
                return {
                    "error": "Project configuration not found",
                    "error_count": error_count,
                    "gpt_response": "Произошла ошибка конфигурации проекта",
                    "pic": ""
                }
            
            # Initialize services
            logger.debug(f"Message ID: {message_id} - Initializing services for client_id={client_id}")
            queue_service = MessageQueueService(db)
            claude_service = ClaudeService(db)
            sheets_service = GoogleSheetsService(project_config)
            booking_service = BookingService(db, project_config, contact_send_id=contact_send_id)
            
            # Get message from queue
            logger.debug(f"Message ID: {message_id} - Getting message from queue for client_id={client_id}")
            message_item = queue_service.get_message_for_processing(project_id, client_id, message_id)
            if not message_item:
                error_count += 1
                logger.warning(f"Message ID: {message_id} - No message found in queue for client_id={client_id}")
                return {
                    "error": "No message found in queue",
                    "error_count": error_count,
                    "gpt_response": "Сообщение не найдено в очереди",
                    "pic": ""
                }
            
            logger.info(f"Message ID: {message_id} - Processing message: '{message_item.aggregated_message[:100]}...' for client_id={client_id}")
            
            # Update message status to processing
            logger.debug(f"Message ID: {message_id} - Updating message status to processing for message_id={message_item.id}")
            queue_service.update_message_status(message_item.id, MessageStatus.PROCESSING, message_id)
            
            # Get dialogue history and zip_history
            logger.debug(f"Message ID: {message_id} - Getting dialogue history for client_id={client_id}")
            dialogue_history = get_dialogue_history(db, project_id, client_id, message_id)
            
            # Get compressed dialogue history (zip_history)
            from app.services.dialogue_archiving import DialogueArchivingService
            dialogue_service = DialogueArchivingService()
            zip_history = dialogue_service.get_zip_history(db, project_id, client_id)
            logger.debug(f"Message ID: {message_id} - Got zip_history for client_id={client_id}: {len(zip_history) if zip_history else 0} characters")
            
            # Получаем текущую дату по Берлину и день недели
            berlin_tz = pytz.timezone('Europe/Berlin')
            berlin_now = datetime.now(berlin_tz)
            current_date = berlin_now.strftime("%d.%m.%Y %H:%M")
            day_of_week = berlin_now.strftime("%A")  # Monday, Tuesday, etc.

            # Step 1: Intent detection (async)
            logger.info(f"Message ID: {message_id} - Starting intent detection for client_id={client_id}")
            try:
                intent_result = await claude_service.detect_intent(
                    project_config,
                    dialogue_history,
                    message_item.aggregated_message,
                    current_date,  # Добавляем
                    day_of_week,   # Добавляем
                    message_id, 
                    zip_history
                )
                if intent_result:
                    logger.debug(f"Message ID: {message_id} - Intent detection result for client_id={client_id}: waiting={intent_result.waiting}, date_order={intent_result.date_order}")
            except Exception as e:
                error_count += 1
                logger.error(f"Message ID: {message_id} - Error in intent detection for client_id={client_id}: {e}")
                # Continue with default intent
                from app.models import IntentDetectionResult
                intent_result = IntentDetectionResult(waiting=1)
                # Ensure intent_result is not None
                if intent_result is None:
                    from app.models import IntentDetectionResult
                    intent_result = IntentDetectionResult(waiting=1)            

            # Steps 2 & 3: Run service identification and slot fetching in parallel when possible
            # Ensure intent_result is not None
            if intent_result is None:
                from app.models import IntentDetectionResult
                intent_result = IntentDetectionResult(waiting=1)
            service_result = None
            available_slots = {}
            reserved_slots = {}
            slots_target_date = None  # Track what date the slots are for
            berlin_tz = timezone('Europe/Berlin')
            current_date = datetime.now(berlin_tz)
            day_of_week = datetime.now().strftime("%A")  # Monday, Tuesday, etc.
            
            if not intent_result.waiting:
                # Client is not just chatting - need service info and slots
                logger.info(f"Message ID: {message_id} - Running parallel service identification and slot fetching for client_id={client_id}")
                logger.debug(f"Message ID: {message_id} - Intent result: waiting={intent_result.waiting}, date_order={intent_result.date_order}, desire_time0={intent_result.desire_time0}, desire_time1={intent_result.desire_time1}")
                
                # Prepare tasks that can run in parallel
                tasks = []
                
                # Task 1: Service identification
                service_task = claude_service.identify_service(
                    project_config,
                    dialogue_history,
                    message_item.aggregated_message,
                    message_id
                )
                tasks.append(service_task)
                
                # Task 2: Get slots based on intent (if we have date/time info)
                slot_task = None
                logger.debug(f"Message ID: {message_id} - Checking intent conditions for slot fetching: date_order='{intent_result.date_order}', desire_time0='{intent_result.desire_time0}', desire_time1='{intent_result.desire_time1}'")
                if intent_result.date_order:
                    logger.info(f"Message ID: {message_id} - Preparing slot fetch for specific date {intent_result.date_order}")
                    target_date = parse_date(intent_result.date_order)
                    if target_date:
                        logger.info(f"Message ID: {message_id} - Parsed date successfully: {target_date}")
                        # Use default time_fraction initially, will adjust after service identification
                        slot_task = sheets_service.get_available_slots_async(db, target_date, 1)
                    else:
                        logger.warning(f"Message ID: {message_id} - Failed to parse date '{intent_result.date_order}' from intent detection")
                elif intent_result.desire_time0 and intent_result.desire_time1:
                    logger.debug(f"Message ID: {message_id} - Preparing slot fetch for time range {intent_result.desire_time0}-{intent_result.desire_time1}")
                    start_time = parse_time(intent_result.desire_time0)
                    end_time = parse_time(intent_result.desire_time1)
                    if start_time and end_time:
                        context_date = extract_date_from_context(dialogue_history, zip_history)
                        if context_date:
                            logger.info(f"Message ID: {message_id} - Found date {context_date} in context, using specific date instead of time range")
                            target_date = parse_date(context_date)
                            if target_date:
                                slot_task = sheets_service.get_available_slots_async(db, target_date, 1)
                            else:
                                slot_task = sheets_service.get_available_slots_by_time_range_async(
                                    db, start_time, end_time, 1
                                )
                        else:
                            slot_task = sheets_service.get_available_slots_by_time_range_async(
                                db, start_time, end_time, 1
                            )
                
                if slot_task:
                    logger.debug(f"Message ID: {message_id} - Slot task created, will fetch slots")
                else:
                    logger.warning(f"Message ID: {message_id} - No slot task created - intent detection conditions not met for slot fetching")
                
                if slot_task:
                    tasks.append(slot_task)
                
                # Task 3: Get client bookings (can run in parallel)
                client_bookings_task = asyncio.to_thread(booking_service.get_client_bookings_as_string, client_id)
                tasks.append(client_bookings_task)
                
                try:
                    # Run tasks in parallel
                    logger.debug(f"Message ID: {message_id} - Running {len(tasks)} tasks in parallel for client_id={client_id}")
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results
                    service_result = results[0] if not isinstance(results[0], Exception) else None
                    if isinstance(results[0], Exception):
                        error_count += 1
                        logger.error(f"Message ID: {message_id} - Error in parallel service identification for client_id={client_id}: {results[0]}")
                        from app.models import ServiceIdentificationResult
                        service_result = ServiceIdentificationResult(time_fraction=1, service_name="unknown")
                    
                    if len(results) > 1 and slot_task:
                        slots = results[1] if not isinstance(results[1], Exception) else None
                        if isinstance(results[1], Exception):
                            error_count += 1
                            logger.error(f"Message ID: {message_id} - Error in parallel slot fetching for client_id={client_id}: {results[1]}")
                        elif slots:
                            available_slots = slots.slots_by_specialist
                            reserved_slots = slots.reserved_slots_by_specialist or {}
                            slots_target_date = slots.target_date
                            logger.info(f"Message ID: {message_id} - Found available slots in parallel for target date {slots_target_date}: {len(available_slots)} specialists")
                            for specialist, specialist_slots in available_slots.items():
                                logger.info(f"Message ID: {message_id} - Specialist {specialist}: {len(specialist_slots)} available slots: {specialist_slots}")
                            logger.info(f"Message ID: {message_id} - Found reserved slots for {len(reserved_slots)} specialists")
                            for specialist, specialist_reserved in reserved_slots.items():
                                logger.info(f"Message ID: {message_id} - Specialist {specialist}: {len(specialist_reserved)} reserved slots: {specialist_reserved}")
                            logger.info(f"Message ID: {message_id} - IMPORTANT: These slots are FOR DATE: {slots_target_date}, checked on: {slots.date_of_checking}")

                            for specialist_key, slots_list in available_slots.items():
                                if isinstance(slots_list, list) and len(slots_list) > 0:
                                    specialist_name = specialist_key.replace('available_slots_', '').title()
                                    logger.info(f"Message ID: {message_id} - 🔧 CLAUDE SEES: {specialist_name} has {len(slots_list)} available slots: {slots_list[:5]}{'...' if len(slots_list) > 5 else ''}")
                                else:
                                    specialist_name = specialist_key.replace('available_slots_', '').title() if specialist_key else 'Unknown'
                                    logger.warning(f"Message ID: {message_id} - 🔧 CLAUDE SEES: {specialist_name} has NO available slots")
                        else:
                            logger.warning(f"Message ID: {message_id} - No available slots returned from slot fetching task")
                            slots_target_date = "no_slots"
                    
                    # Get client bookings result
                    client_bookings_idx = 2 if slot_task else 1
                    if len(results) > client_bookings_idx:
                        client_bookings = results[client_bookings_idx] if not isinstance(results[client_bookings_idx], Exception) else ""
                        if isinstance(results[client_bookings_idx], Exception):
                            error_count += 1
                            logger.error(f"Message ID: {message_id} - Error getting client bookings for client_id={client_id}: {results[client_bookings_idx]}")
                            client_bookings = ""
                    else:
                        client_bookings = ""
                    
                    logger.info(f"Message ID: {message_id} - Parallel processing completed for client_id={client_id}")
                    
                except Exception as e:
                    error_count += 1	
                    logger.error(f"Message ID: {message_id} - Error in parallel processing for client_id={client_id}: {e}")
                    # Fallback to default values
                    from app.models import ServiceIdentificationResult
                    service_result = ServiceIdentificationResult(time_fraction=1, service_name="unknown")
                    client_bookings = ""
                
                # If we need to refetch slots with correct time_fraction after service identification
                # Локальный пересчет слотов вместо повторного запроса к Google Sheets
                if service_result and service_result.time_fraction != 1 and available_slots:
                    logger.info(f"Message ID: {message_id} - Starting local slot recalculation for time_fraction={service_result.time_fraction}")
                    from app.utils.slot_calculator import apply_duration_to_all_specialists, apply_reserved_duration_to_all_specialists
                    
                    # Логгируем слоты ДО пересчета
                    for spec, slots in available_slots.items():
                        if isinstance(slots, list):
                            logger.debug(f"Message ID: {message_id} - BEFORE: {spec} has {len(slots)} slots")
                    
                    # Пересчитываем available_slots
                    # Сохраняем оригинальные available_slots для пересчёта reserved
                    original_available_slots = dict(available_slots)
                    available_slots = apply_duration_to_all_specialists(available_slots, service_result.time_fraction)
                    
                    # Логгируем слоты ПОСЛЕ пересчета
                    for spec, slots in available_slots.items():
                        if isinstance(slots, list):
                            logger.info(f"Message ID: {message_id} - AFTER RECALC: {spec} has {len(slots)} slots for {service_result.time_fraction*30}min service")
                            if len(slots) > 0:
                                logger.debug(f"Message ID: {message_id} - {spec} available times: {', '.join(slots[:5])}{'...' if len(slots) > 5 else ''}")
                    
                    # Пересчитываем reserved_slots
                    if reserved_slots is not None:
                        old_reserved_count = sum(len(s) if isinstance(s, list) else 0 for s in reserved_slots.values())
                        reserved_slots = apply_reserved_duration_to_all_specialists(reserved_slots, original_available_slots, service_result.time_fraction)
                        new_reserved_count = sum(len(s) if isinstance(s, list) else 0 for s in reserved_slots.values())
                        logger.info(f"Message ID: {message_id} - Reserved slots expanded from {old_reserved_count} to {new_reserved_count} for time_fraction={service_result.time_fraction}")
                    
                    logger.info(f"Message ID: {message_id} - Slots recalculated locally, saved 4-8 Google API calls and ~4-8 seconds")
                else:
                    logger.debug(f"Message ID: {message_id} - No slot recalculation needed: time_fraction={getattr(service_result, 'time_fraction', 1)}")
                
            else:
                # Client is just chatting/waiting - only need basic info
                logger.info(f"Message ID: {message_id} - Client is waiting/chatting for client_id={client_id} (waiting={intent_result.waiting}), skipping service identification and slot fetching")
                try:
                    client_bookings = await asyncio.to_thread(booking_service.get_client_bookings_as_string, client_id)
                except Exception as e:
                    error_count += 1
                    logger.error(f"Message ID: {message_id} - Error getting client bookings for client_id={client_id}: {e}")
                    client_bookings = ""
            
            # Step 3: Generate main response (async)
            logger.info(f"Message ID: {message_id} - Generating main response for client_id={client_id}")
            
            # Log detailed slot information for debugging
            total_available_slots = sum(len(slots) for slots in available_slots.values()) if available_slots else 0
            if total_available_slots == 0:
                logger.warning(f"Message ID: {message_id} - NO AVAILABLE SLOTS FOUND for client request. This might cause the bot to say 'no data available'")
            else:
                logger.info(f"Message ID: {message_id} - Found {total_available_slots} total available slots across all specialists")
                
            logger.info(f"Message ID: {message_id} - SENDING TO CLAUDE: available_slots={available_slots}, reserved_slots={reserved_slots}, slots_target_date={slots_target_date}")
            try:
                # Получаем последний record_error если есть
                record_error = None
                from app.database import Dialogue
                last_error = db.query(Dialogue).filter(
                    Dialogue.client_id == client_id,
                    Dialogue.project_id == project_id,
                    Dialogue.role == "system",
                    Dialogue.message.like("RECORD_ERROR:%")
                ).order_by(Dialogue.timestamp.desc()).first()
            
                if last_error:
                    record_error = last_error.message.replace("RECORD_ERROR: ", "")
                    db.delete(last_error)
                    db.commit()
                    logger.info(f"Message ID: {message_id} - Retrieved record_error: {record_error}")
                # Запускаем проверку истории массажей параллельно
                newbie_check_task = asyncio.create_task(
                    sheets_service.check_client_massage_history(client_id)
                )
                logger.info(f"Message ID: {message_id} - Started parallel newbie check for {client_id}")
                
                # ... здесь остается весь существующий код между проверкой и вызовом generate_main_response ...
                
                # Получаем результат проверки новичков (вставить ПРЯМО ПЕРЕД main_response = await claude_service.generate_main_response)
                try:
                    is_newbie = await newbie_check_task
                    newbie_status = 1 if is_newbie else 0
                    logger.info(f"Message ID: {message_id} - Massage newbie status for {client_id}: {newbie_status}")
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Failed to check newbie status: {e}")
                    newbie_status = 1
                
                # Existing call to generate_main_response
                main_response = await claude_service.generate_main_response(
                    project_config,
                    dialogue_history,
                    message_item.aggregated_message,
                    current_date.strftime("%d.%m.%Y %H:%M"),
                    day_of_week, 
                    available_slots,
                    reserved_slots,
                    client_bookings,
                    message_id,
                    slots_target_date,  # Pass the target date information
                    zip_history,  # Pass compressed dialogue history
                    record_error,
                    newbie_status=newbie_status
                )
                logger.debug(f"Message ID: {message_id} - Main response generated for client_id={client_id}: activate_booking={main_response.activate_booking}, reject_order={main_response.reject_order}, change_order={main_response.change_order}")
            except Exception as e:
                error_count += 1
                logger.error(f"Message ID: {message_id} - Error generating main response for client_id={client_id}: {e}")
                # Return error response
                queue_service.update_message_status(message_item.id, MessageStatus.CANCELLED, message_id)
                return {
                    "error": "Error generating AI response",
                    "error_count": error_count,
                    "gpt_response": "Извините, произошла ошибка при генерации ответа. Попробуйте еще раз.",
                    "pic": ""
                }
            
            # Process booking actions (async)
            booking_result = {"success": False, "message": ""}
            if any([main_response.activate_booking, main_response.reject_order, main_response.change_order, main_response.booking_confirmed, main_response.booking_declined]):
                logger.info(f"Message ID: {message_id} - Processing booking action for client_id={client_id}")
                try:
                    booking_result = await booking_service.process_booking_action(main_response, client_id, message_id, contact_send_id)
                    logger.info(f"Message ID: {message_id} - Booking action result for client_id={client_id}: success={booking_result['success']}, message={booking_result['message']}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Message ID: {message_id} - Error processing booking action for client_id={client_id}: {e}")
                    booking_result = {"success": False, "message": "Ошибка при обработке бронирования"}
            else:
                logger.debug(f"Message ID: {message_id} - No booking action required for client_id={client_id}")
            # Обработка подтверждения/отклонения записи
            # Обработка подтверждения/отклонения записи
            if main_response.booking_confirmed or main_response.booking_declined:
                logger.info(f"Message ID: {message_id} - Processing booking confirmation: confirmed={main_response.booking_confirmed}, declined={main_response.booking_declined}")
                try:
                    # Проверяем, есть ли данные в кеше
                    if client_id in pending_confirmations:
                        cached_data = pending_confirmations[client_id]
                        sheets_service = GoogleSheetsService(project_config)
                        status = 'approved' if main_response.booking_confirmed else 'cancelled'
                        
                        # Обновляем статус в таблице Make.com
                        success = await sheets_service.update_booking_status_in_make_table(
                            client_id,
                            cached_data['date'],
                            cached_data['time'],
                            status
                        )
                        
                        if success:
                            logger.info(f"Message ID: {message_id} - Updated Make table status to {status} for {cached_data['date']} {cached_data['time']}")
                            
                            # Также обновляем статус в основной таблице специалиста
                            if 'specialist' in cached_data and cached_data['specialist']:
                                main_table_success = await sheets_service.update_booking_status_in_main_table(
                                    cached_data['specialist'],
                                    cached_data['date'],
                                    cached_data['time'],
                                    status
                                )
                                if main_table_success:
                                    logger.info(f"Message ID: {message_id} - Updated main table status to {status} for specialist {cached_data['specialist']}")
                                else:
                                    logger.warning(f"Message ID: {message_id} - Failed to update main table status for specialist {cached_data['specialist']}")
                            # Очищаем кеш после успешного обновления
                            del pending_confirmations[client_id]
                        else:
                            logger.warning(f"Message ID: {message_id} - Failed to update Make table status")
                    else:
                        logger.warning(f"Message ID: {message_id} - No cached data found for client {client_id}")
                    
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Error processing booking confirmation: {e}")
            
            # Process feedback separately (even if there's no booking action)
            if main_response.feedback:
                logger.info(f"Message ID: {message_id} - Processing client feedback for client_id={client_id}")
                try:
                    await booking_service._save_feedback(main_response, client_id, message_id)
                    logger.info(f"Message ID: {message_id} - Feedback processed successfully for client_id={client_id}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Message ID: {message_id} - Error processing feedback for client_id={client_id}: {e}")
                    # Continue anyway - feedback errors shouldn't break the main flow
            
            def format_time_difference(timestamp1: datetime, timestamp2: datetime) -> str:
                """Format time difference between two timestamps in human-readable format"""
                if not timestamp1 or not timestamp2:
                    return ""
    
                diff = abs(timestamp2 - timestamp1)
                total_seconds = int(diff.total_seconds())
    
                if total_seconds < 60:
                    return f"через {total_seconds} сек"
                elif total_seconds < 3600:
                    minutes = total_seconds // 60
                    return f"через {minutes} мин"
                elif total_seconds < 86400:
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    if minutes > 0:
                        return f"через {hours} ч {minutes} мин"
                    return f"через {hours} ч"
                else:
                    days = total_seconds // 86400
                    hours = (total_seconds % 86400) // 3600
                    if hours > 0:
                        return f"через {days} дн {hours} ч"
                    return f"через {days} дн"

            # Save dialogue entry
            logger.debug(f"Message ID: {message_id} - Saving dialogue entries for client_id={client_id}")
            try:
                save_dialogue_entry(db, project_id, client_id, message_item.original_message, "client", message_id)
                save_dialogue_entry(db, project_id, client_id, main_response.gpt_response, "claude", message_id)
            except Exception as e:
                error_count += 1
                logger.error(f"Message ID: {message_id} - Error saving dialogue entries for client_id={client_id}: {e}")
                # Continue anyway
             
            # Mark current message as completed (only if not superseded)
            current_status = queue_service.check_if_message_superseded(message_item.id, message_id)
            if not current_status:
                logger.debug(f"Message ID: {message_id} - Updating message status to completed for message_id={message_item.id}")
                queue_service.update_message_status(message_item.id, MessageStatus.COMPLETED, message_id)
            else:
                logger.debug(f"Message ID: {message_id} - Message {message_item.id} was superseded, preserving superseded status")
            
            # Prepare final response
            final_response = main_response.gpt_response
            if booking_result["success"]:
                if booking_result.get("message") and booking_result["message"] not in [None, "", "None", "No booking action required"]:
                    final_response += f"\n\n{booking_result['message']}"
            elif booking_result.get("message") and booking_result["message"] not in [None, "", "None"]:
                final_response += f"\n\nОшибка: {booking_result['message']}"
            
            logger.info(f"Message ID: {message_id} - Message processing completed for client_id={client_id} with {error_count} errors")
            
            # Return response data for webhook
            if error_count > 0:
                return {
                    "error": f"Processing completed with {error_count} errors",
                    "error_count": error_count,
                    "gpt_response": final_response,
                    "pic": main_response.pic or ""
                }
            else:
                return {
                    "gpt_response": final_response,
                    "pic": main_response.pic or ""
                }
            
        finally:
            db.close()
            logger.debug(f"Message ID: {message_id} - Database session closed for client_id={client_id}")
            
    except Exception as e:
        error_count += 1
        logger.error(f"Message ID: {message_id} - Error processing message for client_id={client_id}: {e}", exc_info=True)
        # Update message status to failed
        try:
            from app.database import SessionLocal
            db = SessionLocal()
            queue_service = MessageQueueService(db)
            logger.debug(f"Message ID: {message_id} - Marking message as cancelled due to error for queue_item_id={queue_item_id}")
            queue_service.update_message_status(queue_item_id, MessageStatus.CANCELLED, message_id)
            db.close()
        except Exception as cleanup_error:
            error_count += 1
            logger.error(f"Message ID: {message_id} - Failed to update message status during error cleanup: {cleanup_error}")
        
        return {
            "error": f"Critical error during processing: {str(e)}",
            "error_count": error_count,
            "gpt_response": "Произошла критическая ошибка при обработке сообщения",
            "pic": ""
        }


def get_dialogue_history(db: Session, project_id: str, client_id: str, message_id: str) -> str:
    """Get recent dialogue history (last 24 hours) for a client"""
    from app.services.dialogue_archiving import DialogueArchivingService
    
    logger.debug(f"Message ID: {message_id} - Getting recent dialogue history for client_id={client_id}, project_id={project_id}")
    
    dialogue_service = DialogueArchivingService()
    recent_history = dialogue_service.get_recent_dialogue_history(db, project_id, client_id)
    
    logger.debug(f"Message ID: {message_id} - Built recent dialogue history for client_id={client_id}: {len(recent_history)} characters")
    
    return recent_history


def save_dialogue_entry(db: Session, project_id: str, client_id: str, message: str, role: str, message_id: str):
    """Save a dialogue entry using the new dialogue management system"""
    from app.services.dialogue_archiving import DialogueArchivingService
    
    logger.debug(f"Message ID: {message_id} - Saving dialogue entry: client_id={client_id}, role={role}, message_length={len(message)}")
    
    dialogue_service = DialogueArchivingService()
    dialogue_service.add_dialogue_entry(db, project_id, client_id, role, message)
    
    logger.debug(f"Message ID: {message_id} - Dialogue entry saved successfully for client_id={client_id}, role={role}")


def parse_date(date_str: str) -> Optional[date]:
    """Parse date string with improved error handling"""
    if not date_str:
        logger.warning("Empty date string provided")
        return None
    
    logger.debug(f"Parsing date string: '{date_str}'")
    
    try:
        # Clean the input string
        cleaned_date = date_str.strip()
        
        # Handle DD.MM format
        if len(cleaned_date.split('.')) == 2:
            current_year = datetime.now().year
            parsed_date = datetime.strptime(f"{cleaned_date}.{current_year}", "%d.%m.%Y").date()
            logger.info(f"Successfully parsed date '{date_str}' as {parsed_date}")
            
            # Validate that the date is reasonable (not too far in the past or future)
            today = date.today()
            if parsed_date < today:
                # If the date is in the past, assume it's next year
                parsed_date = parsed_date.replace(year=current_year + 1)
                logger.info(f"Date was in the past, adjusting to next year: {parsed_date}")
            elif parsed_date > today.replace(year=current_year + 1):
                logger.warning(f"Date is too far in the future: {parsed_date}")
                return None
                
            return parsed_date
            
        # Handle DD.MM.YYYY format  
        elif len(cleaned_date.split('.')) == 3:
            parsed_date = datetime.strptime(cleaned_date, "%d.%m.%Y").date()
            logger.info(f"Successfully parsed full date '{date_str}' as {parsed_date}")
            return parsed_date
            
        else:
            logger.warning(f"Invalid date format: '{date_str}' (expected DD.MM or DD.MM.YYYY)")
            return None
            
    except ValueError as e:
        logger.warning(f"Failed to parse date '{date_str}': {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing date '{date_str}': {e}")
        return None


def parse_time(time_str: str) -> Optional[time]:
    """Parse time string"""
    logger.debug(f"Parsing time string: '{time_str}'")
    try:
        parsed_time = datetime.strptime(time_str, "%H:%M").time()
        logger.debug(f"Successfully parsed time: {parsed_time}")
        return parsed_time
    except Exception as e:
        logger.warning(f"Failed to parse time '{time_str}': {e}")
        return None


def extract_date_from_context(dialogue_history: str, zip_history: str) -> Optional[str]:
    """Extract date from conversation context"""
    import re
    
    # Combine both histories to search for dates
    combined_text = f"{dialogue_history} {zip_history or ''}"
    
    # Look for dates in DD.MM format in recent context
    date_patterns = [
        r'\b(\d{1,2})\.\s*(\d{1,2})\b',  # 16.08 or 16. 08
        r'на\s+(\d{1,2})\.(\d{1,2})',   # на 16.08
        r'записаться\s+(\d{1,2})\.(\d{1,2})',  # записаться 16.08
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, combined_text)
        if matches:
            # Get the most recent match
            day, month = matches[-1]
            date_str = f"{day.zfill(2)}.{month.zfill(2)}"
            logger.debug(f"Extracted date from context: {date_str}")
            return date_str
    
    # Also check for "16.08" explicitly mentioned in zip_history
    if zip_history and "16.08" in zip_history:
        logger.debug("Found 16.08 mentioned in zip_history")
        return "16.08"
    
    logger.debug("No date found in conversation context")
    return None


@app.get("/projects/{project_id}/stats", response_model=ProjectStats)
async def get_project_stats(project_id: str, db: Session = Depends(get_db)):
    """Get statistics for a project"""
    from app.database import MessageQueue, Booking
    
    total_messages = db.query(MessageQueue).filter(
        MessageQueue.project_id == project_id
    ).count()
    
    total_bookings = db.query(Booking).filter(
        Booking.project_id == project_id
    ).count()
    
    active_bookings = db.query(Booking).filter(
        and_(
            Booking.project_id == project_id,
            Booking.status == "active"
        )
    ).count()
    
    total_clients = db.query(func.count(func.distinct(MessageQueue.client_id))).filter(
        MessageQueue.project_id == project_id
    ).scalar()
    
    return ProjectStats(
        project_id=project_id,
        total_messages=total_messages,
        total_bookings=total_bookings,
        active_bookings=active_bookings,
        total_clients=total_clients
    )


@app.get("/projects/{project_id}/queue")
async def get_queue_stats(project_id: str, db: Session = Depends(get_db)):
    """Get message queue statistics"""
    queue_service = MessageQueueService(db)
    return queue_service.get_queue_stats(project_id)


@app.post("/projects/{project_id}/config")
async def update_project_config(project_id: str, config_data: Dict[str, Any]):
    """Update project configuration"""
    config = ProjectConfig.from_dict(config_data)
    project_configs[project_id] = config
    return {"success": True, "message": "Configuration updated"}


@app.get("/projects/{project_id}/config")
async def get_project_config(project_id: str):
    """Get project configuration"""
    config = project_configs.get(project_id, project_configs.get("default"))
    if not config:
        raise HTTPException(status_code=404, detail="Project not found")
    return config.to_dict()


@app.post("/admin/compress-dialogues")
async def trigger_dialogue_compression(db: Session = Depends(get_db)):
    """Manually trigger dialogue compression for testing"""
    try:
        from app.services.dialogue_archiving import DialogueArchivingService
        
        compression_service = DialogueArchivingService()
        
        # Run compression
        await compression_service.compress_old_dialogues(project_configs)
        
        # Get stats
        stats = compression_service.get_archiving_stats(db)
        
        return {
            "message": "Dialogue compression triggered successfully",
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error triggering dialogue compression: {e}")
        return {"error": str(e)}


@app.post("/admin/reset-dialogues-archived")
async def reset_dialogues_archived(db: Session = Depends(get_db)):
    """Reset archived status of recent dialogues for testing"""
    try:
        from app.database import Dialogue
        from datetime import datetime, timedelta
        
        # Reset dialogues from last 24 hours to unarchived for testing
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        updated_count = db.query(Dialogue).filter(
            Dialogue.timestamp >= cutoff_time
        ).update({Dialogue.is_archived: False})
        
        db.commit()
        
        logger.info(f"Reset {updated_count} dialogues to unarchived status")
        
        return {
            "message": f"Reset {updated_count} dialogues to unarchived status",
            "cutoff_time": cutoff_time.isoformat()
        }
    except Exception as e:
        logger.error(f"Error resetting dialogue archived status: {e}")
        db.rollback()
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port) 
