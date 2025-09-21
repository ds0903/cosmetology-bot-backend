from typing import Dict, Any, Optional, List
from datetime import datetime, date, time, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc
import logging

from ..database import Booking, Feedback
from ..models import ClaudeMainResponse, BookingRecord
from ..config import ProjectConfig
from ..services.google_sheets import GoogleSheetsService
from app.services.dialogue_export import DialogueExporter

logger = logging.getLogger(__name__)


class BookingService:
    """Service for handling booking operations"""
    
    def __init__(self, db: Session, project_config: ProjectConfig, contact_send_id: str = None):
        self.db = db
        self.project_config = project_config
        self.contact_send_id = contact_send_id
        self.sheets_service = GoogleSheetsService(project_config)
        self.dialogue_exporter = DialogueExporter(project_name=project_config.project_id)
        logger.debug(f"BookingService initialized for project {project_config.project_id}")
    
        logger.info(f"BookingService init: contact_send_id={contact_send_id}")
    async def process_booking_action(self, claude_response: ClaudeMainResponse, client_id: str, message_id: str, contact_send_id: str = None) -> Dict[str, Any]:
        """Process booking action from Claude response"""
        logger.info(f"Message ID: {message_id} - Processing booking action for client_id={client_id}")
        logger.debug(f"Message ID: {message_id} - Booking action details: activate={claude_response.activate_booking}, reject={claude_response.reject_order}, change={claude_response.change_order}")
        
        result = {"success": False, "message": "", "action": None}
        
        try:
            if claude_response.activate_booking:

                if claude_response.double_booking and claude_response.specialists_list:
                    logger.info(f"Message ID: {message_id} - Processing DOUBLE booking activation")
                    result = await self._activate_double_booking(claude_response, client_id, message_id,
                                                                 contact_send_id)
                else:
                    logger.info(f"Message ID: {message_id} - Processing SINGLE booking activation")
                    result = await self._activate_booking(claude_response, client_id, message_id, contact_send_id)
                result["action"] = "activate"

            elif claude_response.reject_order:
                logger.info(f"Message ID: {message_id} - Processing booking rejection for client_id={client_id}")
                result = await self._reject_booking(claude_response, client_id, message_id)
                result["action"] = "reject"
            elif claude_response.change_order:
                logger.info(f"Message ID: {message_id} - Processing booking change for client_id={client_id}")
                result = await self._change_booking(claude_response, client_id, message_id)
                result["action"] = "change"
            else:
                logger.debug(f"Message ID: {message_id} - No booking action required for client_id={client_id}")
                result = {"success": True, "message": "No booking action required", "action": "none"}
            
            
            logger.info(f"Message ID: {message_id} - Booking action completed for client_id={client_id}: {result['action']} - success={result['success']}")
            return result
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error processing booking action for client_id={client_id}: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Ошибка при обработке заказа: {str(e)}",
                "action": "error"
            }
    
    async def _activate_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str, contact_send_id: str = None) -> Dict[str, Any]:
        """Activate a new booking"""
        logger.info(f"Message ID: {message_id} - 🔧 IMPROVED: Activating booking for client_id={client_id}")
        logger.info(f"DEBUG START: _activate_booking called with contact_send_id={contact_send_id}")
        
        # ИСПРАВЛЕНИЕ: Добавляем детальное логирование типа записи
        logger.info(f"Message ID: {message_id} - Checking booking type: double_booking={response.double_booking}, specialists_list={response.specialists_list}")
        logger.info(f"Message ID: {message_id} - Single booking fields: cosmetolog={response.cosmetolog}, date={response.date_order}, time={response.time_set_up}")
        
        try:
            if response.double_booking and response.specialists_list:
                # Для двойной записи проверяем specialists_list
                if not response.specialists_list or len(response.specialists_list) < 2 or not response.date_order or not response.time_set_up:
                    logger.warning(f"Message ID: {message_id} - Missing required fields for DOUBLE booking for client_id={client_id}: specialists={response.specialists_list}, date={response.date_order}, time={response.time_set_up}")
                    return {
                        "success": False,
                        "message": "Недостаточно данных для создания двойной записи"
                    }
                logger.info(f"Message ID: {message_id} - DOUBLE booking detected, redirecting to _activate_double_booking")
                # Перенаправляем на метод двойной записи
                return await self._activate_double_booking(response, client_id, message_id, contact_send_id)
            else:
                # Для одинарной записи проверяем cosmetolog
                if not response.cosmetolog or not response.date_order or not response.time_set_up:
                    logger.warning(f"Message ID: {message_id} - Missing required fields for SINGLE booking for client_id={client_id}: specialist={response.cosmetolog}, date={response.date_order}, time={response.time_set_up}")
                    return {
                        "success": False,
                        "message": "Недостаточно данных для создания записи"
                    }
            
            # Parse date and time
            try:
                booking_date = datetime.strptime(response.date_order, "%d.%m.%Y").date()
            except ValueError:
                try:
                    booking_date = datetime.strptime(response.date_order, "%d.%m").date().replace(year=datetime.now().year)
                except ValueError:
                    logger.warning(f"Message ID: {message_id} - Invalid date format for client_id={client_id}: {response.date_order}")
                    return {
                        "success": False,
                        "message": f"Неверный формат даты: {response.date_order}"
                    }
            
            try:
                booking_time = datetime.strptime(response.time_set_up, "%H:%M").time()
            except ValueError:
                logger.warning(f"Message ID: {message_id} - Invalid time format for client_id={client_id}: {response.time_set_up}")
                return {
                    "success": False,
                    "message": f"Неверный формат времени: {response.time_set_up}"
                }
            
            # Check if specialist exists
            if response.cosmetolog not in self.project_config.specialists:
                logger.warning(f"Message ID: {message_id} - Unknown specialist requested: {response.cosmetolog}, available: {self.project_config.specialists}")
                return {
                    "success": False,
                    "message": f"Специалист {response.cosmetolog} не найден"
                }
            
            # Determine service duration
            duration_slots = 1
            normalized_service = response.procedure
            
            if response.procedure and response.procedure in self.project_config.services:
                # Direct match found
                duration_slots = self.project_config.services[response.procedure]
                logger.info(f"Message ID: {message_id} - Service '{response.procedure}' requires {duration_slots} slots ({duration_slots * 30} minutes)")
            elif response.procedure:
                # No direct match - try service normalization
                logger.info(f"Message ID: {message_id} - Service '{response.procedure}' not found in dictionary, attempting normalization...")
                
                from ..services.claude_service import ClaudeService
                from ..database import SessionLocal
                
                try:
                    # Create a new Claude service for normalization
                    normalize_db = SessionLocal()
                    claude_service = ClaudeService(normalize_db)
                    
                    normalized_service = await claude_service.normalize_service_name(
                        self.project_config, 
                        response.procedure, 
                        message_id
                    )
                    
                    if normalized_service in self.project_config.services:
                        duration_slots = self.project_config.services[normalized_service]
                        logger.info(f"Message ID: {message_id} - Normalized service '{normalized_service}' requires {duration_slots} slots ({duration_slots * 30} minutes)")
                    else:
                        logger.warning(f"Message ID: {message_id} - Service normalization failed, using default duration: 1 slot (30 minutes)")
                    
                    normalize_db.close()
                    
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Error during service normalization: {e}")
                    logger.warning(f"Message ID: {message_id} - Using default duration: 1 slot (30 minutes)")
            else:
                logger.warning(f"Message ID: {message_id} - No service specified, using default duration: 1 slot (30 minutes)")

            logger.info(f"Message ID: {message_id} - 🔧 STARTING ATOMIC SLOT CHECK + BOOKING")
            logger.info(f"Message ID: {message_id} - Checking specialist={response.cosmetolog}, date={booking_date}, time={booking_time}, duration={duration_slots}")
            
            try:
                # 1. Получаем текущее состояние слотов
                current_slots = await self.sheets_service.get_available_slots_async(self.db, booking_date, duration_slots)
                specialist_key = f'available_slots_{response.cosmetolog.lower()}'
                reserved_key = f'reserved_slots_{response.cosmetolog.lower()}'
                
                logger.info(f"Message ID: {message_id} - SLOT CHECK: Got current slots for {response.cosmetolog}")
                logger.info(f"Message ID: {message_id} - SPECIALIST KEY: {specialist_key}")
                logger.info(f"Message ID: {message_id} - RESERVED KEY: {reserved_key}")
                
                # DEBUG: Показать все доступные ключи
                logger.info(f"Message ID: {message_id} - ALL AVAILABLE KEYS: {list(current_slots.slots_by_specialist.keys())}")
                logger.info(f"Message ID: {message_id} - ALL RESERVED KEYS: {list(current_slots.reserved_slots_by_specialist.keys())}")
                
                available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                
                logger.info(f"Message ID: {message_id} - Available slots for {response.cosmetolog}: {available_slots}")
                logger.info(f"Message ID: {message_id} - Reserved slots for {response.cosmetolog}: {reserved_slots}")
                
                # ИСПРАВЛЕНИЕ: Если нет слотов для специалиста, проверим другие варианты имени
                if not available_slots and not reserved_slots:
                    # Попробуем найти специалиста без учета регистра
                    for key in current_slots.slots_by_specialist.keys():
                        if response.cosmetolog.lower() in key.lower():
                            logger.info(f"Message ID: {message_id} - Found alternative key: {key}")
                            specialist_key = key
                            reserved_key = key.replace('available_slots_', 'reserved_slots_')
                            available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                            reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                            logger.info(f"Message ID: {message_id} - Using alternative - Available: {available_slots}, Reserved: {reserved_slots}")
                            break
                
                # 2. Проверяем конкретное время
                requested_time = booking_time.strftime("%H:%M")
                
                # Проверяем все слоты, которые займет эта запись
                slots_to_check = []
                for i in range(duration_slots):
                    check_time = (datetime.combine(booking_date, booking_time) + timedelta(minutes=30*i)).time()
                    slots_to_check.append(check_time.strftime("%H:%M"))
                
                logger.info(f"Message ID: {message_id} - Need to check {len(slots_to_check)} slots: {slots_to_check}")
                
                # 3. КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Упрощенная проверка доступности
                # Если нет зарезервированных слотов для этого времени - слот доступен
                unavailable_slots = []
                
                for slot_time in slots_to_check:
                    # Проверяем только резервированные слоты
                    if slot_time in reserved_slots:
                        unavailable_slots.append(slot_time)
                        logger.warning(f"Message ID: {message_id} - ❌ Slot {slot_time} IS RESERVED")
                    else:
                        logger.info(f"Message ID: {message_id} - ✅ Slot {slot_time} is FREE")
                
                # НОВАЯ ЛОГИКА: Если слота нет в reserved_slots - он доступен
                if unavailable_slots:
                    logger.error(f"Message ID: {message_id} - 🚨 BOOKING BLOCKED: Unavailable slots found: {unavailable_slots}")
                    return {
                        "success": False,
                        "message": f"Время {', '.join(unavailable_slots)} уже занято",
                        "record_error": f"Слоты недоступны: {', '.join(unavailable_slots)}"
                    }
                
                logger.info(f"Message ID: {message_id} - ✅ ALL SLOTS AVAILABLE - proceeding with booking")
                
            except Exception as e:
                logger.error(f"Message ID: {message_id} - Error in atomic slot check: {e}, aborting booking", exc_info=True)
                return {
                    "success": False,
                    "message": "Ошибка проверки доступности",
                    "record_error": f"Ошибка проверки: {str(e)}"
                }
            
            # 4. СОЗДАЕМ ЗАПИСЬ (только если все проверки прошли)
            end_time = datetime.combine(booking_date, booking_time) + timedelta(minutes=30 * duration_slots)
            logger.info(f"Message ID: {message_id} - Creating new booking: client_id={client_id}, specialist={response.cosmetolog}")
            logger.info(f"Message ID: {message_id} -   Service: {normalized_service} ({duration_slots} slots)")
            logger.info(f"Message ID: {message_id} -   Time: {booking_date} {booking_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
            
            booking = Booking(
                project_id=self.project_config.project_id,
                specialist_name=response.cosmetolog,
                appointment_date=booking_date,
                appointment_time=booking_time,
                client_id=client_id,
                client_name=response.name,
                service_name=normalized_service,
                client_phone=response.phone,
                duration_minutes=duration_slots * 30,
                status="active"
            )
            
            self.db.add(booking)
            self.db.commit()
            self.db.refresh(booking)
            
            logger.info(f"Message ID: {message_id} - ✅ Booking created successfully: booking_id={booking.id}")
            
            # 5. Обновляем Google Sheets сразу после создания записи
            try:
                logger.debug(f"Message ID: {message_id} - Updating Google Sheets for booking {booking.id}")
                sheets_success = await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                if sheets_success:
                    logger.info(f"Message ID: {message_id} - ✅ Google Sheets updated successfully")
                else:
                    logger.warning(f"Message ID: {message_id} - ⚠️ Google Sheets update returned false")
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - ❌ Failed to update Google Sheets: {sheets_error}")
                # Don't fail the booking for sheets sync issues
            # Экспортируем диалог на Google Drive
            try:
                from app.database import SessionLocal, Dialogue
                db = SessionLocal()
                try:
                    dialogues = db.query(Dialogue).filter(
                        Dialogue.client_id == client_id,
                        Dialogue.project_id == self.project_config.project_id
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
                # Не прерываем процесс записи если экспорт не удался

            # Add to Make.com table for 24h reminders
            logger.info(f"DEBUG: self.contact_send_id={self.contact_send_id}, client_id={client_id}")
            logger.info(f"DEBUG: Using contact_send_id={contact_send_id} for Make.com table")
            try:
                make_booking_data = {
                    'date': booking_date.strftime("%d.%m.%Y"),
                    'client_id': contact_send_id if contact_send_id else client_id,  # Используем SendPulse ID для Make.com
                    'messenger_client_id': client_id,  # ДОБАВЛЯЕМ: Messenger ID для истории
                    'time': booking_time.strftime('%H:%M'),
                    'client_name': response.name or "Клиент",
                    'service': response.procedure or "Услуга",
                    'specialist': response.cosmetolog
                }
                logger.info(f"Message ID: {message_id} - About to call add_booking_to_make_table_async with data: {make_booking_data}")
                await self.sheets_service.add_booking_to_make_table_async(make_booking_data)
                logger.info(f"Message ID: {message_id} - Added booking to Make.com table for 24h reminder")
            except Exception as make_error:
                logger.error(f"Message ID: {message_id} - Failed to add to Make.com table: {make_error}")
                # Don't fail the booking if Make.com table update fails 

            # 🔧 ИСПРАВЛЕНО: Google Sheets уже обновлены выше (пункт 5)
            
            return {
                "success": True,
                #  "message": f"Запись создана: {response.cosmetolog}, {booking_date.strftime('%d.%m.%Y')} {booking_time.strftime('%H:%M')}",
                "message": None,
                "booking_id": booking.id
            }
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error creating booking for client_id={client_id}: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Ошибка при создании записи: {str(e)}"
            }

    async def _reject_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[str, Any]:
        """Reject/cancel a booking (single or double)"""
        try:
            # Проверяем, это двойная запись или одинарная
            if response.double_booking and response.specialists_list:
                logger.info(f"Message ID: {message_id} - Processing DOUBLE booking rejection")
                return await self._reject_double_booking(response, client_id, message_id)
            else:
                logger.info(f"Message ID: {message_id} - Processing SINGLE booking rejection")
                return await self._reject_single_booking(response, client_id, message_id)
        except Exception as e:
            return {
                "success": False,
                "message": f"Ошибка при отмене записи: {str(e)}"
            }

    async def _reject_single_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Reject/cancel a single booking"""
        # Ваш существующий код _reject_booking переместить сюда
        # ... (весь код из текущего _reject_booking)

    async def _reject_double_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Reject/cancel a double booking"""
        try:
            if not response.specialists_list or len(response.specialists_list) < 2:
                return {
                    "success": False,
                    "message": "Недостаточно специалистов для отмены двойной записи"
                }

            # Parse date and time
            booking_date = self._parse_date(response.date_reject)
            booking_time = self._parse_time(response.time_reject)

            if not booking_date or not booking_time:
                return {
                    "success": False,
                    "message": "Неверный формат даты или времени"
                }

            cancelled_bookings = []

            # Найти и отменить записи для ОБОИХ мастеров
            for specialist in response.specialists_list:
                booking = self.db.query(Booking).filter(
                    and_(
                        Booking.project_id == self.project_config.project_id,
                        Booking.client_id == client_id,
                        Booking.specialist_name == specialist,
                        Booking.appointment_date == booking_date,
                        Booking.appointment_time == booking_time,
                        Booking.status == "active"
                    )
                ).first()

                if booking:
                    # Cancel booking
                    booking.status = "cancelled"
                    booking.updated_at = datetime.utcnow()
                    cancelled_bookings.append(booking)

                    # Clear slot in Google Sheets
                    try:
                        duration_slots = booking.duration_minutes // 30
                        await self.sheets_service.clear_booking_slot_async(
                            booking.specialist_name,
                            booking.appointment_date,
                            booking.appointment_time,
                            duration_slots
                        )

                        # Log cancellation
                        cancellation_data = {
                            "date": booking.appointment_date.strftime("%d.%m"),
                            "full_date": booking.appointment_date.strftime("%d.%m.%Y"),
                            "time": str(booking.appointment_time),
                            "client_id": client_id,
                            "client_name": booking.client_name or "Клиент",
                            "service": f"{booking.service_name} (двойная запись)",
                            "specialist": specialist
                        }
                        await self.sheets_service.log_cancellation(cancellation_data)

                    except Exception as sheets_error:
                        logger.error(
                            f"Message ID: {message_id} - Failed to clear booking slot for {specialist}: {sheets_error}")

            self.db.commit()

            if cancelled_bookings:
                specialists_names = [b.specialist_name for b in cancelled_bookings]
                return {
                    "success": True,
                    "message": f"Двойная запись отменена: {', '.join(specialists_names)}",
                    "booking_ids": [b.id for b in cancelled_bookings]
                }
            else:
                return {
                    "success": False,
                    "message": "Записи не найдены для отмены"
                }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error cancelling double booking: {e}")
            return {
                "success": False,
                "message": f"Ошибка при отмене двойной записи: {str(e)}"
            }

    async def _change_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[str, Any]:
        """Change an existing booking (single or double)"""
        try:
            # Проверяем, это перенос в двойную запись или из двойной записи
            if response.double_booking and response.specialists_list:
                logger.info(f"Message ID: {message_id} - Processing DOUBLE booking change")
                return await self._change_double_booking(response, client_id, message_id)
            else:
                logger.info(f"Message ID: {message_id} - Processing SINGLE booking change")
                return await self._change_single_booking(response, client_id, message_id)
        except Exception as e:
            return {
                "success": False,
                "message": f"Ошибка при изменении записи: {str(e)}"
            }

    async def _change_single_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Change a single booking"""
        # Ваш существующий код _change_booking переместить сюда
        # ... (весь код из текущего _change_booking)

    async def _change_double_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Change a double booking"""
        try:
            if not response.specialists_list or len(response.specialists_list) < 2:
                return {
                    "success": False,
                    "message": "Недостаточно специалистов для переноса двойной записи"
                }

            # Найти существующие записи для переноса
            old_bookings = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.status == "active"
                )
            ).all()

            if not old_bookings:
                return {
                    "success": False,
                    "message": "Активные записи не найдены"
                }

            # Parse new date and time
            new_date = self._parse_date(response.date_order)
            new_time = self._parse_time(response.time_set_up)

            if not new_date or not new_time:
                return {
                    "success": False,
                    "message": "Неверный формат новой даты или времени"
                }

            # Проверить доступность ОБОИХ новых мастеров
            specialist1, specialist2 = response.specialists_list[0], response.specialists_list[1]

            slot1_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist1, new_date,
                                                                                          new_time)
            slot2_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist2, new_date,
                                                                                          new_time)

            if not slot1_available or not slot2_available:
                occupied_specialists = []
                if not slot1_available:
                    occupied_specialists.append(specialist1)
                if not slot2_available:
                    occupied_specialists.append(specialist2)
                return {
                    "success": False,
                    "message": f"Новое время занято у мастера(ов): {', '.join(occupied_specialists)}"
                }

            # Найти записи для переноса (берем две последние активные записи клиента)
            bookings_to_change = sorted(old_bookings, key=lambda x: x.created_at, reverse=True)[:2]

            if len(bookings_to_change) < 2:
                return {
                    "success": False,
                    "message": "Недостаточно записей для переноса в двойную запись"
                }

            # Сохранить старые данные для логирования
            old_data = []
            for booking in bookings_to_change:
                old_data.append({
                    "specialist": booking.specialist_name,
                    "date": booking.appointment_date,
                    "time": booking.appointment_time,
                    "duration_slots": booking.duration_minutes // 30
                })

            # Очистить старые слоты
            for i, booking in enumerate(bookings_to_change):
                try:
                    await self.sheets_service.clear_booking_slot_async(
                        booking.specialist_name,
                        booking.appointment_date,
                        booking.appointment_time,
                        booking.duration_minutes // 30
                    )
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Failed to clear old slot: {e}")

            # Обновить записи для новых мастеров
            for i, booking in enumerate(bookings_to_change):
                new_specialist = response.specialists_list[i]

                booking.specialist_name = new_specialist
                booking.appointment_date = new_date
                booking.appointment_time = new_time
                booking.client_name = response.name or booking.client_name
                booking.service_name = response.procedure or booking.service_name
                booking.client_phone = response.phone or booking.client_phone
                booking.updated_at = datetime.utcnow()

            self.db.commit()

            # Обновить Google Sheets для ОБОИХ новых мастеров
            for booking in bookings_to_change:
                await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)

            # Логировать перенос
            for i, booking in enumerate(bookings_to_change):
                try:
                    transfer_data = {
                        "old_date": old_data[i]["date"].strftime("%d.%m"),
                        "old_full_date": old_data[i]["date"].strftime("%d.%m.%Y"),
                        "old_time": str(old_data[i]["time"]),
                        "new_date": new_date.strftime("%d.%m"),
                        "new_time": str(new_time),
                        "client_id": client_id,
                        "client_name": booking.client_name or "Клиент",
                        "service": f"{booking.service_name} (двойная запись)",
                        "old_specialist": old_data[i]["specialist"],
                        "new_specialist": booking.specialist_name
                    }
                    await self.sheets_service.log_transfer(transfer_data)
                except Exception as log_error:
                    logger.error(f"Message ID: {message_id} - Failed to log transfer: {log_error}")

            return {
                "success": True,
                "message": f"Двойная запись перенесена: {specialist1} + {specialist2}",
                "booking_ids": [b.id for b in bookings_to_change]
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error changing double booking: {e}")
            return {
                "success": False,
                "message": f"Ошибка при переносе двойной записи: {str(e)}"
            }
    
    def _is_slot_available(self, specialist: str, booking_date: date, booking_time: time, duration_slots: int, exclude_booking_id: Optional[int] = None) -> bool:
        """Check if a time slot is available for booking"""
        # Generate all time slots that would be occupied
        occupied_slots = []
        for i in range(duration_slots):
            slot_datetime = datetime.combine(booking_date, booking_time) + timedelta(minutes=30*i)
            occupied_slots.append(slot_datetime.time())
        
        # Check for conflicts
        query = self.db.query(Booking).filter(
            and_(
                Booking.project_id == self.project_config.project_id,
                Booking.specialist_name == specialist,
                Booking.appointment_date == booking_date,
                Booking.status == "active"
            )
        )
        
        if exclude_booking_id:
            query = query.filter(Booking.id != exclude_booking_id)
        
        existing_bookings = query.all()
        
        for booking in existing_bookings:
            # Check if any of the required slots conflict with existing bookings
            booking_duration_slots = booking.duration_minutes // 30
            for i in range(booking_duration_slots):
                existing_slot = datetime.combine(booking_date, booking.appointment_time) + timedelta(minutes=30*i)
                if existing_slot.time() in occupied_slots:
                    return False
        
        return True
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string in various formats with detailed logging"""
        if not date_str:
            logger.warning("Empty date string provided")
            return None
            
        logger.info(f"PARSING DATE: Input string: '{date_str}'")
        
        try:
            # Clean the input string
            date_str = str(date_str).strip()
            logger.info(f"PARSING DATE: Cleaned string: '{date_str}'")
            
            # Try DD.MM.YYYY format
            if len(date_str.split('.')) == 3:
                result = datetime.strptime(date_str, "%d.%m.%Y").date()
                logger.info(f"PARSING DATE: Successfully parsed DD.MM.YYYY format: {result}")
                return result
            # Try DD.MM format (assume current year)
            elif len(date_str.split('.')) == 2:
                current_year = datetime.now().year
                full_date_str = f"{date_str}.{current_year}"
                result = datetime.strptime(full_date_str, "%d.%m.%Y").date()
                logger.info(f"PARSING DATE: Successfully parsed DD.MM format with current year: {result}")
                return result
            # Try other formats
            else:
                # Try YYYY-MM-DD
                try:
                    result = datetime.strptime(date_str, "%Y-%m-%d").date()
                    logger.info(f"PARSING DATE: Successfully parsed YYYY-MM-DD format: {result}")
                    return result
                except ValueError:
                    pass
                
                # Try DD/MM/YYYY
                try:
                    result = datetime.strptime(date_str, "%d/%m/%Y").date()
                    logger.info(f"PARSING DATE: Successfully parsed DD/MM/YYYY format: {result}")
                    return result
                except ValueError:
                    pass
                    
                logger.error(f"PARSING DATE: Unrecognized date format: '{date_str}'")
                return None
        except Exception as e:
            logger.error(f"PARSING DATE: Error parsing '{date_str}': {e}")
            return None
    
    def _parse_time(self, time_str: str) -> Optional[time]:
        """Parse time string in HH:MM format with detailed logging"""
        if not time_str:
            logger.warning("Empty time string provided")
            return None
            
        logger.info(f"PARSING TIME: Input string: '{time_str}'")
        
        try:
            # Clean the input string
            time_str = str(time_str).strip()
            logger.info(f"PARSING TIME: Cleaned string: '{time_str}'")
            
            # Try HH:MM format
            result = datetime.strptime(time_str, "%H:%M").time()
            logger.info(f"PARSING TIME: Successfully parsed HH:MM format: {result}")
            return result
        except ValueError as e:
            # Try alternative formats
            try:
                # Try H:MM format (single digit hour)
                result = datetime.strptime(time_str, "%H:%M").time()
                logger.info(f"PARSING TIME: Successfully parsed H:MM format: {result}")
                return result
            except ValueError:
                pass
                
            try:
                # Try HH.MM format
                time_str_fixed = time_str.replace('.', ':')
                result = datetime.strptime(time_str_fixed, "%H:%M").time()
                logger.info(f"PARSING TIME: Successfully parsed HH.MM format: {result}")
                return result
            except ValueError:
                pass
                
            logger.error(f"PARSING TIME: Unable to parse time '{time_str}': {e}")
            return None
        except Exception as e:
            logger.error(f"PARSING TIME: Unexpected error parsing '{time_str}': {e}")
            return None
    
    def get_client_bookings(self, client_id: str) -> List[BookingRecord]:
        """Get all bookings for a client"""
        bookings = self.db.query(Booking).filter(
            and_(
                Booking.project_id == self.project_config.project_id,
                Booking.client_id == client_id,
                Booking.status == "active"
            )
        ).all()
        
        return [
            BookingRecord(
                id=booking.id,
                project_id=booking.project_id,
                specialist_name=booking.specialist_name,
                date=booking.appointment_date,
                time=booking.appointment_time,
                client_id=booking.client_id,
                client_name=booking.client_name,
                service_name=booking.service_name,
                phone=booking.client_phone,
                duration_slots=booking.duration_minutes // 30,
                status=booking.status,
                created_at=booking.created_at,
                updated_at=booking.updated_at
            )
            for booking in bookings
        ]
    
    def get_client_bookings_as_string(self, client_id: str) -> str:
        """Get client bookings formatted as string for Claude"""
        bookings = self.get_client_bookings(client_id)
        
        if not bookings:
            return "У клиента нет активных записей"
        
        booking_strings = []
        for booking in bookings:
            booking_str = f"{booking.specialist_name} - {booking.date.strftime('%d.%m.%Y')} {booking.time.strftime('%H:%M')}"
            if booking.service_name:
                booking_str += f" ({booking.service_name})"
            booking_strings.append(booking_str)
        
        return "\n".join(booking_strings)
    
    async def _save_feedback(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> None:
        """Save client feedback to database and Google Sheets"""
        try:
            logger.debug(f"Message ID: {message_id} - Creating feedback record for client_id={client_id}")
            
            # Save to database
            feedback = Feedback(
                project_id=self.project_config.project_id,
                client_id=client_id,
                comment=response.feedback
            )
            
            self.db.add(feedback)
            self.db.commit()
            logger.info(f"Message ID: {message_id} - Feedback saved to database for client_id={client_id}")
            
            # Save to Google Sheets "Хран" sheet
            try:
                # Get client information from response or existing bookings
                client_name = response.name or ""
                client_phone = response.phone or ""
                
                # If no name/phone in response, try to get from recent bookings
                if not client_name or not client_phone:
                    recent_bookings = self.db.query(Booking).filter(
                        and_(
                            Booking.project_id == self.project_config.project_id,
                            Booking.client_id == client_id
                        )
                    ).order_by(desc(Booking.created_at)).limit(1).all()
                    
                    if recent_bookings:
                        recent_booking = recent_bookings[0]
                        if not client_name and recent_booking.client_name:
                            client_name = recent_booking.client_name
                        if not client_phone and recent_booking.client_phone:
                            client_phone = recent_booking.client_phone
                
                logger.debug(f"Message ID: {message_id} - Saving feedback to 'Хран' sheet with name='{client_name}', phone='{client_phone}'")
                sheets_success = await self.sheets_service.save_feedback_to_sheets_async(
                    client_id=client_id,
                    client_name=client_name,
                    client_phone=client_phone,
                    feedback_text=response.feedback
                )
                
                if sheets_success:
                    logger.info(f"Message ID: {message_id} - Feedback saved to Google Sheets successfully for client_id={client_id}")
                else:
                    logger.warning(f"Message ID: {message_id} - Failed to save feedback to Google Sheets for client_id={client_id}")
                    
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - Error saving feedback to Google Sheets for client_id={client_id}: {sheets_error}")
                # Don't fail the entire feedback save if sheets fails
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error saving feedback for client_id={client_id}: {e}")
    
    def get_booking_stats(self) -> Dict[str, Any]:
        """Get booking statistics for the project"""
        total_bookings = self.db.query(Booking).filter(
            Booking.project_id == self.project_config.project_id
        ).count()
        
        active_bookings = self.db.query(Booking).filter(
            and_(
                Booking.project_id == self.project_config.project_id,
                Booking.status == "active"
            )
        ).count()
        
        cancelled_bookings = self.db.query(Booking).filter(
            and_(
                Booking.project_id == self.project_config.project_id,
                Booking.status == "cancelled"
            )
        ).count()
        
        return {
            "total_bookings": total_bookings,
            "active_bookings": active_bookings,
            "cancelled_bookings": cancelled_bookings,
            "specialists": self.project_config.specialists,
            "services": self.project_config.services
        }

    async def _activate_double_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str,
                                       contact_send_id: str = None) -> Dict[str, Any]:
        """ИСПРАВЛЕННАЯ активация двойной записи к двум мастерам"""
        logger.info(f"Message ID: {message_id} - 🔧 IMPROVED: Activating DOUBLE booking for client_id={client_id}")
        logger.info(f"Message ID: {message_id} - Double booking fields: specialists={response.specialists_list}, date={response.date_order}, time={response.time_set_up}")

        # ИСПРАВЛЕНИЕ: Детальная проверка полей
        if not response.specialists_list or len(response.specialists_list) < 2:
            logger.warning(f"Message ID: {message_id} - Invalid specialists_list for double booking: {response.specialists_list}")
            return {"success": False, "message": "Недостаточно специалистов для двойной записи"}
        
        if not response.date_order or not response.time_set_up:
            logger.warning(f"Message ID: {message_id} - Missing date/time for double booking: date={response.date_order}, time={response.time_set_up}")
            return {"success": False, "message": "Недостаточно данных для двойной записи"}

        specialist1, specialist2 = response.specialists_list[0], response.specialists_list[1]
        logger.info(f"Message ID: {message_id} - Double booking specialists: {specialist1} + {specialist2}")
        
        # ИСПРАВЛЕНИЕ: Используем улучшенную валидацию даты и времени
        booking_date = self._parse_date(response.date_order)
        booking_time = self._parse_time(response.time_set_up)
        
        if not booking_date or not booking_time:
            logger.warning(f"Message ID: {message_id} - Invalid date/time format: date={response.date_order}, time={response.time_set_up}")
            return {
                "success": False,
                "message": "Неверный формат даты или времени"
            }
        
        # Проверить что оба специалиста существуют
        for specialist in [specialist1, specialist2]:
            if specialist not in self.project_config.specialists:
                logger.warning(f"Message ID: {message_id} - Unknown specialist: {specialist}, available: {self.project_config.specialists}")
                return {
                    "success": False,
                    "message": f"Специалист {specialist} не найден"
                }
        
        logger.info(f"Message ID: {message_id} - 🔧 STARTING ATOMIC DOUBLE BOOKING CHECK")
        logger.info(f"Message ID: {message_id} - Checking {specialist1} and {specialist2} for {booking_date} {booking_time}")
        
        try:
            # Получаем текущее состояние слотов для ОБОИХ мастеров
            duration_slots = 2  # Стандартная длительность для двойной записи
            current_slots = await self.sheets_service.get_available_slots_async(self.db, booking_date, duration_slots)
            
            requested_time = booking_time.strftime("%H:%M")
            occupied_specialists = []
            
            logger.info(f"Message ID: {message_id} - DOUBLE BOOKING CHECK: All available keys: {list(current_slots.slots_by_specialist.keys())}")
            logger.info(f"Message ID: {message_id} - DOUBLE BOOKING CHECK: All reserved keys: {list(current_slots.reserved_slots_by_specialist.keys())}")
            
            # Проверяем каждого мастера
            for specialist in [specialist1, specialist2]:
                specialist_key = f'available_slots_{specialist.lower()}'
                reserved_key = f'reserved_slots_{specialist.lower()}'
                
                available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                
                # ИСПРАВЛЕНИЕ: Если нет слотов для специалиста, попробуем найти альтернативный ключ
                if not available_slots and not reserved_slots:
                    for key in current_slots.slots_by_specialist.keys():
                        if specialist.lower() in key.lower():
                            logger.info(f"Message ID: {message_id} - Found alternative key for {specialist}: {key}")
                            specialist_key = key
                            reserved_key = key.replace('available_slots_', 'reserved_slots_')
                            available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                            reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                            break
                
                logger.info(f"Message ID: {message_id} - {specialist} available: {available_slots}")
                logger.info(f"Message ID: {message_id} - {specialist} reserved: {reserved_slots}")
                
                # ИСПРАВЛЕНИЕ: Упрощенная логика - проверяем только reserved_slots
                if requested_time in reserved_slots:
                    occupied_specialists.append(specialist)
                    logger.warning(f"Message ID: {message_id} - ❌ {specialist} NOT available at {requested_time} (slot is reserved)")
                else:
                    logger.info(f"Message ID: {message_id} - ✅ {specialist} available at {requested_time}")
            
            if occupied_specialists:
                logger.error(f"Message ID: {message_id} - 🚨 DOUBLE BOOKING BLOCKED: Specialists unavailable: {occupied_specialists}")
                return {
                    "success": False,
                    "message": f"Мастер(а) {', '.join(occupied_specialists)} заняты на это время"
                }
            
            logger.info(f"Message ID: {message_id} - ✅ BOTH SPECIALISTS AVAILABLE - proceeding with double booking")
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in double booking slot check: {e}", exc_info=True)
            return {
                "success": False,
                "message": "Ошибка проверки доступности"
            }

        # ИСПРАВЛЕНИЕ: Создать ДВЕ записи в БД с улучшенным логированием
        logger.info(f"Message ID: {message_id} - Creating DOUBLE booking records in database")
        bookings = []
        
        try:
            for i, specialist in enumerate([specialist1, specialist2]):
                logger.info(f"Message ID: {message_id} - Creating booking {i+1}/2 for {specialist}")
                booking = Booking(
                    project_id=self.project_config.project_id,
                    specialist_name=specialist,
                    appointment_date=booking_date,
                    appointment_time=booking_time,
                    client_id=client_id,
                    client_name=response.name,
                    service_name=response.procedure,
                    client_phone=response.phone,
                    duration_minutes=60,  # Стандартная длительность
                    status="active"
                )
                self.db.add(booking)
                bookings.append(booking)
            
            self.db.commit()
            
            # Обновляем ID после commit
            for booking in bookings:
                self.db.refresh(booking)
                logger.info(f"Message ID: {message_id} - ✅ Booking created: ID={booking.id}, specialist={booking.specialist_name}")
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error creating double booking records: {e}")
            self.db.rollback()
            return {
                "success": False,
                "message": f"Ошибка при создании записи: {str(e)}"
            }

        # ИСПРАВЛЕНИЕ: Обновить Google Sheets для ОБОИХ мастеров с обработкой ошибок
        logger.info(f"Message ID: {message_id} - Updating Google Sheets for both specialists")
        sheets_success_count = 0
        
        for i, booking in enumerate(bookings):
            try:
                logger.debug(f"Message ID: {message_id} - Updating Google Sheets for {booking.specialist_name} (booking {i+1}/2)")
                sheets_success = await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                if sheets_success:
                    sheets_success_count += 1
                    logger.info(f"Message ID: {message_id} - ✅ Google Sheets updated for {booking.specialist_name}")
                else:
                    logger.warning(f"Message ID: {message_id} - ⚠️ Google Sheets update failed for {booking.specialist_name}")
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - ❌ Failed to update Google Sheets for {booking.specialist_name}: {sheets_error}")
        
        logger.info(f"Message ID: {message_id} - Google Sheets updates completed: {sheets_success_count}/2 successful")
        
        # ИСПРАВЛЕНИЕ: Добавить в Make.com таблицу с обработкой ошибок
        try:
            make_booking_data = {
                'date': booking_date.strftime("%d.%m.%Y"),
                'client_id': contact_send_id if contact_send_id else client_id,
                'messenger_client_id': client_id,  # Добавляем Messenger ID
                'time': booking_time.strftime('%H:%M'),
                'client_name': response.name or "Клиент",
                'service': f"{response.procedure or 'Услуга'} (двойная запись)",
                'specialist': f"{specialist1} + {specialist2}"
            }
            logger.info(f"Message ID: {message_id} - Adding double booking to Make.com table")
            await self.sheets_service.add_booking_to_make_table_async(make_booking_data)
            logger.info(f"Message ID: {message_id} - ✅ Double booking added to Make.com table for 24h reminders")
        except Exception as make_error:
            logger.error(f"Message ID: {message_id} - ❌ Failed to add double booking to Make.com table: {make_error}")
            # Не прерываем процесс если Make.com не удалось

        return {
            "success": True,
            "message": f"Двойная запись создана: {specialist1} + {specialist2}",
            "booking_ids": [b.id for b in bookings]
        }
