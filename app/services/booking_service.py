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
                logger.info(f"Message ID: {message_id} - Available slots: {current_slots.slots_by_specialist.get(specialist_key, [])}")
                logger.info(f"Message ID: {message_id} - Reserved slots: {current_slots.reserved_slots_by_specialist.get(reserved_key, [])}")
                
                # 2. Проверяем конкретное время
                requested_time = booking_time.strftime("%H:%M")
                
                # Проверяем все слоты, которые займет эта запись
                slots_to_check = []
                for i in range(duration_slots):
                    check_time = (datetime.combine(booking_date, booking_time) + timedelta(minutes=30*i)).time()
                    slots_to_check.append(check_time.strftime("%H:%M"))
                
                logger.info(f"Message ID: {message_id} - Need to check {len(slots_to_check)} slots: {slots_to_check}")
                
                # 3. Проверка доступности ВСЕХ нужных слотов
                available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                
                unavailable_slots = []
                for slot_time in slots_to_check:
                    if slot_time not in available_slots:
                        unavailable_slots.append(slot_time)
                        logger.warning(f"Message ID: {message_id} - Slot {slot_time} NOT in available slots")
                    if slot_time in reserved_slots:
                        unavailable_slots.append(slot_time)
                        logger.warning(f"Message ID: {message_id} - Slot {slot_time} IS in reserved slots")
                
                if unavailable_slots:
                    logger.error(f"Message ID: {message_id} - 🚨 BOOKING BLOCKED: Unavailable slots found: {unavailable_slots}")
                    return {
                        "success": False,
                        "message": f"Время {', '.join(unavailable_slots)} уже занято",
                        "record_error": f"Слоты недоступны: {', '.join(unavailable_slots)}"
                    }
                
                logger.info(f"Message ID: {message_id} - ✅ ALL SLOTS AVAILABLE - proceeding with booking")
                
            except Exception as e:
                logger.error(f"Message ID: {message_id} - Error in atomic slot check: {e}, aborting booking")
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

    async def _reject_single_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[str, Any]:
        """Отменить одинарную запись"""
        try:
            # Parse date and time
            booking_date = self._parse_date(response.date_reject)
            booking_time = self._parse_time(response.time_reject)

            if not booking_date or not booking_time:
                return {
                    "success": False,
                    "message": "Неверный формат даты или времени"
                }

            # Find the booking to cancel
            booking = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.specialist_name == response.cosmetolog,
                    Booking.appointment_date == booking_date,
                    Booking.appointment_time == booking_time,
                    Booking.status == "active"
                )
            ).first()

            if not booking:
                return {
                    "success": False,
                    "message": "Запись не найдена"
                }

            # Cancel booking
            booking.status = "cancelled"
            booking.updated_at = datetime.utcnow()
            self.db.commit()

            # Clear slot in Google Sheets
            try:
                duration_slots = booking.duration_minutes // 30
                await self.sheets_service.clear_booking_slot_async(
                    booking.specialist_name,
                    booking.appointment_date,
                    booking.appointment_time,
                    duration_slots
                )
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - Failed to clear booking slot: {sheets_error}")

            # Log cancellation
            try:
                cancellation_data = {
                    "date": booking.appointment_date.strftime("%d.%m"),
                    "full_date": booking.appointment_date.strftime("%d.%m.%Y"),
                    "time": str(booking.appointment_time),
                    "client_id": client_id,
                    "client_name": booking.client_name or "Клиент",
                    "service": booking.service_name or "Услуга",
                    "specialist": booking.specialist_name
                }
                await self.sheets_service.log_cancellation(cancellation_data)
            except Exception as log_error:
                logger.error(f"Message ID: {message_id} - Failed to log cancellation: {log_error}")

            return {
                "success": True,
                "message": f"Запись отменена: {booking.specialist_name}",
                "booking_id": booking.id
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error cancelling single booking: {e}")
            return {
                "success": False,
                "message": f"Ошибка при отмене записи: {str(e)}"
            }

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

    async def _change_single_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[str, Any]:
        """Перенести одинарную запись (или автоматически найти все записи на старую дату)"""
        try:
            # Parse dates and times
            old_date = self._parse_date(response.date_reject)
            new_date = self._parse_date(response.date_order)

            if not old_date or not new_date:
                return {
                    "success": False,
                    "message": "Неверный формат даты"
                }
                
            # ПОКРАЩЕНО: Проверяем, насколько записей у клиента на старую дату
            all_bookings_on_old_date = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.appointment_date == old_date,
                    Booking.status == "active"
                )
            ).all()
            
            logger.info(f"Message ID: {message_id} - Found {len(all_bookings_on_old_date)} active bookings for client on {old_date}")
            
            # ОПТИМИЗАЦИЯ: Если на старой дате несколько записей - переносим все
            if len(all_bookings_on_old_date) > 1:
                logger.info(f"Message ID: {message_id} - Multiple bookings found, transferring ALL {len(all_bookings_on_old_date)} bookings to {new_date}")
                return await self._transfer_multiple_bookings(all_bookings_on_old_date, new_date, response, client_id, message_id)
            
            # Одинарный перенос (как раньше)
            old_time = self._parse_time(response.time_reject)
            new_time = self._parse_time(response.time_set_up)

            if not old_time or not new_time:
                return {
                    "success": False,
                    "message": "Неверный формат времени"
                }

            # Find the specific booking to change
            booking = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.specialist_name == response.cosmetolog,
                    Booking.appointment_date == old_date,
                    Booking.appointment_time == old_time,
                    Booking.status == "active"
                )
            ).first()

            if not booking:
                return {
                    "success": False,
                    "message": "Запись не найдена"
                }

            # Check if new slot is available
            if not self._is_slot_available(response.cosmetolog, new_date, new_time, booking.duration_minutes // 30, booking.id):
                return {
                    "success": False,
                    "message": f"Новое время уже занято"
                }

            # Save old data for logging
            old_booking_data = {
                "specialist": booking.specialist_name,
                "date": booking.appointment_date,
                "time": booking.appointment_time,
                "duration_slots": booking.duration_minutes // 30
            }

            # Clear old slot
            try:
                await self.sheets_service.clear_booking_slot_async(
                    booking.specialist_name,
                    booking.appointment_date,
                    booking.appointment_time,
                    booking.duration_minutes // 30
                )
            except Exception as e:
                logger.error(f"Message ID: {message_id} - Failed to clear old slot: {e}")

            # Update booking
            booking.specialist_name = response.cosmetolog
            booking.appointment_date = new_date
            booking.appointment_time = new_time
            booking.client_name = response.name or booking.client_name
            booking.service_name = response.procedure or booking.service_name
            booking.client_phone = response.phone or booking.client_phone
            booking.updated_at = datetime.utcnow()

            self.db.commit()

            # Update Google Sheets
            await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)

            # Log the transfer
            try:
                transfer_data = {
                    "old_date": old_booking_data["date"].strftime("%d.%m"),
                    "old_full_date": old_booking_data["date"].strftime("%d.%m.%Y"),
                    "old_time": str(old_booking_data["time"]),
                    "new_date": new_date.strftime("%d.%m"),
                    "new_time": str(new_time),
                    "client_id": client_id,
                    "client_name": booking.client_name or "Клиент",
                    "service": booking.service_name or "Услуга",
                    "old_specialist": old_booking_data["specialist"],
                    "new_specialist": booking.specialist_name
                }
                await self.sheets_service.log_transfer(transfer_data)
            except Exception as log_error:
                logger.error(f"Message ID: {message_id} - Failed to log transfer: {log_error}")

            return {
                "success": True,
                "message": f"Запись перенесена: {booking.specialist_name}",
                "booking_id": booking.id
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error changing single booking: {e}")
            return {
                "success": False,
                "message": f"Ошибка при переносе записи: {str(e)}"
            }
    
    async def _transfer_multiple_bookings(self, bookings: List[Booking], new_date: date, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[str, Any]:
        """
        НОВЫЙ МЕТОД: Переносит несколько записей на новую дату, сохраняя то же время для каждого
        """
        try:
            logger.info(f"Message ID: {message_id} - Transferring {len(bookings)} bookings from {bookings[0].appointment_date} to {new_date}")
            
            transferred_bookings = []
            failed_transfers = []
            
            for booking in bookings:
                try:
                    # Проверяем доступность нового слота
                    if not self._is_slot_available(booking.specialist_name, new_date, booking.appointment_time, booking.duration_minutes // 30, booking.id):
                        failed_transfers.append(f"{booking.specialist_name} ({booking.appointment_time}) - время занято")
                        logger.warning(f"Message ID: {message_id} - Cannot transfer {booking.specialist_name} at {booking.appointment_time} - slot occupied")
                        continue
                    
                    # Сохраняем старые данные для логирования
                    old_booking_data = {
                        "specialist": booking.specialist_name,
                        "date": booking.appointment_date,
                        "time": booking.appointment_time,
                        "duration_slots": booking.duration_minutes // 30
                    }
                    
                    # Очищаем старый слот
                    try:
                        await self.sheets_service.clear_booking_slot_async(
                            booking.specialist_name,
                            booking.appointment_date,
                            booking.appointment_time,
                            booking.duration_minutes // 30
                        )
                        logger.info(f"Message ID: {message_id} - Cleared old slot for {booking.specialist_name}")
                    except Exception as e:
                        logger.error(f"Message ID: {message_id} - Failed to clear old slot for {booking.specialist_name}: {e}")
                    
                    # Обновляем запись
                    booking.appointment_date = new_date
                    booking.client_name = response.name or booking.client_name
                    booking.client_phone = response.phone or booking.client_phone
                    booking.updated_at = datetime.utcnow()
                    
                    transferred_bookings.append(booking)
                    logger.info(f"Message ID: {message_id} - Updated booking for {booking.specialist_name} to {new_date} {booking.appointment_time}")
                    
                except Exception as booking_error:
                    failed_transfers.append(f"{booking.specialist_name} - ошибка: {str(booking_error)}")
                    logger.error(f"Message ID: {message_id} - Failed to transfer {booking.specialist_name}: {booking_error}")
            
            # Проверяем, что хотя бы одна запись была перенесена
            if not transferred_bookings:
                return {
                    "success": False,
                    "message": f"Ни одна запись не была перенесена: {'; '.join(failed_transfers)}"
                }
            
            # Сохраняем изменения в базе
            self.db.commit()
            logger.info(f"Message ID: {message_id} - Committed {len(transferred_bookings)} booking transfers to database")
            
            # Обновляем Google Sheets для каждой перенесенной записи
            sheets_success_count = 0
            for booking in transferred_bookings:
                try:
                    await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                    sheets_success_count += 1
                    logger.info(f"Message ID: {message_id} - Updated Google Sheets for {booking.specialist_name}")
                except Exception as sheets_error:
                    logger.error(f"Message ID: {message_id} - Failed to update sheets for {booking.specialist_name}: {sheets_error}")
            
            logger.info(f"Message ID: {message_id} - Updated Google Sheets: {sheets_success_count}/{len(transferred_bookings)} successful")
            
            # Логируем переносы
            for i, booking in enumerate(transferred_bookings):
                try:
                    # Получаем старые данные из первоначального списка
                    old_booking = bookings[i] if i < len(bookings) else bookings[0]
                    transfer_data = {
                        "old_date": old_booking.appointment_date.strftime("%d.%m") if old_booking else "?",
                        "old_full_date": old_booking.appointment_date.strftime("%d.%m.%Y") if old_booking else "?",
                        "old_time": str(booking.appointment_time),
                        "new_date": new_date.strftime("%d.%m"),
                        "new_time": str(booking.appointment_time),
                        "client_id": client_id,
                        "client_name": booking.client_name or "Клиент",
                        "service": booking.service_name or "Услуга",
                        "old_specialist": booking.specialist_name,
                        "new_specialist": booking.specialist_name  # Мастер остается тот же
                    }
                    await self.sheets_service.log_transfer(transfer_data)
                except Exception as log_error:
                    logger.error(f"Message ID: {message_id} - Failed to log transfer for {booking.specialist_name}: {log_error}")
            
            # Подготавливаем сообщение о результате
            success_names = [f"{b.specialist_name} ({b.appointment_time})" for b in transferred_bookings]
            success_message = f"Перенесено {len(transferred_bookings)} записи: {', '.join(success_names)}"
            
            if failed_transfers:
                success_message += f". Не удалось: {'; '.join(failed_transfers)}"
            
            return {
                "success": True,
                "message": success_message,
                "booking_ids": [b.id for b in transferred_bookings],
                "transferred_count": len(transferred_bookings),
                "failed_count": len(failed_transfers)
            }
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in _transfer_multiple_bookings: {e}")
            self.db.rollback()  # Откатываем все изменения при ошибке
            return {
                "success": False,
                "message": f"Ошибка при переносе нескольких записей: {str(e)}"
            }

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
        """Parse date string in various formats"""
        try:
            # Try DD.MM.YYYY format
            if len(date_str.split('.')) == 3:
                return datetime.strptime(date_str, "%d.%m.%Y").date()
            # Try DD.MM format (assume current year)
            elif len(date_str.split('.')) == 2:
                current_year = datetime.now().year
                return datetime.strptime(f"{date_str}.{current_year}", "%d.%m.%Y").date()
            return None
        except Exception:
            return None
    
    def _parse_time(self, time_str: str) -> Optional[time]:
        """Parse time string in HH:MM format"""
        try:
            return datetime.strptime(time_str, "%H:%M").time()
        except Exception:
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
        """ИСПРАВЛЕННАЯ активация двойной записи с поддержкой разного времени для каждого мастера"""
        logger.info(f"Message ID: {message_id} - 🔧 FIXED: Activating DOUBLE booking for client_id={client_id}")
        logger.info(f"Message ID: {message_id} - Double booking fields: specialists={response.specialists_list}, date={response.date_order}, time={response.time_set_up}")

        # Проверка полей
        if not response.specialists_list or len(response.specialists_list) < 2:
            logger.warning(f"Message ID: {message_id} - Invalid specialists_list for double booking: {response.specialists_list}")
            return {"success": False, "message": "Недостаточно специалистов для двойной записи"}
        
        if not response.date_order or not response.time_set_up:
            logger.warning(f"Message ID: {message_id} - Missing date/time for double booking: date={response.date_order}, time={response.time_set_up}")
            return {"success": False, "message": "Недостаточно данных для двойной записи"}

        # ИСПРАВЛЕНИЕ: Парсим информацию о времени И процедурах для каждого мастера из gpt_response
        specialist_times = self._parse_specialist_times_from_response(response.gpt_response, response.specialists_list, message_id)
        specialist_procedures = self._parse_specialist_procedures_from_response(response.gpt_response, response.specialists_list, message_id)
        
        logger.info(f"Message ID: {message_id} - Parsed specialist times: {specialist_times}")
        logger.info(f"Message ID: {message_id} - Parsed specialist procedures: {specialist_procedures}")
        logger.info(f"Message ID: {message_id} - Original response.procedure: '{response.procedure}'")
        logger.info(f"Message ID: {message_id} - Original gpt_response: '{response.gpt_response[:500]}...'")
        
        # Проверим что у нас есть время для каждого мастера
        if len(specialist_times) != len(response.specialists_list):
            logger.warning(f"Message ID: {message_id} - Could not parse times for all specialists. Using fallback method.")
            # Фоллбэк: используем базовое время для первого мастера, +3 часа для второго
            base_time = self._parse_time(response.time_set_up)
            if not base_time:
                return {"success": False, "message": "Неверный формат времени"}
            
            from datetime import datetime, timedelta
            second_time = (datetime.combine(datetime.today(), base_time) + timedelta(hours=3)).time()
            specialist_times = {
                response.specialists_list[0]: base_time,
                response.specialists_list[1]: second_time
            }
            logger.info(f"Message ID: {message_id} - Fallback times assigned: {specialist_times}")

        booking_date = self._parse_date(response.date_order)
        if not booking_date:
            logger.warning(f"Message ID: {message_id} - Invalid date format: {response.date_order}")
            return {"success": False, "message": "Неверный формат даты"}
        
        # Проверить что оба специалиста существуют
        for specialist in response.specialists_list:
            if specialist not in self.project_config.specialists:
                logger.warning(f"Message ID: {message_id} - Unknown specialist: {specialist}")
                return {"success": False, "message": f"Специалист {specialist} не найден"}
        
        logger.info(f"Message ID: {message_id} - 🔧 STARTING IMPROVED DOUBLE BOOKING CHECK")
        
        try:
            # Проверяем доступность каждого мастера в его конкретное время
            duration_slots = 2  # Стандартная длительность
            occupied_specialists = []
            
            for specialist, booking_time in specialist_times.items():
                # Проверяем доступность этого мастера в его время
                current_slots = await self.sheets_service.get_available_slots_async(self.db, booking_date, duration_slots)
                specialist_key = f'available_slots_{specialist.lower()}'
                reserved_key = f'reserved_slots_{specialist.lower()}'
                
                available_slots = current_slots.slots_by_specialist.get(specialist_key, [])
                reserved_slots = current_slots.reserved_slots_by_specialist.get(reserved_key, [])
                
                requested_time = booking_time.strftime("%H:%M")
                
                logger.info(f"Message ID: {message_id} - Checking {specialist} at {requested_time}")
                logger.info(f"Message ID: {message_id} - {specialist} available: {available_slots}")
                logger.info(f"Message ID: {message_id} - {specialist} reserved: {reserved_slots}")
                
                if requested_time not in available_slots or requested_time in reserved_slots:
                    occupied_specialists.append(f"{specialist} ({requested_time})")
                    logger.warning(f"Message ID: {message_id} - {specialist} NOT available at {requested_time}")
            
            if occupied_specialists:
                logger.error(f"Message ID: {message_id} - 🚨 DOUBLE BOOKING BLOCKED: {occupied_specialists}")
                return {
                    "success": False,
                    "message": f"Время занято у: {', '.join(occupied_specialists)}"
                }
            
            logger.info(f"Message ID: {message_id} - ✅ ALL SPECIALISTS AVAILABLE at their respective times")
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in double booking slot check: {e}")
            return {"success": False, "message": "Ошибка проверки доступности"}

        # ИСПРАВЛЕНИЕ: Создать записи с РАЗНЫМ временем И процедурами для каждого мастера
        logger.info(f"Message ID: {message_id} - Creating DOUBLE booking records with different times and procedures")
        bookings = []
        
        try:
            for i, (specialist, booking_time) in enumerate(specialist_times.items()):
                # ИСПРАВЛЕНО: Получаем процедуру для этого конкретного мастера БЕЗ fallback на общую процедуру
                if specialist in specialist_procedures:
                    specialist_procedure = specialist_procedures[specialist]
                    logger.info(f"Message ID: {message_id} - ✅ Found parsed procedure for {specialist}: {specialist_procedure}")
                else:
                    # ИСПРАВЛЕНИЕ: Если не удалось распарсить - пробуем разделить общую процедуру пополам
                    if response.procedure and ('+' in response.procedure or 'обертывание' in response.procedure.lower() or 'массаж' in response.procedure.lower()):
                        # Попытка разделить процедуры
                        procedures_parts = self._split_combined_procedure(response.procedure, response.specialists_list, message_id)
                        if len(procedures_parts) >= len(response.specialists_list):
                            specialist_procedure = procedures_parts[i] if i < len(procedures_parts) else procedures_parts[0]
                            logger.info(f"Message ID: {message_id} - 🔄 Using split procedure for {specialist}: {specialist_procedure}")
                        else:
                            specialist_procedure = f"Процедура {i+1}"  # Fallback
                            logger.warning(f"Message ID: {message_id} - ⚠️ Using fallback procedure for {specialist}: {specialist_procedure}")
                    else:
                        specialist_procedure = f"Процедура для {specialist}"  # Fallback
                        logger.warning(f"Message ID: {message_id} - ⚠️ Using generic fallback for {specialist}: {specialist_procedure}")
                
                logger.info(f"Message ID: {message_id} - Creating booking {i+1}/{len(specialist_times)} for {specialist} at {booking_time} for procedure: {specialist_procedure}")
                
                booking = Booking(
                    project_id=self.project_config.project_id,
                    specialist_name=specialist,
                    appointment_date=booking_date,
                    appointment_time=booking_time,  # ИСПРАВЛЕНО: используем разное время для каждого
                    client_id=client_id,
                    client_name=response.name,
                    service_name=specialist_procedure,  # ИСПРАВЛЕНО: используем правильную процедуру для каждого мастера
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
                logger.info(f"Message ID: {message_id} - ✅ Booking created: ID={booking.id}, specialist={booking.specialist_name}, time={booking.appointment_time}")
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error creating double booking records: {e}")
            self.db.rollback()
            return {"success": False, "message": f"Ошибка при создании записи: {str(e)}"}

        # Обновить Google Sheets для КАЖДОГО мастера в его время
        logger.info(f"Message ID: {message_id} - Updating Google Sheets for each specialist")
        sheets_success_count = 0
        
        for i, booking in enumerate(bookings):
            try:
                logger.debug(f"Message ID: {message_id} - Updating sheets for {booking.specialist_name} at {booking.appointment_time}")
                sheets_success = await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                if sheets_success:
                    sheets_success_count += 1
                    logger.info(f"Message ID: {message_id} - ✅ Google Sheets updated for {booking.specialist_name}")
                else:
                    logger.warning(f"Message ID: {message_id} - ⚠️ Google Sheets update failed for {booking.specialist_name}")
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - ❌ Failed to update sheets for {booking.specialist_name}: {sheets_error}")
        
        logger.info(f"Message ID: {message_id} - Google Sheets updates completed: {sheets_success_count}/{len(bookings)} successful")
        
        # Добавить в Make.com таблицу
        try:
            specialist_names = " + ".join([booking.specialist_name for booking in bookings])
            times_info = ", ".join([f"{b.specialist_name} {b.appointment_time}" for b in bookings])
            services_info = ", ".join([f"{b.specialist_name}: {b.service_name}" for b in bookings])
            
            make_booking_data = {
                'date': booking_date.strftime("%d.%m.%Y"),
                'client_id': contact_send_id if contact_send_id else client_id,
                'messenger_client_id': client_id,
                'time': response.time_set_up,  # Базовое время для сортировки
                'client_name': response.name or "Клиент",
                'service': f"Двойная запись: {services_info} (время: {times_info})",
                'specialist': specialist_names
            }
            logger.info(f"Message ID: {message_id} - Adding double booking to Make.com table")
            await self.sheets_service.add_booking_to_make_table_async(make_booking_data)
            logger.info(f"Message ID: {message_id} - ✅ Double booking added to Make.com table")
        except Exception as make_error:
            logger.error(f"Message ID: {message_id} - ❌ Failed to add to Make.com table: {make_error}")

        specialist_names = " + ".join([booking.specialist_name for booking in bookings])
        services_summary = ", ".join([f"{b.specialist_name} ({b.service_name})" for b in bookings])
        
        return {
            "success": True,
            "message": f"Двойная запись создана: {services_summary}",
            "booking_ids": [b.id for b in bookings]
        }

    def _parse_specialist_times_from_response(self, gpt_response: str, specialists_list: List[str], message_id: str) -> Dict[str, time]:
        """
        НОВЫЙ МЕТОД: Парсит время для каждого мастера из ответа Claude
        Ищет паттерны типа "11:00 к Ольге", "14:00 к Анне"
        """
        specialist_times = {}
        
        try:
            import re
            # Паттерн для поиска времени и имени мастера
            # Ищем: "время к/у имя", "время - имя", "имя время", "имя в время"
            time_patterns = [
                r'(\d{1,2}:\d{2})\s*(?:к|у|р|на|к|ж|д|к)\s*([А-ЯЁа-яё]+)',  # "11:00 к Ольге"
                r'([А-ЯЁа-яё]+).*?(\d{1,2}:\d{2})',  # "Ольге 11:00" или "к Ольге на 11:00"
                r'(\d{1,2}:\d{2})\s*[-–—]\s*([А-ЯЁа-яё]+)',  # "11:00 - Ольга"
                r'([А-ЯЁа-яё]+)\s*[-–—]\s*(\d{1,2}:\d{2})'   # "Ольга - 11:00"
            ]
            
            logger.info(f"Message ID: {message_id} - Parsing times from response: {gpt_response[:200]}...")
            
            for pattern in time_patterns:
                matches = re.findall(pattern, gpt_response, re.IGNORECASE)
                logger.debug(f"Message ID: {message_id} - Pattern '{pattern}' found matches: {matches}")
                
                for match in matches:
                    if len(match) == 2:
                        # Определяем что время, а что имя
                        if re.match(r'\d{1,2}:\d{2}', match[0]):
                            time_str, name = match[0], match[1]
                        else:
                            name, time_str = match[0], match[1]
                        
                        # Найти подходящего специалиста из списка
                        matched_specialist = None
                        name_clean = name.lower().strip()
                        
                        for specialist in specialists_list:
                            if specialist.lower() in name_clean or name_clean in specialist.lower():
                                matched_specialist = specialist
                                break
                        
                        if matched_specialist:
                            try:
                                parsed_time = self._parse_time(time_str)
                                if parsed_time:
                                    specialist_times[matched_specialist] = parsed_time
                                    logger.info(f"Message ID: {message_id} - Matched: {matched_specialist} -> {time_str}")
                            except:
                                continue
            
            logger.info(f"Message ID: {message_id} - Final parsed times: {specialist_times}")
            return specialist_times
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error parsing specialist times: {e}")
            return {}
    
    def _split_combined_procedure(self, combined_procedure: str, specialists_list: List[str], message_id: str) -> List[str]:
        """
        НОВЫЙ МЕТОД: Разделяет объединенную процедуру на отдельные части для каждого мастера
        """
        try:
            import re
            
            logger.info(f"Message ID: {message_id} - Splitting combined procedure: '{combined_procedure}'")
            
            # Удаляем лишние символы и нормализуем
            clean_procedure = combined_procedure.strip().replace('  ', ' ')
            
            # Паттерны для разделения процедур
            split_patterns = [
                r'\s*\+\s*',  # "массаж + обертывание"
                r'\s*,\s*',   # "массаж, обертывание"
                r'\s*и\s*',   # "массаж и обертывание" 
                r'\s*;\s*'    # "массаж; обертывание"
            ]
            
            parts = [clean_procedure]  # Начинаем с полной строки
            
            # Пробуем разные паттерны разделения
            for pattern in split_patterns:
                new_parts = []
                for part in parts:
                    split_result = re.split(pattern, part)
                    if len(split_result) > 1:
                        new_parts.extend([p.strip() for p in split_result if p.strip()])
                    else:
                        new_parts.append(part)
                parts = new_parts
                
                # Если получили достаточно частей - останавливаемся
                if len(parts) >= len(specialists_list):
                    break
            
            # Очищаем части от лишних слов
            cleaned_parts = []
            for part in parts:
                # Убираем типичные вводные слова
                clean_part = part.replace('разовое посещение', '').replace('посещение', '').strip()
                if clean_part:
                    cleaned_parts.append(clean_part)
            
            logger.info(f"Message ID: {message_id} - Split result: {cleaned_parts}")
            
            # Если не получилось разделить достаточно - дублируем или создаем вариации
            while len(cleaned_parts) < len(specialists_list):
                if len(cleaned_parts) == 1:
                    # Если всего одна процедура - создаем вариации
                    base_procedure = cleaned_parts[0]
                    for i in range(len(specialists_list) - len(cleaned_parts)):
                        cleaned_parts.append(f"{base_procedure} (вариант {i+2})")
                else:
                    # Если несколько - дублируем последнюю
                    cleaned_parts.append(cleaned_parts[-1])
            
            return cleaned_parts[:len(specialists_list)]  # Возвращаем точно столько, сколько мастеров
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error splitting procedure: {e}")
            # Fallback: создаем простые названия
            return [f"Процедура {i+1}" for i in range(len(specialists_list))]

    def _parse_specialist_procedures_from_response(self, gpt_response: str, specialists_list: List[str], message_id: str) -> Dict[str, str]:
        """
        ПОКРАЩЕНИЙ МЕТОД: Парсит процедуры для каждого мастера из ответа Claude
        Ищет паттерны типа "к Ольге разовый массаж", "к Анне обертывание"
        """
        specialist_procedures = {}
        
        try:
            import re
            # Расширенные паттерны для поиска процедур и имен мастеров
            procedure_patterns = [
                # Основные паттерны
                r'(?:к|у)\s*([A-ЯЁа-яё]+).*?(разовый[\w\s]*массаж[\w\s]*|обертывание[\w\s]*|массаж[\w\s]*|фибро[\w\s-]*|стратосф[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*|Stratosph[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*)',  # "к Ольге разовый массаж"
                r'([A-ЯЁа-яё]+).*?[-–—]\s*(разовый[\w\s]*массаж[\w\s]*|обертывание[\w\s]*|массаж[\w\s]*|фибро[\w\s-]*|стратосф[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*|Stratosph[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*)',  # "Ольга - разовый массаж"
                
                # Паттерн для строк с буллетами типа "• 11:00 к Ольге — Разовое посещение массажа Stratosphère"
                r'\d{1,2}:\d{2}\s*к\s*([A-ЯЁа-яё]+).*?[—-]\s*([^\(\n]*?)(?:\s*\(|$)',
                
                # Дополнительные паттерны
                r'(разовый[\w\s]*массаж[\w\s]*|обертывание[\w\s]*|массаж[\w\s]*|фибро[\w\s-]*|стратосф[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*|Stratosph[\w\s\u00e8\u00e9\u00e0\u00e2\u00e8]*).*?(?:к|у|для)\s*([A-ЯЁа-яё]+)',  # "разовый массаж к Ольге"
            ]
            
            logger.info(f"Message ID: {message_id} - Parsing procedures from response: {gpt_response[:400]}...")
            
            for i, pattern in enumerate(procedure_patterns):
                matches = re.findall(pattern, gpt_response, re.IGNORECASE | re.MULTILINE)
                logger.debug(f"Message ID: {message_id} - Procedure pattern {i+1} found matches: {matches}")
                
                for match in matches:
                    if len(match) == 2:
                        name_candidate = match[0].strip()
                        procedure_candidate = match[1].strip()
                        
                        # Проверим что первое - это имя мастера
                        matched_specialist = None
                        name_clean = name_candidate.lower()
                        
                        for specialist in specialists_list:
                            if specialist.lower() in name_clean or name_clean in specialist.lower():
                                matched_specialist = specialist
                                # Очистим процедуру
                                clean_procedure = self._clean_procedure_name(procedure_candidate)
                                if clean_procedure and matched_specialist not in specialist_procedures:
                                    specialist_procedures[matched_specialist] = clean_procedure
                                    logger.info(f"Message ID: {message_id} - Matched procedure: {matched_specialist} -> {clean_procedure}")
                                break
                        
                        # Если первое не имя мастера, попробуем наоборот
                        if not matched_specialist:
                            name_candidate, procedure_candidate = match[1].strip(), match[0].strip()
                            name_clean = name_candidate.lower()
                            
                            for specialist in specialists_list:
                                if specialist.lower() in name_clean or name_clean in specialist.lower():
                                    matched_specialist = specialist
                                    clean_procedure = self._clean_procedure_name(procedure_candidate)
                                    if clean_procedure and matched_specialist not in specialist_procedures:
                                        specialist_procedures[matched_specialist] = clean_procedure
                                        logger.info(f"Message ID: {message_id} - Matched procedure (reversed): {matched_specialist} -> {clean_procedure}")
                                    break
            
            logger.info(f"Message ID: {message_id} - Final parsed procedures: {specialist_procedures}")
            return specialist_procedures
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error parsing specialist procedures: {e}")
            return {}
    
    def _clean_procedure_name(self, procedure_text: str) -> str:
        """
        Очищает название процедуры от лишних слов
        """
        if not procedure_text:
            return ""
        
        # Убираем лишние слова и символы
        clean = procedure_text.strip()
        clean = clean.replace('Разовое посещение', '')
        clean = clean.replace('посещение', '')
        clean = clean.replace('массажа', 'массаж')
        clean = clean.replace('—', '').replace('–', '').replace('-', '')
        clean = ' '.join(clean.split())  # Убираем лишние пробелы
        
        return clean.strip()
