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

    async def process_booking_action(self, claude_response: ClaudeMainResponse, client_id: str, message_id: str,
                                     contact_send_id: str = None) -> Dict[str, Any]:
        """Process booking action from Claude response"""
        logger.info(f"Message ID: {message_id} - Processing booking action for client_id={client_id}")
        logger.debug(
            f"Message ID: {message_id} - Booking action details: activate={claude_response.activate_booking}, reject={claude_response.reject_order}, change={claude_response.change_order}")

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

            logger.info(
                f"Message ID: {message_id} - Booking action completed for client_id={client_id}: {result['action']} - success={result['success']}")
            return result

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error processing booking action for client_id={client_id}: {e}",
                         exc_info=True)
            return {
                "success": False,
                "message": f"Ошибка при обработке заказа: {str(e)}",
                "action": "error"
            }

    async def _activate_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str,
                                contact_send_id: str = None) -> Dict[str, Any]:
        """Activate a new booking"""
        logger.info(f"Message ID: {message_id} - Activating booking for client_id={client_id}")
        logger.info(f"DEBUG START: _activate_booking called with contact_send_id={contact_send_id}")

        try:
            # Validate required fields
            if not response.cosmetolog or not response.date_order or not response.time_set_up:
                logger.warning(
                    f"Message ID: {message_id} - Missing required booking fields for client_id={client_id}: specialist={response.cosmetolog}, date={response.date_order}, time={response.time_set_up}")
                return {
                    "success": False,
                    "message": "Недостаточно данных для создания записи"
                }

            # Parse date and time
            try:
                booking_date = datetime.strptime(response.date_order, "%d.%m.%Y").date()
            except ValueError:
                try:
                    booking_date = datetime.strptime(response.date_order, "%d.%m").date().replace(
                        year=datetime.now().year)
                except ValueError:
                    logger.warning(
                        f"Message ID: {message_id} - Invalid date format for client_id={client_id}: {response.date_order}")
                    return {
                        "success": False,
                        "message": f"Неверный формат даты: {response.date_order}"
                    }

            try:
                booking_time = datetime.strptime(response.time_set_up, "%H:%M").time()
            except ValueError:
                logger.warning(
                    f"Message ID: {message_id} - Invalid time format for client_id={client_id}: {response.time_set_up}")
                return {
                    "success": False,
                    "message": f"Неверный формат времени: {response.time_set_up}"
                }

            # Check if specialist exists
            if response.cosmetolog not in self.project_config.specialists:
                logger.warning(
                    f"Message ID: {message_id} - Unknown specialist requested: {response.cosmetolog}, available: {self.project_config.specialists}")
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
                logger.info(
                    f"Message ID: {message_id} - Service '{response.procedure}' requires {duration_slots} slots ({duration_slots * 30} minutes)")
            elif response.procedure:
                # No direct match - try service normalization
                logger.info(
                    f"Message ID: {message_id} - Service '{response.procedure}' not found in dictionary, attempting normalization...")

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
                        logger.info(
                            f"Message ID: {message_id} - Normalized service '{normalized_service}' requires {duration_slots} slots ({duration_slots * 30} minutes)")
                    else:
                        logger.warning(
                            f"Message ID: {message_id} - Service normalization failed, using default duration: 1 slot (30 minutes)")

                    normalize_db.close()

                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Error during service normalization: {e}")
                    logger.warning(f"Message ID: {message_id} - Using default duration: 1 slot (30 minutes)")
            else:
                logger.warning(
                    f"Message ID: {message_id} - No service specified, using default duration: 1 slot (30 minutes)")

            # Check if time slot is available (double-check both database and Google Sheets)
            logger.debug(
                f"Message ID: {message_id} - Checking slot availability: specialist={response.cosmetolog}, date={booking_date}, time={booking_time}, duration={duration_slots}")

            # FIRST check Google Sheets as primary source
            try:
                if not await self.sheets_service.is_slot_available_in_sheets_async(response.cosmetolog, booking_date,
                                                                                   booking_time):
                    logger.warning(
                        f"Message ID: {message_id} - Time slot not available in Google Sheets: specialist={response.cosmetolog}, date={booking_date}, time={booking_time}")
                    return {
                        "success": False,
                        "message": "Выбранное время уже занято"
                    }
            except Exception as sheets_check_error:
                logger.error(
                    f"Message ID: {message_id} - Could not verify slot availability in Google Sheets: {sheets_check_error}")
                # CRITICAL: Do not allow booking if we can't verify sheets availability
                return {
                    "success": False,
                    "message": "Ошибка проверки доступности времени"
                }

            # Then check database as secondary validation
            if not self._is_slot_available(response.cosmetolog, booking_date, booking_time, duration_slots):
                logger.warning(
                    f"Message ID: {message_id} - Time slot not available in database: specialist={response.cosmetolog}, date={booking_date}, time={booking_time}")
                # Don't block if DB says busy but Sheets says free
                logger.info(
                    f"Message ID: {message_id} - Continuing despite DB conflict - Google Sheets is primary source")

            # Create booking
            end_time = datetime.combine(booking_date, booking_time) + timedelta(minutes=30 * duration_slots)
            logger.info(
                f"Message ID: {message_id} - Creating new booking: client_id={client_id}, specialist={response.cosmetolog}")
            logger.info(f"Message ID: {message_id} -   Service: {normalized_service} ({duration_slots} slots)")
            logger.info(
                f"Message ID: {message_id} -   Time: {booking_date} {booking_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")

            # ФИНАЛЬНАЯ ПРОВЕРКА КОЛЛИЗИЙ (добавить перед booking = Booking)
            # Проверяем слот еще раз непосредственно перед записью
            try:
                final_check = await self.sheets_service.get_available_slots_async(self.db, booking_date, duration_slots)
                reserved_key = f'reserved_slots_{response.cosmetolog}'

                # Проверяем все слоты, которые займет эта запись
                slots_to_check = []
                for i in range(duration_slots):
                    check_time = (datetime.combine(booking_date, booking_time) + timedelta(minutes=30 * i)).time()
                    slots_to_check.append(check_time.strftime("%H:%M"))

                # Если хоть один слот занят - блокируем запись
                if reserved_key in final_check:
                    for slot in slots_to_check:
                        if slot in final_check[reserved_key]:
                            logger.error(
                                f"Message ID: {message_id} - COLLISION! Slot {slot} became occupied during booking!")
                            return {
                                "success": False,
                                "message": "ОШИБКА! СЛОТ ОКАЗАЛСЯ ЗАНЯТ",
                                "record_error": "ОШИБКА! СЛОТ ОКАЗАЛСЯ ЗАНЯТ"
                            }

                logger.info(f"Message ID: {message_id} - Final collision check passed for {len(slots_to_check)} slots")

            except Exception as e:
                logger.error(f"Message ID: {message_id} - Final check failed: {e}, aborting booking")
                return {
                    "success": False,
                    "message": "Ошибка проверки доступности",
                    "record_error": "Ошибка проверки доступности"
                }

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

            logger.info(
                f"Message ID: {message_id} - Booking created successfully: booking_id={booking.id}, client_id={client_id}")
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
                    'client_id': contact_send_id if contact_send_id else client_id,
                    # Используем SendPulse ID для Make.com
                    'messenger_client_id': client_id,  # ДОБАВЛЯЕМ: Messenger ID для истории
                    'time': booking_time.strftime('%H:%M'),
                    'client_name': response.name or "Клиент",
                    'service': response.procedure or "Услуга",
                    'specialist': response.cosmetolog
                }
                logger.info(
                    f"Message ID: {message_id} - About to call add_booking_to_make_table_async with data: {make_booking_data}")
                await self.sheets_service.add_booking_to_make_table_async(make_booking_data)
                logger.info(f"Message ID: {message_id} - Added booking to Make.com table for 24h reminder")
            except Exception as make_error:
                logger.error(f"Message ID: {message_id} - Failed to add to Make.com table: {make_error}")
                # Don't fail the booking if Make.com table update fails

            # Update Google Sheets - targeted update for this specific booking (async)
            try:
                logger.debug(f"Message ID: {message_id} - Updating specific booking slot {booking.id} in Google Sheets")
                sheets_success = await self.sheets_service.update_single_booking_slot_async(booking.specialist_name,
                                                                                            booking)
                if sheets_success:
                    logger.debug(f"Message ID: {message_id} - Google Sheets slot update completed successfully")
                else:
                    logger.warning(f"Message ID: {message_id} - Google Sheets slot update returned false")
            except Exception as sheets_error:
                logger.error(
                    f"Message ID: {message_id} - Failed to update booking slot in Google Sheets: {sheets_error}")
                # Don't fail the booking for sheets sync issues

            return {
                "success": True,
                #  "message": f"Запись создана: {response.cosmetolog}, {booking_date.strftime('%d.%m.%Y')} {booking_time.strftime('%H:%M')}",
                "message": None,
                "booking_id": booking.id
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error creating booking for client_id={client_id}: {e}",
                         exc_info=True)
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
        logger.info(f"Message ID: {message_id} - Rejecting single booking for client_id={client_id}")

        try:
            # Validate required fields
            if not response.date_reject or not response.time_reject:
                logger.warning(
                    f"Message ID: {message_id} - Missing booking data: date={response.date_reject}, time={response.time_reject}")
                return {
                    "success": False,
                    "message": "Недостаточно данных для отмены записи"
                }

            # Parse date and time
            booking_date = self._parse_date(response.date_reject)
            booking_time = self._parse_time(response.time_reject)

            if not booking_date or not booking_time:
                logger.warning(f"Message ID: {message_id} - Invalid date/time format")
                return {
                    "success": False,
                    "message": "Неверный формат даты или времени"
                }

            # Find booking to cancel
            booking = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.appointment_date == booking_date,
                    Booking.appointment_time == booking_time,
                    Booking.status == "active"
                )
            ).first()

            if not booking:
                logger.warning(f"Message ID: {message_id} - Booking not found for cancellation")
                return {
                    "success": False,
                    "message": "Запись для отмены не найдена"
                }

            # Cancel booking
            booking.status = "cancelled"
            booking.updated_at = datetime.utcnow()

            self.db.commit()

            logger.info(f"Message ID: {message_id} - Booking cancelled in database: booking_id={booking.id}")

            # Clear slot in Google Sheets
            try:
                duration_slots = booking.duration_minutes // 30
                await self.sheets_service.clear_booking_slot_async(
                    booking.specialist_name,
                    booking.appointment_date,
                    booking.appointment_time,
                    duration_slots
                )
                logger.debug(f"Message ID: {message_id} - Cleared booking slot in Google Sheets")
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - Failed to clear booking slot: {sheets_error}")
                # Continue despite error

            # Log cancellation to Google Sheets
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
                logger.debug(f"Message ID: {message_id} - Cancellation logged to Google Sheets")
            except Exception as log_error:
                logger.error(f"Message ID: {message_id} - Failed to log cancellation: {log_error}")

            return {
                "success": True,
                "message": f"Запись отменена: {booking.specialist_name}, {booking.appointment_date.strftime('%d.%m.%Y')} {booking.appointment_time.strftime('%H:%M')}",
                "booking_id": booking.id
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error cancelling single booking: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Ошибка при отмене записи: {str(e)}"
            }

    async def _reject_double_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Reject/cancel a double booking - supports different times for each specialist"""
        try:
            if not response.specialists_list or len(response.specialists_list) < 2:
                return {
                    "success": False,
                    "message": "Недостаточно специалистов для отмены двойной записи"
                }

            specialist1, specialist2 = response.specialists_list[0], response.specialists_list[1]

            # Parse date
            booking_date = self._parse_date(response.date_reject)
            if not booking_date:
                return {
                    "success": False,
                    "message": "Неверный формат даты"
                }

            # Извлекаем время для КАЖДОГО мастера отдельно
            times_reject_list = getattr(response, 'times_reject_list', None)
            if times_reject_list and len(times_reject_list) >= 2:
                booking_time1 = self._parse_time(times_reject_list[0])
                booking_time2 = self._parse_time(times_reject_list[1])
                logger.info(f"Message ID: {message_id} - Cancelling with different times: {specialist1} at {times_reject_list[0]}, {specialist2} at {times_reject_list[1]}")
            else:
                # Если список времен не предоставлен - ищем по ЛЮБОМУ времени на эту дату
                booking_time1 = None
                booking_time2 = None
                logger.info(f"Message ID: {message_id} - No times_reject_list, will search for any booking on {booking_date}")

            cancelled_bookings = []

            # Найти и отменить записи для КАЖДОГО мастера С ЕГО ВРЕМЕНЕМ
            specialists_times = [
                (specialist1, booking_time1),
                (specialist2, booking_time2)
            ]

            for specialist, booking_time in specialists_times:
                # Строим запрос в зависимости от того, есть ли время
                if booking_time:
                    # Ищем по конкретному времени
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
                else:
                    # Ищем ЛЮБУЮ запись на эту дату
                    booking = self.db.query(Booking).filter(
                        and_(
                            Booking.project_id == self.project_config.project_id,
                            Booking.client_id == client_id,
                            Booking.specialist_name == specialist,
                            Booking.appointment_date == booking_date,
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
                # Формируем подробное сообщение с временем для каждого мастера
                details = []
                for booking in cancelled_bookings:
                    details.append(f"{booking.specialist_name} в {booking.appointment_time.strftime('%H:%M')}")
                
                return {
                    "success": True,
                    "message": f"Двойная запись отменена: {' + '.join(details)}",
                    "booking_ids": [b.id for b in cancelled_bookings]
                }
            else:
                return {
                    "success": False,
                    "message": f"Записи не найдены для отмены на {booking_date.strftime('%d.%m.%Y')}"
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
        logger.info(f"Message ID: {message_id} - Changing single booking for client_id={client_id}")

        try:
            # Validate required fields
            if not response.date_reject or not response.time_reject:
                logger.warning(
                    f"Message ID: {message_id} - Missing old booking data: date={response.date_reject}, time={response.time_reject}")
                return {
                    "success": False,
                    "message": "Недостаточно данных для поиска старой записи"
                }

            if not response.date_order or not response.time_set_up:
                logger.warning(
                    f"Message ID: {message_id} - Missing new booking data: date={response.date_order}, time={response.time_set_up}")
                return {
                    "success": False,
                    "message": "Недостаточно данных для новой записи"
                }

            # Parse dates and times
            old_date = self._parse_date(response.date_reject)
            old_time = self._parse_time(response.time_reject)
            new_date = self._parse_date(response.date_order)
            new_time = self._parse_time(response.time_set_up)

            if not old_date or not old_time:
                logger.warning(f"Message ID: {message_id} - Invalid old date/time format")
                return {
                    "success": False,
                    "message": "Неверный формат старой даты или времени"
                }

            if not new_date or not new_time:
                logger.warning(f"Message ID: {message_id} - Invalid new date/time format")
                return {
                    "success": False,
                    "message": "Неверный формат новой даты или времени"
                }

            # Find existing booking
            booking = self.db.query(Booking).filter(
                and_(
                    Booking.project_id == self.project_config.project_id,
                    Booking.client_id == client_id,
                    Booking.appointment_date == old_date,
                    Booking.appointment_time == old_time,
                    Booking.status == "active"
                )
            ).first()

            if not booking:
                logger.warning(
                    f"Message ID: {message_id} - Booking not found for transfer: client_id={client_id}, date={old_date}, time={old_time}")
                return {
                    "success": False,
                    "message": "Запись для переноса не найдена"
                }

            # Check if new time slot is available
            new_specialist = response.cosmetolog or booking.specialist_name
            duration_slots = booking.duration_minutes // 30

            # Check in Google Sheets
            try:
                if not await self.sheets_service.is_slot_available_in_sheets_async(new_specialist, new_date, new_time):
                    logger.warning(f"Message ID: {message_id} - New time slot not available in Google Sheets")
                    return {
                        "success": False,
                        "message": "Новое время уже занято"
                    }
            except Exception as sheets_error:
                logger.error(f"Message ID: {message_id} - Error checking new slot availability: {sheets_error}")
                return {
                    "success": False,
                    "message": "Ошибка проверки доступности нового времени"
                }

            # Save old booking data for logging AND for clearing slots
            old_specialist = booking.specialist_name
            old_date = booking.appointment_date
            old_time = booking.appointment_time
            old_duration_slots = booking.duration_minutes // 30
            old_procedure = booking.service_name

            # Clear old slot in Google Sheets BEFORE updating booking
            try:
                await self.sheets_service.clear_booking_slot_async(
                    old_specialist,
                    old_date,
                    old_time,
                    old_duration_slots
                )
                logger.debug(f"Message ID: {message_id} - Cleared old booking slot in Google Sheets: {old_specialist} at {old_date} {old_time}")
            except Exception as clear_error:
                logger.error(f"Message ID: {message_id} - Failed to clear old slot: {clear_error}")
                # Continue despite error

            # Update booking with new data
            booking.specialist_name = new_specialist
            booking.appointment_date = new_date
            booking.appointment_time = new_time

            if response.name:
                booking.client_name = response.name
            if response.procedure:
                booking.service_name = response.procedure
            if response.phone:
                booking.client_phone = response.phone

            booking.updated_at = datetime.utcnow()

            self.db.commit()
            self.db.refresh(booking)

            logger.info(f"Message ID: {message_id} - Booking updated in database: booking_id={booking.id}")

            # Update new slot in Google Sheets
            try:
                await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                logger.debug(f"Message ID: {message_id} - Updated new booking slot in Google Sheets")
            except Exception as update_error:
                logger.error(f"Message ID: {message_id} - Failed to update new slot: {update_error}")

            # Log transfer to Google Sheets
            try:
                transfer_data = {
                    "old_date": old_date.strftime("%d.%m"),
                    "old_full_date": old_date.strftime("%d.%m.%Y"),
                    "old_time": str(old_time),
                    "new_date": new_date.strftime("%d.%m"),
                    "new_time": str(new_time),
                    "client_id": client_id,
                    "client_name": booking.client_name or "Клиент",
                    "service": booking.service_name or old_procedure or "Услуга",
                    "old_specialist": old_specialist,
                    "new_specialist": new_specialist
                }
                await self.sheets_service.log_transfer(transfer_data)
                logger.debug(f"Message ID: {message_id} - Transfer logged to Google Sheets")
            except Exception as log_error:
                logger.error(f"Message ID: {message_id} - Failed to log transfer: {log_error}")

            return {
                "success": True,
                "message": f"Запись перенесена: {new_specialist}, {new_date.strftime('%d.%m.%Y')} {new_time.strftime('%H:%M')}",
                "booking_id": booking.id
            }

        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error changing single booking: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Ошибка при переносе записи: {str(e)}"
            }

    async def _change_double_booking(self, response: ClaudeMainResponse, client_id: str, message_id: str) -> Dict[
        str, Any]:
        """Change a double booking"""
        logger.info(
            f"🔧 TRANSFER DEBUG: _change_double_booking START for message_id={message_id}, client_id={client_id}")

        try:
            # 🔒 Перевірка базових даних з Claude
            if not response or not isinstance(response, ClaudeMainResponse):
                return {"success": False, "message": "Некорректный ответ от нейросети"}

            if not response.specialists_list or len(response.specialists_list) < 2:
                return {"success": False, "message": "Недостаточно специалистов для переноса двойной записи"}

            if not getattr(response, "date_order", None):
                return {"success": False, "message": "Не указана новая дата для переноса"}

            # Ініціалізація дати, з якої переносимо
            source_date = None
            if getattr(response, "date_reject", None):
                source_date = self._parse_date(response.date_reject)
                logger.info(f"Message ID: {message_id} - Looking for bookings to transfer FROM date: {source_date}")
            else:
                logger.warning(
                    f"Message ID: {message_id} - No date_reject specified, will search latest active bookings")

            specialist1, specialist2 = response.specialists_list[0], response.specialists_list[1]

            # 🔍 Пошук вихідних записів
            if source_date:
                booking1 = self.db.query(Booking).filter(
                    and_(
                        Booking.project_id == self.project_config.project_id,
                        Booking.client_id == client_id,
                        Booking.specialist_name == specialist1,
                        Booking.appointment_date == source_date,
                        Booking.status == "active"
                    )
                ).first()
                booking2 = self.db.query(Booking).filter(
                    and_(
                        Booking.project_id == self.project_config.project_id,
                        Booking.client_id == client_id,
                        Booking.specialist_name == specialist2,
                        Booking.appointment_date == source_date,
                        Booking.status == "active"
                    )
                ).first()
            else:
                booking1 = self.db.query(Booking).filter(
                    and_(
                        Booking.project_id == self.project_config.project_id,
                        Booking.client_id == client_id,
                        Booking.specialist_name == specialist1,
                        Booking.status == "active"
                    )
                ).order_by(Booking.appointment_date.desc()).first()
                booking2 = self.db.query(Booking).filter(
                    and_(
                        Booking.project_id == self.project_config.project_id,
                        Booking.client_id == client_id,
                        Booking.specialist_name == specialist2,
                        Booking.status == "active"
                    )
                ).order_by(Booking.appointment_date.desc()).first()

            if not booking1 or not booking2:
                missing = []
                if not booking1:
                    missing.append(
                        f"{specialist1} ({'нет активных записей' if not source_date else source_date.strftime('%d.%m.%Y')})")
                if not booking2:
                    missing.append(
                        f"{specialist2} ({'нет активных записей' if not source_date else source_date.strftime('%d.%m.%Y')})")
                return {"success": False, "message": f"Не найдены записи для переноса у: {', '.join(missing)}"}

            bookings_to_change = [booking1, booking2]

            # Нова дата
            new_date = self._parse_date(response.date_order)
            if not new_date:
                return {"success": False, "message": "Неверный формат новой даты"}

            # Нові часи
            times_list = getattr(response, "times_set_up_list", None)
            if times_list and len(times_list) >= 2:
                new_time1 = self._parse_time(times_list[0])
                new_time2 = self._parse_time(times_list[1])
            else:
                new_time1, new_time2 = booking1.appointment_time, booking2.appointment_time

            # Нові процедури
            procedures_list = getattr(response, "procedures_list", None)
            if procedures_list and len(procedures_list) >= 2:
                new_procedure1, new_procedure2 = procedures_list[0], procedures_list[1]
            else:
                new_procedure1, new_procedure2 = booking1.service_name, booking2.service_name

            # Перевірка доступності слотів
            slot1_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist1, new_date,
                                                                                          new_time1)
            slot2_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist2, new_date,
                                                                                          new_time2)
            if not slot1_available or not slot2_available:
                occupied = []
                if not slot1_available:
                    occupied.append(f"{specialist1} на {new_time1.strftime('%H:%M')}")
                if not slot2_available:
                    occupied.append(f"{specialist2} на {new_time2.strftime('%H:%M')}")
                return {"success": False, "message": f"Новое время занято: {', '.join(occupied)}"}

            # Збереження старих даних
            old_data = []
            for booking in bookings_to_change:
                old_data.append({
                    "specialist": booking.specialist_name,
                    "date": booking.appointment_date,
                    "time": booking.appointment_time,
                    "duration_slots": booking.duration_minutes // 30,
                    "service_name": booking.service_name
                })

            # Очищення старих слотів
            for old_slot_data in old_data:
                try:
                    await self.sheets_service.clear_booking_slot_async(
                        old_slot_data["specialist"],
                        old_slot_data["date"],
                        old_slot_data["time"],
                        old_slot_data["duration_slots"]
                    )
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Failed to clear old slot: {e}")

            # Оновлення об’єктів у БД
            new_times = [new_time1, new_time2]
            new_procedures = [new_procedure1, new_procedure2]
            for i, booking in enumerate(bookings_to_change):
                booking.appointment_date = new_date
                booking.appointment_time = new_times[i]
                booking.service_name = new_procedures[i]
                booking.client_name = response.name or booking.client_name
                booking.client_phone = response.phone or booking.client_phone
                booking.updated_at = datetime.utcnow()

                if new_procedures[i] in self.project_config.services:
                    duration_slots = self.project_config.services[new_procedures[i]]
                    booking.duration_minutes = duration_slots * 30

            self.db.commit()

            # Створення нових слотів у Sheets
            for booking in bookings_to_change:
                try:
                    await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Failed to create new slot: {e}")

            # Логування переносу
            for i, booking in enumerate(bookings_to_change):
                try:
                    transfer_data = {
                        "old_date": old_data[i]["date"].strftime("%d.%m"),
                        "old_full_date": old_data[i]["date"].strftime("%d.%m.%Y"),
                        "old_time": str(old_data[i]["time"]),
                        "new_date": new_date.strftime("%d.%m"),
                        "new_time": str(new_times[i]),
                        "client_id": client_id,
                        "client_name": booking.client_name or "Клиент",
                        "service": booking.service_name,
                        "old_specialist": old_data[i]["specialist"],
                        "new_specialist": booking.specialist_name
                    }
                    await self.sheets_service.log_transfer(transfer_data)
                except Exception as e:
                    logger.error(f"Message ID: {message_id} - Failed to log transfer: {e}")

            details = []
            for i, booking in enumerate(bookings_to_change):
                details.append(
                    f"{booking.specialist_name}: {old_data[i]['time'].strftime('%H:%M')}→{new_times[i].strftime('%H:%M')} ({booking.service_name})"
                )

            return {"success": True, "message": f"Двойная запись перенесена: {' + '.join(details)}",
                    "booking_ids": [b.id for b in bookings_to_change]}

        except Exception as e:
            logger.error(f"🔧 TRANSFER DEBUG: _change_double_booking EXCEPTION for message_id={message_id}: {e}",
                         exc_info=True)
            return {"success": False, "message": f"Ошибка при переносе двойной записи: {str(e)}"}

    def _is_slot_available(self, specialist: str, booking_date: date, booking_time: time, duration_slots: int,
                           exclude_booking_id: Optional[int] = None) -> bool:
        """Check if a time slot is available for booking"""
        # Generate all time slots that would be occupied
        occupied_slots = []
        for i in range(duration_slots):
            slot_datetime = datetime.combine(booking_date, booking_time) + timedelta(minutes=30 * i)
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
                existing_slot = datetime.combine(booking_date, booking.appointment_time) + timedelta(minutes=30 * i)
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

                logger.debug(
                    f"Message ID: {message_id} - Saving feedback to 'Хран' sheet with name='{client_name}', phone='{client_phone}'")
                sheets_success = await self.sheets_service.save_feedback_to_sheets_async(
                    client_id=client_id,
                    client_name=client_name,
                    client_phone=client_phone,
                    feedback_text=response.feedback
                )

                if sheets_success:
                    logger.info(
                        f"Message ID: {message_id} - Feedback saved to Google Sheets successfully for client_id={client_id}")
                else:
                    logger.warning(
                        f"Message ID: {message_id} - Failed to save feedback to Google Sheets for client_id={client_id}")

            except Exception as sheets_error:
                logger.error(
                    f"Message ID: {message_id} - Error saving feedback to Google Sheets for client_id={client_id}: {sheets_error}")
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
        """Активация двойной записи к двум мастерам"""
        logger.info(f"Message ID: {message_id} - Activating DOUBLE booking for client_id={client_id}")

        if not response.specialists_list or len(response.specialists_list) < 2:
            return {"success": False, "message": "Недостаточно специалистов для двойной записи"}

        specialist1, specialist2 = response.specialists_list[0], response.specialists_list[1]

        # Проверить доступность ОБОИХ мастеров
        booking_date = self._parse_date(response.date_order)
        
        # Извлекаем время для КАЖДОГО мастера отдельно
        # Если есть список времен - используем его, иначе используем одно время для обоих
        times_list = getattr(response, 'times_set_up_list', None)
        if times_list and len(times_list) >= 2:
            booking_time1 = self._parse_time(times_list[0])
            booking_time2 = self._parse_time(times_list[1])
            logger.info(f"Message ID: {message_id} - Using different times: {specialist1} at {times_list[0]}, {specialist2} at {times_list[1]}")
        else:
            # Если список времен не предоставлен, используем одно время
            booking_time1 = booking_time2 = self._parse_time(response.time_set_up)
            logger.warning(f"Message ID: {message_id} - No times_set_up_list provided, using same time {response.time_set_up} for both specialists")
        
        # Извлекаем процедуры для КАЖДОГО мастера отдельно
        procedures_list = getattr(response, 'procedures_list', None)
        if procedures_list and len(procedures_list) >= 2:
            procedure1 = procedures_list[0]
            procedure2 = procedures_list[1]
            logger.info(f"Message ID: {message_id} - Using different procedures: {specialist1} - {procedure1}, {specialist2} - {procedure2}")
        else:
            # Если список процедур не предоставлен, используем одну процедуру
            procedure1 = procedure2 = response.procedure
            logger.warning(f"Message ID: {message_id} - No procedures_list provided, using same procedure '{response.procedure}' for both specialists")

        # Проверка в Google Sheets для обоих мастеров С ИХ ВРЕМЕНЕМ
        slot1_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist1, booking_date,
                                                                                      booking_time1)
        slot2_available = await self.sheets_service.is_slot_available_in_sheets_async(specialist2, booking_date,
                                                                                      booking_time2)

        if not slot1_available or not slot2_available:
            occupied_specialists = []
            if not slot1_available:
                occupied_specialists.append(f"{specialist1} на {booking_time1.strftime('%H:%M')}")
            if not slot2_available:
                occupied_specialists.append(f"{specialist2} на {booking_time2.strftime('%H:%M')}")
            return {
                "success": False,
                "message": f"Время занято: {', '.join(occupied_specialists)}"
            }

        # Создать ДВЕ записи в БД с РАЗНЫМ временем и РАЗНЫМИ процедурами
        bookings = []
        specialists_data = [
            (specialist1, booking_time1, procedure1),
            (specialist2, booking_time2, procedure2)
        ]
        
        for specialist, booking_time, procedure in specialists_data:
            # Определяем длительность процедуры
            duration_slots = 1
            if procedure and procedure in self.project_config.services:
                duration_slots = self.project_config.services[procedure]
                logger.info(f"Message ID: {message_id} - Service '{procedure}' for {specialist} requires {duration_slots} slots")
            
            booking = Booking(
                project_id=self.project_config.project_id,
                specialist_name=specialist,
                appointment_date=booking_date,
                appointment_time=booking_time,
                client_id=client_id,
                client_name=response.name,
                service_name=procedure,
                client_phone=response.phone,
                duration_minutes=duration_slots * 30,
                status="active"
            )
            self.db.add(booking)
            bookings.append(booking)
            logger.info(f"Message ID: {message_id} - Created booking for {specialist} at {booking_time.strftime('%H:%M')} - {procedure}")

        self.db.commit()

        # Обновить Google Sheets для ОБОИХ мастеров
        for booking in bookings:
            await self.sheets_service.update_single_booking_slot_async(booking.specialist_name, booking)

        # Добавить в Make.com таблицу (для каждого мастера отдельно)
        for i, booking in enumerate(bookings):
            make_booking_data = {
                'date': booking_date.strftime("%d.%m.%Y"),
                'client_id': contact_send_id if contact_send_id else client_id,
                'time': booking.appointment_time.strftime('%H:%M'),
                'client_name': response.name or "Клиент",
                'service': booking.service_name,
                'specialist': booking.specialist_name
            }
            await self.sheets_service.add_booking_to_make_table_async(make_booking_data)

        return {
            "success": True,
            "message": f"Двойная запись создана: {specialist1} ({procedure1} в {booking_time1.strftime('%H:%M')}) + {specialist2} ({procedure2} в {booking_time2.strftime('%H:%M')})",
            "booking_ids": [b.id for b in bookings]
        }
