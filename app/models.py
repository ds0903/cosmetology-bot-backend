from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date, time
from enum import Enum


class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


class BookingAction(str, Enum):
    ACTIVATE = "activate_booking"
    REJECT = "reject_order"
    CHANGE = "change_order"


class Attachment(BaseModel):
    """Attachment from SendPulse"""
    type: str = Field(..., description="Type: photo, video, document")
    url: str = Field(..., description="URL to file")


class SendPulseMessage(BaseModel):
    """Message received from SendPulse webhook"""
    date: str = Field(..., description="Date in format 02.07.2025 19:18")
    count: int = Field(default=0, description="Counter for handling message floods")
    retry: bool = Field(default=False, description="Whether this is a retry attempt")
    tg_id: Optional[str] = Field(None, description="Telegram client ID")
    contact_send_id: Optional[str] = Field(None, description="SendPulse internal contact ID")
    response: str = Field(..., description="Message text from client")
    project_id: str = Field(..., description="Project identifier")
    
    # Image support fields
    image_url: Optional[str] = Field(None, description="Direct URL of image from user")
    attachments: Optional[List[Attachment]] = Field(None, description="List of attachments from SendPulse")
    
    def get_image_url(self) -> Optional[str]:
        """Get image URL from either direct field or attachments"""
        # First check direct URL
        if self.image_url:
            return self.image_url
        
        # Then check attachments
        if self.attachments:
            for attachment in self.attachments:
                if attachment.type in ["photo", "image"]:
                    return attachment.url
        
        return None


class MessageQueueItem(BaseModel):
    """Message in the processing queue"""
    id: str
    project_id: str
    client_id: str
    original_message: str
    aggregated_message: str
    status: MessageStatus
    created_at: datetime
    updated_at: datetime
    retry_count: int = 0


class IntentDetectionResult(BaseModel):
    """Result from Claude intent detection"""
    waiting: Optional[int] = Field(None, description="1 if client is just chatting")
    date_order: Optional[str] = Field(None, description="Date in DD.MM format")
    desire_time0: Optional[str] = Field(None, description="Start time preference")
    desire_time1: Optional[str] = Field(None, description="End time preference")


class ServiceIdentificationResult(BaseModel):
    """Result from Claude service identification"""
    time_fraction: int = Field(..., description="Service duration in 30-min slots")
    service_name: str = Field(..., description="Identified service name")


class AvailableSlots(BaseModel):
    """Available booking slots for specialists"""
    date_of_checking: str  # The date these slots are for (DD.MM format)
    target_date: str  # The target date being searched (DD.MM format) 
    slots_by_specialist: Dict[str, List[str]]
    reserved_slots_by_specialist: Optional[Dict[str, List[str]]] = Field(default_factory=dict)


class ClaudeMainResponse(BaseModel):
    """Main response from Claude with booking actions"""
    gpt_response: str
    pic: Optional[str] = Field(None, description="Picture URL if needed")
    activate_booking: Optional[bool] = Field(None)
    reject_order: Optional[bool] = Field(None)
    change_order: Optional[bool] = Field(None)
    booking_confirmed: Optional[bool] = Field(None)
    booking_declined: Optional[bool] = Field(None)
    cosmetolog: Optional[str] = Field(None, description="Specialist name")
    time_set_up: Optional[str] = Field(None, description="Booking time HH:MM")
    date_order: Optional[str] = Field(None, description="Booking date")
    time_reject: Optional[str] = Field(None, description="Time to reject")
    date_reject: Optional[str] = Field(None, description="Date to reject")
    procedure: Optional[str] = Field(None, description="Service name")
    phone: Optional[str] = Field(None, description="Client phone")
    name: Optional[str] = Field(None, description="Client name")
    feedback: Optional[str] = Field(None, description="Client feedback")

    cosmetolog_2: Optional[str] = Field(None, description="Second specialist name")
    double_booking: Optional[bool] = Field(None, description="Is this a double booking")
    specialists_list: Optional[List[str]] = Field(None, description="List of specialists for double booking")
    times_set_up_list: Optional[List[str]] = Field(None, description="List of times for each specialist in double booking (HH:MM format)")
    procedures_list: Optional[List[str]] = Field(None, description="List of procedures for each specialist in double booking")


class BookingRecord(BaseModel):
    """Single booking record"""
    id: Optional[int] = None
    project_id: str
    specialist_name: str
    date: date
    time: time
    client_id: str
    client_name: Optional[str] = None
    service_name: Optional[str] = None
    phone: Optional[str] = None
    duration_slots: int = Field(default=1, description="Duration in 30-min slots")
    status: str = Field(default="active")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DialogueEntry(BaseModel):
    """Single dialogue entry"""
    id: Optional[int] = None
    project_id: str
    client_id: str
    role: str = Field(..., description="'client' or 'claude'")
    message: str
    timestamp: datetime
    is_archived: bool = Field(default=False)


class DialogueHistory(BaseModel):
    """Complete dialogue history for a client"""
    project_id: str
    client_id: str
    entries: List[DialogueEntry]
    last_message_at: datetime
    zip_history: Optional[str] = Field(None, description="Compressed dialogue history")


class FeedbackRecord(BaseModel):
    """Client feedback record"""
    id: Optional[int] = None
    project_id: str
    client_id: str
    client_name: Optional[str] = None
    phone: Optional[str] = None
    feedback_text: str
    feedback_date: datetime
    created_at: Optional[datetime] = None


class WebhookResponse(BaseModel):
    """Response sent back to SendPulse"""
    gpt_response: str = Field(..., description="Response text to send to client")
    pic: str = Field(default="", description="Picture URL, empty string if no picture")
    status: str = Field(default="200", description="HTTP status code")
    send_status: str = Field(..., description="TRUE or FALSE")
    count: Optional[str] = Field(None, description="Error count as string, None if no errors but FALSE status, 0 if TRUE status")
    user_message: Optional[str] = Field(None, description="Message from user")


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None


class ProjectStats(BaseModel):
    """Statistics for a project"""
    project_id: str
    total_messages: int
    total_bookings: int
    active_bookings: int
    total_clients: int
    avg_response_time: Optional[float] = None 
