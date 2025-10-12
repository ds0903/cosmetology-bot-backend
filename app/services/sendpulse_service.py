import httpx
import logging
from typing import Optional
from ..config import settings

logger = logging.getLogger(__name__)


class SendPulseService:
    """Service for sending responses back to SendPulse API"""
    
    def __init__(self):
        self.api_url = settings.sendpulse_api_url
        self.api_token = settings.sendpulse_api_token
        self.client = httpx.AsyncClient(timeout=30.0)
        logger.info("SendPulseService initialized")
    
    async def send_response(
        self, 
        client_id: str, 
        project_id: str,
        response_text: str,
        pic: str = "",
        count: str = "0",
        send_status: str = "TRUE"
    ) -> bool:
        """Send response back to SendPulse via webhook callback or direct API"""
        try:
            if not self.api_url:
                logger.warning("SendPulse API URL not configured, response will not be sent")
                logger.info(f"Would send to client_id={client_id}: {response_text[:100]}...")
                return True  # Return True to not block the flow
            
            # Prepare payload for SendPulse
            payload = {
                "contact_id": client_id,
                "message": response_text
            }
            
            if pic:
                payload["image_url"] = pic
            
            headers = {
                "Content-Type": "application/json"
            }
            
            if self.api_token:
                headers["Authorization"] = f"Bearer {self.api_token}"
            
            logger.info(f"Sending message to SendPulse for client_id={client_id}")
            logger.debug(f"Payload: {payload}")
            
            response = await self.client.post(
                self.api_url,
                json=payload,
                headers=headers
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully sent response to SendPulse for client_id={client_id}")
                return True
            else:
                logger.error(f"SendPulse API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send response to SendPulse for client_id={client_id}: {e}")
            return False
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 