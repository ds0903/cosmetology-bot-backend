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
        """Send response back to SendPulse API"""
        try:
            if not self.api_url or not self.api_token:
                logger.warning("SendPulse API URL or token not configured, skipping response send")
                return False
            
            payload = {
                "client_id": client_id,
                "project_id": project_id,
                "gpt_response": response_text,
                "pic": pic,
                "count": count,
                "send_status": send_status
            }
            
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Sending response to SendPulse API for client_id={client_id}")
            logger.info(f"Response length: {len(response_text)} chars, send_status: {send_status}, count: {count}")
            
            response = await self.client.post(
                self.api_url,
                json=payload,
                headers=headers
            )
            
            if response.status_code == 200:
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