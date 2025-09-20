import redis
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from ..database import MessageQueue, ClientLastActivity
from ..models import SendPulseMessage, MessageQueueItem, MessageStatus
from ..config import settings

logger = logging.getLogger(__name__)

# Redis connection for real-time queue management
redis_client = redis.from_url(settings.redis_url)


class MessageQueueService:
    """Service for handling message queue operations"""
    
    def __init__(self, db: Session):
        self.db = db
        self.redis_client = redis_client
        logger.debug("MessageQueueService initialized")
    
    def process_incoming_message(self, message: SendPulseMessage, message_id: str) -> Dict[str, Any]:
        """
        Process incoming message from SendPulse according to the technical specification:
        1. Check if message should be processed (retry logic)
        2. Add to queue or update existing message
        3. Handle message aggregation for flood protection
        4. Coordinate send_status for concurrent messages
        """
        client_id = message.tg_id
        if not client_id:
            logger.error(f"Message ID: {message_id} - No client ID provided in message: {message}")
            return {"error": "No client ID provided"}
        
        logger.info(f"Message ID: {message_id} - Processing incoming message {message.response[:100]} from client_id={client_id}, project_id={message.project_id}")
        logger.info(f"Message ID: {message_id} - Message details: count={message.count}, retry={message.retry}, message='{message.response[:100]}...'")
        
        # Step 3: Check retry logic
        if not self._should_process_message(message, message_id):
            logger.info(f"Message ID: {message_id} - Message skipped due to retry logic for client_id={client_id}")
            return {
                "send_status": "FALSE",
                "count": "1",
                "message": "Message skipped due to retry logic"
            }
        
        # Step 2: Check for existing messages and coordinate responses
        coordination_result = self._coordinate_client_messages(message, message_id)
        
        if coordination_result.get("should_return_false"):
            logger.info(f"Message ID: {message_id} - Message should return FALSE due to newer messages for client_id={client_id}")
            return {
                "send_status": "FALSE", 
                "queue_item_id": coordination_result["queue_item_id"],
                "message": "Superseded by newer message"
            }
        
        # Step 4: Add to queue and handle aggregation
        logger.debug(f"Message ID: {message_id} - Adding message to queue for client_id={client_id}")
        queue_item = coordination_result["queue_item"]
        
        # Update client last activity
        logger.debug(f"Message ID: {message_id} - Updating client activity for client_id={client_id}")
        self._update_client_activity(message.project_id, client_id, message_id)
        
        logger.info(f"Message ID: {message_id} - Message queued successfully for client_id={client_id}, queue_item_id={queue_item.id}")
        return {
            "queue_item_id": queue_item.id,
            "status": "queued",
            "aggregated_message": queue_item.aggregated_message
        }
    
    def _should_process_message(self, message: SendPulseMessage, message_id: str) -> bool:
        """
        Determine if message should be processed based on retry logic
        From spec: if (retry = false) or (retry = true and count != 0) - process
        """
        client_id = message.tg_id
        should_process = False
        
        if not message.retry:
            should_process = True
            logger.debug(f"Message ID: {message_id} - Processing message for client_id={client_id}: not a retry")
        elif message.retry and message.count != 0:
            should_process = True
            logger.debug(f"Message ID: {message_id} - Processing message for client_id={client_id}: retry with count={message.count}")
        else:
            logger.debug(f"Message ID: {message_id} - Skipping message for client_id={client_id}: retry with count=0")
        
        return should_process
    
    def check_for_new_messages_during_processing(self, project_id: str, client_id: str, processing_message_id: str) -> Optional[str]:
        """
        Check if new messages arrived while a message was being processed
        If yes, return the concatenated batch of new messages
        """
        logger.debug(f"Checking for new messages during processing for client_id={client_id}")
        
        # Get messages that arrived while processing (newer than the one being processed)
        processing_message = self.db.query(MessageQueue).filter(MessageQueue.id == processing_message_id).first()
        if not processing_message:
            logger.warning(f"Processing message {processing_message_id} not found")
            return None
            
        new_messages = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == project_id,
                MessageQueue.client_id == client_id,
                MessageQueue.status == MessageStatus.PENDING,
                MessageQueue.created_at > processing_message.created_at
            )
        ).order_by(MessageQueue.created_at).all()
        
        if not new_messages:
            logger.debug(f"No new messages arrived during processing for client_id={client_id}")
            return None
            
        logger.info(f"Found {len(new_messages)} new messages that arrived during processing for client_id={client_id}")
        
        # Concatenate all new messages
        concatenated_message = " ".join([msg.original_message for msg in new_messages])
        logger.info(f"Concatenated new messages for client_id={client_id}: '{concatenated_message[:100]}...'")
        
        # Mark all these messages as cancelled since we're batching them
        for msg in new_messages:
            msg.status = MessageStatus.CANCELLED
            msg.updated_at = datetime.utcnow()
            logger.debug(f"Cancelled message {msg.id} (batching into new processing)")
            
        self.db.commit()
        
        return concatenated_message
    
    def create_batched_message(self, project_id: str, client_id: str, concatenated_message: str) -> MessageQueueItem:
        """
        Create a new queue item for a batch of concatenated messages
        """
        logger.info(f"Creating batched message for client_id={client_id}: '{concatenated_message[:100]}...'")
        
        queue_item_id = str(uuid.uuid4())
        queue_item = MessageQueue(
            id=queue_item_id,
            project_id=project_id,
            client_id=client_id,
            original_message=concatenated_message,
            aggregated_message=concatenated_message,
            status=MessageStatus.PENDING,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.db.add(queue_item)
        self.db.commit()
        self.db.refresh(queue_item)
        
        logger.info(f"Batched message {queue_item_id} created successfully for client_id={client_id}")
        return queue_item
    
    def is_client_currently_processing(self, project_id: str, client_id: str) -> bool:
        """
        Check if a client currently has a message being processed
        """
        processing_message = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == project_id,
                MessageQueue.client_id == client_id,
                MessageQueue.status == MessageStatus.PROCESSING
            )
        ).first()
        
        is_processing = processing_message is not None
        logger.debug(f"Client {client_id} processing status: {is_processing}")
        return is_processing

    def _coordinate_client_messages(self, message: SendPulseMessage, message_id: str) -> Dict[str, Any]:
        """
        Coordinate messages for a client to ensure proper send_status behavior:
        - Mark previous pending/processing messages to return FALSE
        - Create/update queue item for current message  
        - Return coordination instructions
        
        Uses database row-level locking to prevent race conditions between concurrent webhook requests.
        """
        client_id = message.tg_id
        
        # Start a transaction with proper isolation to prevent race conditions
        try:
            # Use SELECT FOR UPDATE to lock existing messages for this client
            # This prevents other concurrent webhook requests from interfering
            logger.debug(f"Message ID: {message_id} - Acquiring locks for client_id={client_id} message coordination")
            
            existing_messages = self.db.query(MessageQueue).filter(
                and_(
                    MessageQueue.project_id == message.project_id,
                    MessageQueue.client_id == client_id,
                    MessageQueue.status.in_([MessageStatus.PENDING, MessageStatus.PROCESSING])
                )
            ).with_for_update().all()
            
            logger.debug(f"Message ID: {message_id} - Found {len(existing_messages)} existing messages for client_id={client_id}")
            
            # If there are existing messages, mark them to return FALSE
            if existing_messages:
                logger.info(f"Message ID: {message_id} - Marking {len(existing_messages)} existing messages to return FALSE for client_id={client_id}")
                
                # Collect all message text for aggregation
                all_messages = []
                for msg in existing_messages:
                    all_messages.append(msg.aggregated_message)
                    # Mark existing messages as superseded (they should return FALSE)
                    msg.status = MessageStatus.SUPERSEDED
                    msg.updated_at = datetime.utcnow()
                    logger.debug(f"Message ID: {message_id} - Marked message {msg.id} as superseded for client_id={client_id}")
                
                # Add current message to aggregation
                all_messages.append(message.response)
                aggregated_text = " ".join(all_messages)
                
                logger.info(f"Message ID: {message_id} - Aggregating {len(all_messages)} messages for client_id={client_id}: '{aggregated_text[:100]}...'")
            else:
                logger.debug(f"Message ID: {message_id} - No existing messages for client_id={client_id}, processing normally")
                aggregated_text = message.response
            
            # Create new queue item for current message
            queue_item_id = str(uuid.uuid4())
            logger.debug(f"Message ID: {message_id} - Creating new queue item {queue_item_id} for client_id={client_id}")
            queue_item = MessageQueue(
                id=queue_item_id,
                project_id=message.project_id,
                client_id=client_id,
                original_message=message.response,
                aggregated_message=aggregated_text,
                status=MessageStatus.PENDING,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            self.db.add(queue_item)
            self.db.commit()
            self.db.refresh(queue_item)
            
            logger.info(f"Message ID: {message_id} - Queue item {queue_item_id} created successfully for client_id={client_id}")
            
            return {
                "queue_item": queue_item,
                "queue_item_id": queue_item_id,
                "should_return_false": False  # Current message should be processed
            }
            
        except Exception as e:
            logger.error(f"Message ID: {message_id} - Error in message coordination for client_id={client_id}: {e}")
            self.db.rollback()
            raise
    
    def get_message_for_processing(self, project_id: str, client_id: str, message_id: str = None) -> Optional[MessageQueueItem]:
        """Get the latest pending message for a client"""
        if message_id:
            logger.debug(f"Message ID: {message_id} - Getting message for processing: project_id={project_id}, client_id={client_id}")
        else:
            logger.debug(f"Getting message for processing: project_id={project_id}, client_id={client_id}")
        
        message = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == project_id,
                MessageQueue.client_id == client_id,
                MessageQueue.status == MessageStatus.PENDING
            )
        ).order_by(desc(MessageQueue.created_at)).first()
        
        if message:
            if message_id:
                logger.debug(f"Message ID: {message_id} - Found pending message {message.id} for client_id={client_id}")
            else:
                logger.debug(f"Found pending message {message.id} for client_id={client_id}")
            return MessageQueueItem(
                id=message.id,
                project_id=message.project_id,
                client_id=message.client_id,
                original_message=message.original_message,
                aggregated_message=message.aggregated_message,
                status=MessageStatus(message.status),
                created_at=message.created_at,
                updated_at=message.updated_at,
                retry_count=message.retry_count
            )
        else:
            if message_id:
                logger.debug(f"Message ID: {message_id} - No pending message found for client_id={client_id}")
            else:
                logger.debug(f"No pending message found for client_id={client_id}")
            return None
    
    def update_message_status(self, queue_item_id: str, status: MessageStatus, message_id: str = None) -> bool:
        """Update message status"""
        if message_id:
            logger.debug(f"Message ID: {message_id} - Updating message status: queue_item_id={queue_item_id}, status={status.value}")
        else:
            logger.debug(f"Updating message status: queue_item_id={queue_item_id}, status={status.value}")
        
        message = self.db.query(MessageQueue).filter(MessageQueue.id == queue_item_id).first()
        if message:
            old_status = message.status
            message.status = status.value
            message.updated_at = datetime.utcnow()
            self.db.commit()
            if message_id:
                logger.info(f"Message ID: {message_id} - Message status updated: queue_item_id={queue_item_id}, {old_status} -> {status.value}")
            else:
                logger.info(f"Message status updated: queue_item_id={queue_item_id}, {old_status} -> {status.value}")
            return True
        else:
            if message_id:
                logger.warning(f"Message ID: {message_id} - Message not found for status update: queue_item_id={queue_item_id}")
            else:
                logger.warning(f"Message not found for status update: queue_item_id={queue_item_id}")
            return False
    
    def check_message_still_valid(self, message_id: str) -> bool:
        """
        Check if message is still valid for processing (not superseded by new messages)
        This is called before sending response to ensure no new messages arrived
        """
        message = self.db.query(MessageQueue).filter(MessageQueue.id == message_id).first()
        if not message:
            return False
        
        # Check if there are newer pending messages from same client
        newer_messages = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == message.project_id,
                MessageQueue.client_id == message.client_id,
                MessageQueue.status == MessageStatus.PENDING,
                MessageQueue.created_at > message.created_at
            )
        ).count()
        
        return newer_messages == 0
    
    def clear_client_queue(self, project_id: str, client_id: str) -> None:
        """Clear all messages for a client after successful processing"""
        logger.debug(f"Clearing client queue: project_id={project_id}, client_id={client_id}")
        
        messages = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == project_id,
                MessageQueue.client_id == client_id,
                MessageQueue.status.in_([MessageStatus.PENDING, MessageStatus.PROCESSING])
            )
        ).all()
        
        logger.info(f"Clearing {len(messages)} messages for client_id={client_id}")
        
        for message in messages:
            message.status = MessageStatus.COMPLETED
            message.updated_at = datetime.utcnow()
            logger.debug(f"Marked message {message.id} as completed for client_id={client_id}")
        
        self.db.commit()
        
        # Also clear Redis cache
        redis_key = f"aggregated_{project_id}_{client_id}"
        deleted_count = self.redis_client.delete(redis_key)
        if deleted_count > 0:
            logger.debug(f"Cleared Redis aggregation cache for client_id={client_id}")
        
        logger.info(f"Client queue cleared successfully for client_id={client_id}")
    
    def _update_client_activity(self, project_id: str, client_id: str, message_id: str) -> None:
        """Update client last activity timestamp"""
        logger.debug(f"Message ID: {message_id} - Updating client activity for project_id={project_id}, client_id={client_id}")
        activity = self.db.query(ClientLastActivity).filter(
            and_(
                ClientLastActivity.project_id == project_id,
                ClientLastActivity.client_id == client_id
            )
        ).first()
        
        if activity:
            activity.last_message_at = datetime.utcnow()
            logger.debug(f"Message ID: {message_id} - Updated existing activity record for client_id={client_id}")
        else:
            activity = ClientLastActivity(
                project_id=project_id,
                client_id=client_id,
                last_message_at=datetime.utcnow()
            )
            self.db.add(activity)
            logger.debug(f"Message ID: {message_id} - Created new activity record for client_id={client_id}")
        
        self.db.commit()
    
    def get_clients_for_archiving(self, hours: int = 24) -> List[Dict[str, str]]:
        """Get clients that haven't been active for specified hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        activities = self.db.query(ClientLastActivity).filter(
            ClientLastActivity.last_message_at < cutoff_time
        ).all()
        
        return [
            {
                "project_id": activity.project_id,
                "client_id": activity.client_id,
                "last_message_at": activity.last_message_at.isoformat()
            }
            for activity in activities
        ]
    
    def has_pending_messages(self, project_id: str, client_id: str, message_id: str = None) -> bool:
        """
        Check if client has any pending messages in queue
        Used to determine correct send_status and count values
        """
        if message_id:
            logger.debug(f"Message ID: {message_id} - Checking for pending messages: project_id={project_id}, client_id={client_id}")
        else:
            logger.debug(f"Checking for pending messages: project_id={project_id}, client_id={client_id}")
        
        pending_count = self.db.query(MessageQueue).filter(
            and_(
                MessageQueue.project_id == project_id,
                MessageQueue.client_id == client_id,
                MessageQueue.status == MessageStatus.PENDING
            )
        ).count()
        
        has_pending = pending_count > 0
        if message_id:
            logger.debug(f"Message ID: {message_id} - Client {client_id} has {pending_count} pending messages")
        else:
            logger.debug(f"Client {client_id} has {pending_count} pending messages")
        return has_pending
    
    def check_if_message_superseded(self, queue_item_id: str, message_id: str = None) -> bool:
        """
        Check if a message was marked as superseded during processing
        This is used to coordinate send_status between concurrent webhook calls
        """
        if message_id:
            logger.debug(f"Message ID: {message_id} - Checking if queue_item {queue_item_id} was superseded")
        else:
            logger.debug(f"Checking if queue_item {queue_item_id} was superseded")
        
        message = self.db.query(MessageQueue).filter(MessageQueue.id == queue_item_id).first()
        if not message:
            if message_id:
                logger.warning(f"Message ID: {message_id} - Queue item {queue_item_id} not found when checking superseded status")
            else:
                logger.warning(f"Queue item {queue_item_id} not found when checking superseded status")
            return False
        
        is_superseded = message.status == MessageStatus.SUPERSEDED
        if message_id:
            logger.debug(f"Message ID: {message_id} - Queue item {queue_item_id} superseded status: {is_superseded}")
        else:
            logger.debug(f"Queue item {queue_item_id} superseded status: {is_superseded}")
        return is_superseded
    
    def try_claim_as_winner(self, project_id: str, client_id: str, queue_item_id: str, message_id: str = None) -> bool:
        """
        Atomically try to claim this message as the "winner" that should return send_status=TRUE.
        
        This method ensures that exactly one message per client gets send_status=TRUE by:
        1. Using database locks to prevent race conditions
        2. Checking for newer messages atomically
        3. Marking older messages as superseded if this wins
        
        Returns True if this message won and should return send_status=TRUE
        Returns False if this message was superseded and should return send_status=FALSE
        """
        if message_id:
            logger.debug(f"Message ID: {message_id} - Trying to claim winner status for queue_item {queue_item_id}, client_id={client_id}")
        else:
            logger.debug(f"Trying to claim winner status for queue_item {queue_item_id}, client_id={client_id}")
        
        try:
            # Get the current message first
            current_message = self.db.query(MessageQueue).filter(MessageQueue.id == queue_item_id).first()
            if not current_message:
                if message_id:
                    logger.warning(f"Message ID: {message_id} - Queue item {queue_item_id} not found during winner claim")
                else:
                    logger.warning(f"Queue item {queue_item_id} not found during winner claim")
                return False
            
            # Use SELECT FOR UPDATE to lock all messages for this client
            # This prevents race conditions between concurrent processing completions
            # CRITICAL: Include ALL statuses to find the truly latest message, even if it's already SUPERSEDED
            all_client_messages = self.db.query(MessageQueue).filter(
                and_(
                    MessageQueue.project_id == project_id,
                    MessageQueue.client_id == client_id,
                    MessageQueue.status.in_([
                        MessageStatus.PENDING, 
                        MessageStatus.PROCESSING, 
                        MessageStatus.COMPLETED,
                        MessageStatus.SUPERSEDED  # CRITICAL: Must include to find true latest
                    ])
                )
            ).with_for_update().all()
            
            # Find the latest message by creation time
            latest_message = None
            latest_time = None
            
            for msg in all_client_messages:
                if latest_time is None or msg.created_at > latest_time:
                    latest_time = msg.created_at
                    latest_message = msg
            
            # Check if current message is the latest
            if latest_message and latest_message.id == queue_item_id:
                # This is the latest message - it wins!
                if message_id:
                    logger.info(f"Message ID: {message_id} - Queue item {queue_item_id} is the latest message for client_id={client_id}, claiming winner status")
                else:
                    logger.info(f"Queue item {queue_item_id} is the latest message for client_id={client_id}, claiming winner status")
                
                # Mark all older messages as superseded (but don't touch messages that are already superseded)
                for msg in all_client_messages:
                    if msg.id != queue_item_id and msg.status in [MessageStatus.PENDING, MessageStatus.PROCESSING, MessageStatus.COMPLETED]:
                        msg.status = MessageStatus.SUPERSEDED
                        msg.updated_at = datetime.utcnow()
                        if message_id:
                            logger.debug(f"Message ID: {message_id} - Marked older message {msg.id} as superseded for client_id={client_id}")
                        else:
                            logger.debug(f"Marked older message {msg.id} as superseded for client_id={client_id}")
                
                # Ensure current message is marked as COMPLETED (winner status)
                if current_message.status != MessageStatus.COMPLETED:
                    current_message.status = MessageStatus.COMPLETED
                    current_message.updated_at = datetime.utcnow()
                
                self.db.commit()
                return True
            else:
                # This is not the latest message - it loses
                # Only mark as superseded if it's not already superseded
                if current_message.status != MessageStatus.SUPERSEDED:
                    current_message.status = MessageStatus.SUPERSEDED
                    current_message.updated_at = datetime.utcnow()
                
                if message_id:
                    logger.info(f"Message ID: {message_id} - Queue item {queue_item_id} is NOT the latest message for client_id={client_id} (latest: {latest_message.id if latest_message else 'none'}), marking as superseded")
                else:
                    logger.info(f"Queue item {queue_item_id} is NOT the latest message for client_id={client_id} (latest: {latest_message.id if latest_message else 'none'}), marking as superseded")
                
                self.db.commit()
                return False
                
        except Exception as e:
            if message_id:
                logger.error(f"Message ID: {message_id} - Error in winner claim for queue_item {queue_item_id}: {e}")
            else:
                logger.error(f"Error in winner claim for queue_item {queue_item_id}: {e}")
            self.db.rollback()
            # On error, assume not winner to be safe
            return False
    
    def get_queue_stats(self, project_id: str) -> Dict[str, int]:
        """Get queue statistics for a project"""
        stats = {}
        
        for status in MessageStatus:
            count = self.db.query(MessageQueue).filter(
                and_(
                    MessageQueue.project_id == project_id,
                    MessageQueue.status == status.value
                )
            ).count()
            stats[status.value] = count
        
        return stats 