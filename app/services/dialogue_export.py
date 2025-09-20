import logging
import os
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

class DialogueExporter:
    def __init__(self, project_name=None):
        self.drive_service = None
        self.docs_service = None
        self.project_name = "anna-paris-8013"  # Хардкод для этого проекта
        self.workspace_email = os.getenv('GOOGLE_WORKSPACE_EMAIL', '888999888nike@jarvis-dev.ai')
        self.folder_id = None
        
        credentials_file = os.getenv('GOOGLE_WORKSPACE_CREDENTIALS', 'credentials-workspace.json')
        if os.path.exists(credentials_file):
            logger.info(f"Initializing DialogueExporter for project {self.project_name}")
            self._init_services_with_impersonation(credentials_file)
    
    def _init_services_with_impersonation(self, credentials_file):
        """Инициализация с impersonation для Google Workspace"""
        try:
            SCOPES = [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/documents'
            ]
            
            creds = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=SCOPES
            )
            
            # Используем impersonation для создания документов от имени пользователя
            creds = creds.with_subject(self.workspace_email)
            
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.docs_service = build('docs', 'v1', credentials=creds)
            
            logger.info(f"Services initialized with impersonation as {self.workspace_email}")
            
        except Exception as e:
            logger.error(f"Failed to init services: {e}")
    
    def _ensure_project_folder(self):
        """Создает папку проекта если её нет"""
        try:
            query = f"name='{self.project_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            folders = results.get('files', [])
            
            if folders:
                folder_id = folders[0]['id']
                logger.info(f"Using existing folder: {folder_id}")
                return folder_id
            
            file_metadata = {
                'name': self.project_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            folder = self.drive_service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            logger.info(f"Created new folder: {folder_id}")
            return folder_id
            
        except Exception as e:
            logger.error(f"Error ensuring folder: {e}")
            return None
    
    async def save_dialogue_to_drive(self, client_id, client_name, booking_info, dialogue_history):
        """Создает Google Document с диалогом"""
        try:
            logger.info(f"Starting save_dialogue_to_drive for client {client_name}")
            
            if not self.drive_service or not self.docs_service:
                logger.error("Services not initialized")
                return None
            
            folder_id = self._ensure_project_folder()
            if not folder_id:
                logger.error("Failed to get/create folder")
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            doc_title = f"{client_name}_{booking_info.get('date')}_{timestamp}"
            
            document = self.docs_service.documents().create(
                body={'title': doc_title}
            ).execute()
            
            doc_id = document.get('documentId')
            logger.info(f"Created document: {doc_id}")
            
            # Формируем содержимое
            content = []
            content.append("ЗАПИСЬ КЛИЕНТА\n")
            content.append("="*50 + "\n\n")
            content.append(f"Клиент: {client_name}\n")
            content.append(f"WA-Inst ID: {client_id}\n")
            content.append(f"Дата: {booking_info.get('date')}\n")
            content.append(f"Время: {booking_info.get('time')}\n")
            content.append(f"Услуга: {booking_info.get('service')}\n")
            content.append(f"Специалист: {booking_info.get('specialist')}\n\n")
            content.append("="*50 + "\n")
            content.append("ИСТОРИЯ ДИАЛОГА\n")
            content.append("="*50 + "\n\n")
            
            for msg in dialogue_history:
                timestamp = msg.get('timestamp', '')
                if hasattr(timestamp, 'strftime'):
                    timestamp = timestamp.strftime("%d.%m %H:%M")
                role = "Клиент" if msg.get('role') == 'client' else "Бот"
                content.append(f"[{timestamp}] {role}: {msg.get('message', '')}\n\n")
            
            # Вставляем содержимое
            requests = [{
                'insertText': {
                    'location': {'index': 1},
                    'text': ''.join(content)
                }
            }]
            
            self.docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': requests}
            ).execute()
            
            # Перемещаем в папку
            file = self.drive_service.files().get(fileId=doc_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents', []))
            
            if previous_parents:
                self.drive_service.files().update(
                    fileId=doc_id,
                    addParents=folder_id,
                    removeParents=previous_parents,
                    fields='id, parents'
                ).execute()
            else:
                self.drive_service.files().update(
                    fileId=doc_id,
                    addParents=folder_id,
                    fields='id, parents'
                ).execute()
            
            logger.info(f"Document saved: https://docs.google.com/document/d/{doc_id}/edit")
            return doc_id
            
        except Exception as e:
            logger.error(f"ERROR in save_dialogue_to_drive: {str(e)}", exc_info=True)
            return None
