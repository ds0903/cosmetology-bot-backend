# Cosmetology Bot Backend

FastAPI backend for Telegram bot that manages beauty salon appointments using AI (Claude) for natural language processing and Google Sheets for schedule management.

## 🎯 Features

- **AI-Powered Chat**: Uses Claude AI to understand natural language booking requests
- **Smart Scheduling**: Integrates with Google Sheets for real-time appointment management  
- **Message Queue**: Handles message flooding and aggregation for smooth operation
- **Multi-Project Support**: Scalable architecture for multiple salon locations
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Database Integration**: PostgreSQL for persistent data storage
- **Rate Limiting**: Built-in protection against spam and abuse

## 🏗️ Architecture

```
Telegram → SendPulse → FastAPI Backend → Claude AI
                           ↓
                    Google Sheets ← PostgreSQL Database
```

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd cosmetology-bot-backend
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux  
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy example configuration
cp .env-example .env

# Edit .env file with your settings
```

### 3. Set Required Variables

Edit `.env` file and set these **required** variables:

```env
# Database (Required)
DATABASE_URL=postgresql://username:password@localhost:5432/cosmetology_bot
REDIS_URL=redis://localhost:6379

# Claude AI (Required)  
CLAUDE_API_KEY_1=sk-ant-api03-your_key_here
CLAUDE_API_KEY_2=sk-ant-api03-your_backup_key_here
CLAUDE_MODEL=claude-sonnet-4-20250514

# Application
DEBUG=true
HOST=0.0.0.0
PORT=8000
SECRET_KEY=your_very_secure_secret_key_here
```

### 4. Setup External Services

#### PostgreSQL Database
```sql
CREATE DATABASE cosmetology_bot;
CREATE USER bot_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE cosmetology_bot TO bot_user;
```

#### Redis
- **Windows**: Download from [Redis releases](https://github.com/MicrosoftArchive/redis/releases)
- **Mac**: `brew install redis && brew services start redis`
- **Linux**: `sudo apt install redis-server && sudo systemctl start redis`

#### Claude AI API Keys
1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Create API keys in "API Keys" section
3. Add both keys to `.env` for load balancing

### 5. Start the Application

```bash
python start.py
```

The startup script will:
- ✅ Validate all configuration
- ✅ Create database tables
- ✅ Test all connections
- ✅ Start the FastAPI server

## 🔧 Configuration System

The application uses a centralized configuration system in `app/config.py` that loads settings from environment variables.

### Configuration Files

| File | Purpose |
|------|---------|
| `.env` | Your actual configuration (create from template) |
| `.env-example` | Template with all available settings |
| `app/config.py` | Configuration class definitions |
| `check_settings.py` | Configuration validation script |

### Settings Categories

#### 🗄️ Database Settings
```env
DATABASE_URL=postgresql://user:pass@localhost:5432/db_name
REDIS_URL=redis://localhost:6379
```

#### 🤖 Claude AI Settings
```env
CLAUDE_API_KEY_1=sk-ant-api03-your-primary-key
CLAUDE_API_KEY_2=sk-ant-api03-your-backup-key  
CLAUDE_MODEL=claude-sonnet-4-20250514
```

#### 📊 Google Sheets Settings (Optional)
```env
GOOGLE_CREDENTIALS_FILE=google-credentials.json
GOOGLE_SHEET_ID=your_sheet_id_from_url
```

#### 🔗 SendPulse Settings (Optional)
```env
SENDPULSE_WEBHOOK_SECRET=your_webhook_secret
SENDPULSE_API_TOKEN=your_api_token
```

#### ⚙️ Application Settings
```env
DEBUG=true                          # Enable debug mode
HOST=0.0.0.0                       # Server host
PORT=8000                          # Server port
LOG_LEVEL=INFO                     # Logging level
SECRET_KEY=your_secret_key         # Security key
```

#### 🏢 Business Settings
```env
DEFAULT_WORK_START_TIME=09:00      # Salon opening time
DEFAULT_WORK_END_TIME=18:00        # Salon closing time
SLOT_DURATION_MINUTES=30           # Appointment slot duration
```

#### 📬 Message Queue Settings
```env
MAX_QUEUE_SIZE=1000                # Maximum queue size
MESSAGE_RETRY_ATTEMPTS=3           # Retry failed messages
MESSAGE_PROCESSING_TIMEOUT=30      # Processing timeout (seconds)
```

#### 📁 Archive Settings  
```env
DIALOGUE_ARCHIVE_HOURS=24          # Archive old conversations
ARCHIVE_COMPRESSION_ENABLED=true   # Compress archived data
```

#### 🛡️ Security Settings
```env
MAX_MESSAGES_PER_MINUTE=60         # Rate limiting
FLOOD_PROTECTION_THRESHOLD=10      # Spam protection
```

## 🧪 Testing and Validation

### Validate Configuration
```bash
python check_settings.py
```

This script will:
- 📂 Check if `.env` file exists
- ⚙️ Validate all settings values
- 🔍 Test database and Redis connections  
- 🤖 Verify Claude AI configuration
- 📊 Check Google Sheets setup
- 📝 Generate new `.env` template if needed

### Test the API
```bash
# After starting the server
python test_webhook.py
```

### Check Database Status
```bash
python check_db.py
```

## 📁 Project Structure

```
cosmetology-bot-backend/
├── app/
│   ├── config.py              # ⚙️ Configuration management
│   ├── database.py            # 🗄️ Database models and connection
│   ├── models.py              # 📋 Pydantic models
│   ├── services/              # 🔧 Business logic services
│   │   ├── claude_service.py  # 🤖 AI conversation handling
│   │   ├── booking_service.py # 📅 Appointment management
│   │   ├── google_sheets.py   # 📊 Spreadsheet integration
│   │   ├── message_queue.py   # 📬 Message processing queue
│   │   └── sendpulse_service.py # 📱 Telegram integration
│   └── utils/
│       └── prompt_loader.py   # 💬 AI prompt management
├── main.py                    # 🚀 FastAPI application
├── start.py                   # 🏁 Startup script with validation
├── requirements.txt           # 📦 Python dependencies
├── prompts.yml               # 💬 AI conversation prompts
├── .env-example              # 📝 Configuration template
├── check_settings.py         # ✅ Configuration validator
├── test_webhook.py           # 🧪 API testing script
└── check_db.py              # 🗄️ Database status checker
```

## 🔗 API Endpoints

### Core Endpoints
- `POST /webhook/sendpulse` - Main webhook for incoming messages
- `GET /health` - Health check
- `GET /` - Basic status

### Project Management  
- `GET /projects/{project_id}/config` - Get project configuration
- `POST /projects/{project_id}/config` - Update project configuration
- `GET /projects/{project_id}/stats` - Get project statistics
- `GET /projects/{project_id}/queue` - Get message queue status

### Documentation
- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation

## 🐛 Troubleshooting

### Configuration Issues
```bash
# Check all settings
python check_settings.py

# View logs in real-time
tail -f app.log    # Linux/Mac
Get-Content app.log -Wait    # Windows PowerShell
```

### Common Problems

**1. Database Connection Failed**
```bash
# Check PostgreSQL status
sudo systemctl status postgresql    # Linux
brew services list | grep postgresql    # Mac

# Verify database exists
psql -U postgres -c "\l"
```

**2. Redis Connection Failed**  
```bash
# Check Redis status
redis-cli ping    # Should return "PONG"

# Start Redis if not running
sudo systemctl start redis    # Linux
brew services start redis    # Mac
```

**3. Claude API Errors**
- Check API keys are valid and have credits
- Verify model name is correct
- Check rate limits in Anthropic console

**4. Application Won't Start**
```bash
# Run with detailed validation
python start.py

# Check specific settings
python check_settings.py

# Manual database check
python check_db.py
```

## 📊 Monitoring

### Log Levels
- `DEBUG`: Detailed information for debugging
- `INFO`: General operational information  
- `WARNING`: Warning messages about potential issues
- `ERROR`: Error conditions that need attention

### Log Files
- `app.log`: Application logs with rotation
- Console output: Real-time logging during development

### Health Monitoring
```bash
# Check application health
curl http://localhost:8000/health

# Get project statistics  
curl http://localhost:8000/projects/default/stats

# Monitor message queue
curl http://localhost:8000/projects/default/queue
```

## 🔄 Development Workflow

### Making Configuration Changes
1. Update `.env` file with new values
2. Restart application: `python start.py`
3. Validate changes: `python check_settings.py`

### Adding New Settings
1. Add field to `Settings` class in `app/config.py`
2. Add to `.env-example` with example value
3. Update documentation
4. Use setting via `settings.new_field_name`

### Database Changes
1. Modify models in `app/database.py`
2. Restart application to create new tables
3. Use `check_db.py` to verify changes

## 📞 Support

### Getting Help
- 📖 Check this README for common solutions
- 🔧 Run `python check_settings.py` for configuration issues
- 📊 Use `python check_db.py` for database problems
- 📝 Check `app.log` for detailed error messages

### Reporting Issues
When reporting problems, include:
- Output from `python check_settings.py`
- Relevant logs from `app.log`
- Steps to reproduce the issue
- Environment details (OS, Python version)

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details. 