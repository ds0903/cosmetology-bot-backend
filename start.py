#!/usr/bin/env python3
"""
Startup script for Telegram Bot Backend
This script initializes the database and starts the application
"""

import os
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def check_environment():
    """Check if all required environment variables are set"""
    from app.config import settings
    
    required_vars = [
        ('DATABASE_URL', settings.database_url),
        ('REDIS_URL', settings.redis_url),
        ('CLAUDE_API_KEY_1', settings.claude_api_key_1),
        ('CLAUDE_API_KEY_2', settings.claude_api_key_2)
    ]
    
    missing_vars = []
    placeholder_vars = []
    
    for var_name, var_value in required_vars:
        if not var_value or var_value == "":
            missing_vars.append(var_name)
        elif "your_" in str(var_value).lower() or "example" in str(var_value).lower():
            placeholder_vars.append(var_name)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file")
        return False
    
    if placeholder_vars:
        print("⚠️ Found placeholder values in environment variables:")
        for var in placeholder_vars:
            print(f"   - {var}")
        print("\nPlease replace placeholder values with real ones")
        return False
    
    print("✅ All required environment variables are set")
    print(f"✅ Claude model: {settings.claude_model}")
    print(f"✅ Debug mode: {settings.debug}")
    print(f"✅ Log level: {settings.log_level}")
    return True

def create_database_tables():
    """Create database tables if they don't exist"""
    try:
        from app.database import create_tables
        create_tables()
        print("✅ Database tables created successfully")
        return True
    except Exception as e:
        print(f"❌ Error creating database tables: {e}")
        return False

def test_connections():
    """Test connections to external services"""
    from app.config import settings
    
    print("🔍 Testing connections...")
    
    # Test database connection
    try:
        from app.database import engine
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Database connection successful")
        print(f"   📊 Pool size: {engine.pool.size()}")
        print(f"   🔧 Echo mode: {engine.echo}")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    # Test Redis connection
    try:
        import redis
        redis_client = redis.from_url(settings.redis_url)
        redis_client.ping()
        print("✅ Redis connection successful")
        print(f"   🔗 Redis URL: {settings.redis_url}")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print(f"   Make sure Redis is running on {settings.redis_url}")
        return False
    
    # Test Claude API
    try:
        import anthropic
        if settings.claude_api_key_1:
            # Don't actually call the API to avoid charges, just validate format
            if settings.claude_api_key_1.startswith('sk-ant-'):
                print("✅ Claude API key 1 format valid")
            else:
                print("⚠️ Claude API key 1 format may be invalid")
                
            if settings.claude_api_key_2.startswith('sk-ant-'):
                print("✅ Claude API key 2 format valid")
            else:
                print("⚠️ Claude API key 2 format may be invalid")
                
            print(f"   🤖 Model: {settings.claude_model}")
        else:
            print("⚠️ Claude API key not configured")
    except Exception as e:
        print(f"❌ Claude API test failed: {e}")
        return False
    
    # Test Google Sheets credentials
    try:
        if os.path.exists(settings.google_credentials_file):
            print("✅ Google credentials file found")
            print(f"   📄 File: {settings.google_credentials_file}")
            if settings.google_sheet_id:
                print(f"   📊 Sheet ID configured: {settings.google_sheet_id[:10]}...")
            else:
                print("⚠️ Google Sheet ID not configured")
        else:
            print("⚠️ Google credentials file not found")
            print(f"   Expected: {settings.google_credentials_file}")
    except Exception as e:
        print(f"❌ Google Sheets check failed: {e}")
    
    # Check business settings
    print(f"✅ Business hours: {settings.default_work_start_time} - {settings.default_work_end_time}")
    print(f"✅ Slot duration: {settings.slot_duration_minutes} minutes")
    print(f"✅ Max queue size: {settings.max_queue_size}")
    print(f"✅ Message retry attempts: {settings.message_retry_attempts}")
    
    return True

def start_application():
    """Start the FastAPI application"""
    try:
        import uvicorn
        from app.config import settings
        
        print(f"🚀 Starting application on {settings.host}:{settings.port}")
        print(f"📚 API documentation available at: http://{settings.host}:{settings.port}/docs")
        print(f"🔧 Health check endpoint: http://{settings.host}:{settings.port}/health")
        print(f"📊 Settings endpoint: http://{settings.host}:{settings.port}/projects/default/config")
        print("\nPress Ctrl+C to stop the application")
        
        # Additional startup info
        print(f"\n📋 Configuration summary:")
        print(f"   🐛 Debug mode: {settings.debug}")
        print(f"   📝 Log level: {settings.log_level}")
        print(f"   ⏱️ Processing timeout: {settings.message_processing_timeout}s")
        print(f"   🔒 Rate limiting: {settings.max_messages_per_minute}/min")
        print(f"   📁 Dialogue archive: {settings.dialogue_archive_hours}h")
        
        uvicorn.run(
            "main:app",
            host=settings.host,
            port=settings.port,
            reload=settings.debug,
            log_level=settings.log_level.lower(),
            access_log=settings.debug,
            use_colors=True
        )
        
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        return False
    
    return True

def show_startup_banner():
    """Show startup banner with system info"""
    from app.config import settings
    
    print("🤖 Telegram Bot Backend - Starting up...")
    print("=" * 50)
    print(f"📅 Version: 1.0.0")
    print(f"🐍 Python: {sys.version.split()[0]}")
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"🌐 Environment: {'Development' if settings.debug else 'Production'}")
    print("=" * 50)

def main():
    """Main startup function"""
    # Show banner
    show_startup_banner()
    
    # Check environment
    if not check_environment():
        print("\n💡 Tips:")
        print("   - Copy .env-example to .env and fill in your values")
        print("   - Run 'python check_settings.py' for detailed configuration check")
        print("   - Check the documentation for setup instructions")
        sys.exit(1)
    
    # Create database tables
    if not create_database_tables():
        print("\n💡 Database setup tips:")
        print("   - Make sure PostgreSQL is running")
        print("   - Check your DATABASE_URL in .env file")
        print("   - Ensure the database exists and user has permissions")
        sys.exit(1)
    
    # Test connections
    if not test_connections():
        print("\n⚠️ Some connections failed, but continuing anyway...")
        print("💡 Check the logs above for specific issues")
    
    # Start application
    print("\n" + "=" * 50)
    success = start_application()
    
    if not success:
        print("\n❌ Application failed to start")
        print("💡 Check the error messages above")
        print("💡 Run 'python check_settings.py' for configuration validation")
        sys.exit(1)

if __name__ == "__main__":
    main() 