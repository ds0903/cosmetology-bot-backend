import gspread
from google.oauth2 import service_account
from datetime import date

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(
    'credentials.json', scopes=SCOPES
)
gc = gspread.authorize(creds)

# Открываем таблицу
spreadsheet = gc.open_by_key("1chSraYNxqbnzMqExnTW5eZDcOhJgLxHNpf9wMJHeX-s")
print(f"Opened: {spreadsheet.title}")
print(f"Worksheets: {[ws.title for ws in spreadsheet.worksheets()]}")

# Пробуем лист Мастер
try:
    worksheet = spreadsheet.worksheet("Мастер")
    all_values = worksheet.get_all_values()
    print(f"\nRows in 'Мастер': {len(all_values)}")
    if all_values:
        print(f"Columns in first row: {len(all_values[0])}")
        print(f"First 3 rows:")
        for i, row in enumerate(all_values[:3]):
            print(f"Row {i}: {row}")
        
        # Ищем данные на сегодня
        target_date = date.today().strftime("%d.%m.%Y")
        print(f"\nLooking for date: {target_date}")
        for row in all_values[1:]:
            if len(row) > 1 and target_date in str(row[1]):
                print(f"Found: {row}")
except:
    print("Worksheet 'Мастер' not found!")
