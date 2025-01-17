import csv
import os
import gspread
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# Route to the CSV Folders
#CSV_FOLDER_PATH_BRANDS = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\GoogleTrends_Brands_Reports"
#CSV_FOLDER_PATH_KEYWORDS = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\GoogleTrends_Keywords_Reports"
CSV_FOLDER_PATH_BRAND_VOLUME = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\Historical_Search_Volume_Brands_Report"
#CSV_FOLDER_PATH_KEYWORD_VOLUME = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\Historical_Search_Volume_Keywords_Report"
#CSV_FOLDER_PATH_TURKEY_TRENDS_REPORT = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\GoogleTrends_Turkey_Trends_Reports"

# Google Sheet Credentials
GOOGLE_SHEETS_CREDENTIALS_FILE = "C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\monthly-country-report-new-ls-e604856374dd.json"
GOOGLE_SHEETS_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1QD1g_WzpCTzb1NPfisM__LGsOySLRTyrNZ-Yeel4bZk/edit?gid=1697474387#gid=1697474387"

# Google sheets tab's name
#BRAND_SHEET_NAME = "brand_historical_values"
#KEYWORD_SHEET_NAME = "keyword_historical_values"
BRAND_VOLUME_SHEET_NAME = "Historical_Search_Volume_Brands"
#KEYWORD_VOLUME_SHEET_NAME = "keyword_monthly_volume(to_add)"
#TURKEY_TRENDS_QUERIES_REPORT_SHEET_NAME = "trending_queries(Point2)"

# Function to find the most recently uploaded file
def get_most_recent_file(folder_path):
    try:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        most_recent_file = max(files, key=os.path.getmtime)
        print(f"Most recent file allocated: {most_recent_file}")
        return most_recent_file
    except Exception as e:
        print(f"Error finding the most recent file: {e}")
        return None

# Function to read and filter the CSV
def filter_csv_by_latest_date(csv_file_path, date_format):
    try:
        with open(csv_file_path, "r", encoding="utf-8-sig") as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Reed the header and avoid it

             # Filtering valid rows, exluding the ones after "Last Month's Data"
            rows = []
            for row in csv_reader:
                if len(row) >= 1 and row[0].strip() == "Last Month's Data":
                    break  # Ignore everything afterwards
                if len(row) >= 3 and row[2].strip():  # Only valid rows with data on the column 'Date'
                    rows.append(row)

            if not rows:
                raise ValueError("CSV file doesn't have any data on the column 'Date'.")

            # Convert Data to Strtime
            def parse_date(date_str):
                try:
                    return datetime.strptime(date_str, date_format)  # Specific format of the file
                except ValueError:
                    raise ValueError(f"Unknown date format: {date_str}")

            # Find the most recent date
            latest_date = max(parse_date(row[2]) for row in rows)

            # Filter rows by the latest date
            filtered_rows = [row for row in rows if parse_date(row[2]) == latest_date]

            # Convert the latest date to a more readable data
            latest_date_str = latest_date.strftime("%Y-%m-%d")
            print(f"Filtered {len(filtered_rows)} rows for that date {latest_date_str}.")
            return filtered_rows

    except Exception as e:
        print(f"Error filtering the CSV file: {e}")
        return []
# Function to calculate the previous month
def get_first_day_previous_month():
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    last_day_previous_month = first_day_this_month - timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)
    return first_day_previous_month.strftime("%d-%m-%Y")
        
# Function to upload data to Google Sheets
def upload_to_google_sheets(data, spreadsheet_url, sheet_name):
    try:
        # Configure credentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, scope)
        client = gspread.authorize(credentials)

        # Open the Google Sheets and the specific tab
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Find the next empty/available row
        next_row = len(worksheet.get_all_values()) + 1

        # Obtain the date of the first day previous month
        first_day_previous_month = get_first_day_previous_month()

        # Prepare data to be uploaded with the date now
        updated_data = [[row[0], row[1], first_day_previous_month] for row in data]

        # Prepare data to be uploaded with the date now
        updated_data = [
        [
            int(row[0]) if row[0].isdigit() else row[0],  # Convierte a entero si es un número
            float(row[1]) if row[1].replace('.', '', 1).isdigit() else row[1],  # Convierte a flotante si es un número
            datetime.strptime(first_day_previous_month, "%d-%m-%Y").strftime("%Y-%m-%d")  # Convertir fecha al formato ISO
        ]
        for row in data
        ]

        # Data Range to Update
        range_start = f"A{next_row}"
        range_end = f"C{next_row + len(updated_data) - 1}"
        range_to_update = f"{range_start}:{range_end}"

        # Write on Google sheets
        worksheet.update(range_to_update, updated_data)
        print(f"Data uploaded successfully for {first_day_previous_month}")

    except Exception as e:
        print(f"Error uploading the data to the Google Sheets: {e}")

# Main Function to process a folder
def process_csv_folder(folder_path, sheet_name, date_format):
    # Search the most recent file in the folder
    csv_file_path = get_most_recent_file(folder_path)
    if not csv_file_path or not os.path.exists(csv_file_path):
        print("Couldn't find any valid file or the file doesn't exist")
        return

    # Read and filter the CSV File
    filtered_data = filter_csv_by_latest_date(csv_file_path, date_format)

    if not filtered_data:
        print("No data to upload")
        return

    # Upload the filtered data to google Sheets
    upload_to_google_sheets(filtered_data, GOOGLE_SHEETS_SPREADSHEET_URL, sheet_name)

# Main Function
def main():
    # Google trends Brands Report 
   #print("Processing Google Trends Brands Report")
   #process_csv_folder(CSV_FOLDER_PATH_BRANDS, BRAND_SHEET_NAME, "%Y-%m-%d")

    # Procesar datos de Keywords
    #print("Processing Google Trends Keywords Report")
    #process_csv_folder(CSV_FOLDER_PATH_KEYWORDS, KEYWORD_SHEET_NAME, "%Y-%m-%d")

    # Procesar datos de Brand Monthly Volume
    print("Procesando datos de Brand Monthly Volume...")
    process_csv_folder(CSV_FOLDER_PATH_BRAND_VOLUME, BRAND_VOLUME_SHEET_NAME, "%Y-%m-%d")
#
    # # Procesar datos de Keyword Monthly Volume
    #print("Procesando datos de Keyword Monthly Volume...")
    #process_csv_folder(CSV_FOLDER_PATH_KEYWORD_VOLUME, KEYWORD_VOLUME_SHEET_NAME, "%Y-%m-%d")


if __name__ == "__main__":
    main()