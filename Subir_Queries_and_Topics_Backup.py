import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# Configura el acceso a Google Sheets
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = r"C:\Users\ChristianBastidasNie\Desktop\Monthly_Country_Report\monthly-country-report-new-ls-e604856374dd.json" # Asegúrate de que este archivo esté en tu directorio de trabajo
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1QD1g_WzpCTzb1NPfisM__LGsOySLRTyrNZ-Yeel4bZk/edit?gid=920578736#gid=920578736'

TRENDING_QUERIES_SHEET_NAME = 'trending_queries(Point2)'
TRENDING_TOPICS_SHEET_NAME = 'trending_topics(Point2)'
# Ruta del directorio
folder_path = r"C:\Users\ChristianBastidasNie\Desktop\Monthly_Country_Report\GoogleTrends_Turkey_Trends_Reports"

# Encuentra el archivo más reciente en la carpeta
def get_latest_file(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(folder_path, f)))
    return os.path.join(folder_path, latest_file)

# Lee el archivo CSV más reciente
latest_file = get_latest_file(folder_path)
data = pd.read_csv(latest_file, header=0)  # Mantener todas las filas pero considerar encabezados

# Imprime los nombres de las columnas para verificar
print("Columnas en el archivo:", data.columns.tolist())

# Ajusta los nombres de las columnas según el archivo CSV
column_a = 'Queries'  # Primera columna
column_b = 'Value'  # Segunda columna
column_c = 'Topics'  # Tercera columna
column_d = 'Value.1'  # Cuarta columna

# Procesamiento de datos
# Ignorar las dos primeras filas al preparar datos para Google Sheets
def process_columns(data, col1, col2):
    # Verifica si las columnas existen
    if col1 not in data.columns or col2 not in data.columns:
        raise KeyError(f"Las columnas {col1} y/o {col2} no existen en el archivo.")

    # Ignoramos las dos primeras filas para el análisis de datos
    data_filtered = data.iloc[2:].reset_index(drop=True)

    # Busca la fila con "RISING"
    rising_index = data_filtered[data_filtered[col1] == 'RISING'].index[0]

    # Divide los datos en dos partes
    before_rising = data_filtered.iloc[:rising_index, [data_filtered.columns.get_loc(col1), data_filtered.columns.get_loc(col2)]].fillna('')
    after_rising = data_filtered.iloc[rising_index + 1:, [data_filtered.columns.get_loc(col1), data_filtered.columns.get_loc(col2)]].fillna('')

    return before_rising, after_rising

# Procesamos las columnas dinámicamente
before_ab, after_ab = process_columns(data, column_a, column_b)
before_cd, after_cd = process_columns(data, column_c, column_d)

# Subir datos a Google Sheets
# Escritura en lotes para evitar exceder el límite

def upload_to_google_sheets(sheet_url, tab_name, data, start_col):
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url)
    worksheet = sheet.worksheet(tab_name)

    # Determinar la última fila ocupada en la columna A
    last_row = max(len(worksheet.col_values(1)), 2)  # Columna A como referencia, mínimo fila 3
    start_row = last_row + 1  # Primera fila vacía después de los datos existentes

    # Crear rango completo y subir de una vez
    cell_range = f'{start_col}{start_row}:{chr(ord(start_col) + data.shape[1] - 1)}{start_row + len(data) - 1}'
    worksheet.update(cell_range, data.values.tolist())
    time.sleep(2)  # Espera para evitar exceder la cuota

# Subimos los datos en lotes automáticamente en las últimas filas disponibles
upload_to_google_sheets(SPREADSHEET_URL, 'trending_queries(Point2)', before_ab, 'B')
upload_to_google_sheets(SPREADSHEET_URL, 'trending_queries(Point2)', after_ab, 'D')
upload_to_google_sheets(SPREADSHEET_URL, 'trending_topics(Point2)', before_cd, 'B')
upload_to_google_sheets(SPREADSHEET_URL, 'trending_topics(Point2)', after_cd, 'D')

print("Datos subidos exitosamente.")

