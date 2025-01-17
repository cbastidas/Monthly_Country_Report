import csv
import os
import time
from client import RestClient
from datetime import datetime, timedelta
import subprocess

#Variables to Control the execution of the Upload to Sheets Process
executed_first_part1 = False
executed_first_part2 = False

# Login credentials for DataForSEO API
username = "seo@neatplay.com"
password = "7cbf8facb2e8b41e"

# File paths
REPORTS_PATH = r"C:\Users\ChristianBastidasNie\Desktop\Monthly_Country_Report"

# List of 20 keywords (can be updated)
keywords = ["canlı skor", "maç sonuçları", "iddaa sonuçları", "canlı casino", "iddaa tahminleri", "bahis siteleri", "canlı sonuçlar", "canlı bahis", 
"casino siteleri", "casino", "rulet oyna", "rulet", "güvenilir bahis siteleri", "bahis", "canlı bahis siteleri", "slot oyna", "bonus veren bahis siteleri", 
"kaçak bahis siteleri", "en iyi bahis siteleri", "illegal bahis siteleri", "sweet bonanza"]

# List of 100+ brands (can be updated)
brands = ["nesine", "bilyoner", "iddaa", "bets10", "misli", "matbet", "jojobet", "tuttur", "1xbet", "mobilbahis", "vdcasino", "imajbet", 
"oley", "betboo", "tempobet", "perabet", "youwin", "grandbetting", "birebin", "betebet", "betpark", "restbet", "betpas", "pulibet", "betsat", 
"interbahis", "betcup", "milanobet", "betist", "sekabet", "dinamobet", "betvole", "süperbahis", "betnano", "superbetin", "piabet", "ngsbahis", 
"bahsegel", "casinomaxi", "supertotobet", "queenbet", "goldenbahis", "betper", "tumbet", "tipobet365", "celtabet", "jetbahis", "meritroyalbet", 
"klasbahis", "ultrabet", "betgaranti", "jokerbet", "slotbar", "kolaybet", "betkanyon", "casino metropol", "ilbet", "artemisbet", "truvabet", 
"pinbahis", "casinoslot", "asyabahis", "venusbet", "mariobet", "sportotobet", "modabet", "pokerklas", "betwinner", "vegabet", "trbet", "betmatik", 
"hilbet", "bahigo", "lunabet", "melbet", "anadolu casino", "gorabet", "betgram", "sultanbet", "bahis", "betticket", "marsbet", "betmoon", "bahisnow", 
"betlike", "makrobet", "cepbahis", "betbaba", "discount casino", "baymavi", "noktabet", "forvetbet", "princess", "rotabet", "ligobet", "hiltonbet", 
"gobahis", "tipico", "betadonis", "wonclub", "grandpashabet", "betcio", "tarafbet", "betsof", "sahabet", "merkur", "lotobet", "rexbet", "cratos slot", 
"onwin", "mostbet", "redwin", "ikimisli", "parimatch", "casibom", "timebet", "ofansifbet", "hovarda", "casinovale", "betexper", "betroad", "superbahis", 
"betwoon", "setrabet", "madridbet", "kralcasino", "matadorbet", "betturkey", "asper casino", "istanbulcasino", "bettilt", "maksibet", "lord casino", 
"betasus", "neyine", "intobet", "discountcasino", "jet bahis", "nasabet", "radabet", "kingbetting", "tambet", "casinopop", "casinopplus", "ruyabet", 
"betyap", "artemisbet", "masterbet", "xslot", "darkbet", "ajaxbet", "armabahis", "portobet", "betnef", "bethand", "betwild", "betsalvador", "fixbet", 
"betsilin", "palacebet", "1win", "bizbet", "galabet", "rekabet", "bayspin", "baywin", "hepabet", "casipol", "parmabet", "huhubet", "betaverse", 
"betocool", "betyap", "palmibet", "dynamobet", "privebet", "vidobet", "kolaybet", "twinplay", "betpipo", "alevcasino", "casinofast", "superbet", 
"spinco", "bizbet", "marsbahis"]

def main_menu():
    print("\nMain Menu:")
    print("1. Google Trends Report")
    print("2. Historical Search Volume Report")
    print("3. Topics & Queries Report")
    print("4. Exit")
    while True:
        choice = input("Enter your choice (1-4): ")
        if choice in ['1', '2', '3', '4']:
            return choice
        print("Invalid choice. Please try again.")

def google_trends_menu():
    print("\nGoogle Trends Report:")
    print("1. Keywords Report")
    print("2. Brands Report")
    while True:
        choice = input("Enter your choice (1-2): ")
        if choice in ['1', '2']:
            return choice
        print("Invalid choice. Please try again.")

def historical_search_volume_menu():
    print("\nHistorical Search Volume Report:")
    print("1. Keywords Report")
    print("2. Brands Report")
    while True:
        choice = input("Enter your choice (1-2): ")
        if choice in ['1', '2']:
            return choice
        print("Invalid choice. Please try again.")

def get_date_range():
    while True:
        try:
            print("\nEnter date range:")
            from_date = input("From date (YYYY-MM-DD): ")
            to_date = input("To date (YYYY-MM-DD): ")
            time.strptime(from_date, "%Y-%m-%d")
            time.strptime(to_date, "%Y-%m-%d")
            return from_date, to_date
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")

def get_date_range_ym():
    while True:
        try:
            print("\nEnter date range (YYYY-MM):")
            from_date = input("From date (YYYY-MM): ")
            to_date = input("To date (YYYY-MM): ")
            # Validate the date format
            datetime.strptime(from_date, "%Y-%m")
            datetime.strptime(to_date, "%Y-%m")
            return from_date, to_date
        except ValueError:
            print("Invalid date format. Please use YYYY-MM.")

def generate_google_trends_report(report_type, from_date, to_date):
    client = RestClient(username, password)
    
    if report_type == '1':
        report_items = keywords
        file_prefix = "Keywords"
    else:
        report_items = brands
        file_prefix = "Brands"
    save_path = os.path.join(REPORTS_PATH, f"GoogleTrends_{file_prefix}_Reports")
    file_name = f"{file_prefix}_{from_date}_{to_date}.csv"
    file_path = os.path.join(save_path, file_name)
    start_time = time.time()
    total_tasks = 0
    error_count = 0
    cost = 0
    retry_attempts = 6  # Number of retry attempts for transient errors

    # Generate a list of all months in the date range
    start_date = datetime.strptime(from_date, "%Y-%m-%d")
    end_date = datetime.strptime(to_date, "%Y-%m-%d")
    all_months = [start_date.replace(day=1) + timedelta(days=32 * i) for i in range((end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1)]
    all_months = [date.replace(day=1) for date in all_months]

    with open(file_path, 'w', encoding="utf-8-sig", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Keyword", "Value", "Date"])
        for keyword in report_items:
            post_data = {
                0: {
                    "location_name": "Turkiye",
                    "language_name": "Turkish",
                    "date_from": from_date,
                    "date_to": to_date,
                    "item_types": ["google_trends_graph"],
                    "keywords": [keyword]
                }
            }
            monthly_data = {month.strftime("%Y-%m"): (0, month.strftime("%Y-%m-%d")) for month in all_months}
            success = False
            for attempt in range(retry_attempts):
                try:
                    response = client.post("/v3/keywords_data/google_trends/explore/live", post_data)
                    total_tasks += 1
                    cost += 0.009
                    if response["status_code"] == 20000:
                        tasks = response.get("tasks", [])
                        if tasks and isinstance(tasks[0].get("result"), list) and len(tasks[0]["result"]) > 0:
                            items = tasks[0]["result"][0].get("items", [])
                            if items and isinstance(items[0].get("data"), list):
                                for data in items[0]["data"]:
                                    date = datetime.strptime(data["date_from"], "%Y-%m-%d")
                                    month_key = date.strftime("%Y-%m")
                                    if from_date <= data["date_from"] <= to_date:
                                        value = data.get("values", [0])[0] if data.get("values") else 0
                                        value = 0 if value is None else value  # Convert None to 0
                                        monthly_data[month_key] = (value, data["date_from"])
                        success = True
                        break
                    else:
                        print(f"Error for keyword {keyword}. Code: {response['status_code']} Message: {response['status_message']}")
                        error_count += 1
                        break
                except Exception as e:
                    print(f"An error occurred while processing keyword {keyword}: {str(e)}")
                    error_count += 1
                    time.sleep(1)  # Wait before retrying

            if not success:
                print(f"Failed to process keyword {keyword} after {retry_attempts} attempts.")

            # Write data for all months, using 0 for months with no data
            for month, (value, date_str) in sorted(monthly_data.items()):
                csvwriter.writerow([keyword, value, date_str])
                print(f"{keyword}, {value}, {date_str}")

    end_time = time.time()
    duration = end_time - start_time

    return file_path, duration, total_tasks, error_count, cost

def generate_historical_search_volume_report(report_type, from_date, to_date):
    client = RestClient(username, password)
    
    if report_type == '1':
        report_items = keywords
        file_prefix = "Keywords"
        save_path = os.path.join(REPORTS_PATH, "Historical_Search_Volume_Keywords_Report")
    else:
        report_items = brands
        file_prefix = "Brands"
        save_path = os.path.join(REPORTS_PATH, "Historical_Search_Volume_Brands_Report")

    file_name = f"{file_prefix}_{from_date}_{to_date}.csv"
    file_path = os.path.join(save_path, file_name)

    with open(file_path, 'w', encoding="utf-8-sig", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Keyword", "Search Volume", "Date"])

        post_data = [{
            "location_name": "Turkiye",
            "language_name": "Turkish",
            "include_serp_info": True,
            "keywords": report_items
        }]

        response = client.post("/v3/dataforseo_labs/google/historical_search_volume/live", post_data)

        if response["status_code"] == 20000:
            for task in response["tasks"]:
                for item in task["result"][0]["items"]:
                    keyword = item["keyword"]
                    for search in item["keyword_info"]["monthly_searches"]:
                        date = f"{search['year']}-{search['month']:02d}-01"
                        if from_date <= date <= to_date:
                            csvwriter.writerow([keyword, search["search_volume"], date])
                            print(f"{keyword}, {search['search_volume']}, {date}")
        else:
            print(f"Error. Code: {response['status_code']} Message: {response['status_message']}")

    print(f"\nReport saved: {file_path}")

def generate_topics_queries_report(from_date, to_date):
    client = RestClient(username, password)

    save_path = os.path.join(REPORTS_PATH, "Turkiye_Topics_Queries_Reports")
    file_name = f"Topics_Queries_{from_date}_{to_date}.csv"
    file_path = os.path.join(save_path, file_name)

    post_data = [{
        "location_name": "Turkiye",
        "language_name": "Turkish",
        "date_from": from_date,
        "date_to": to_date,
        "item_types": [
            "google_trends_topics_list",
            "google_trends_queries_list"
        ],
        "keywords": ["*"]         
    }]

    # Call API
    response = client.post("/v3/keywords_data/google_trends/explore/live", post_data)

    # Make sure we have a valid response
    status_code = response.get("status_code")
    if status_code != 20000:
        print(f"API error: {status_code} | {response.get('status_message')}")
        # If you want to bail out entirely:
        return file_path

    tasks = response.get("tasks", [])
    if not tasks:
        print("No tasks in the response.")
        return file_path

    with open(file_path, 'w', encoding="utf-8-sig", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write headers
        csvwriter.writerow(["Queries", "Value", "Topics", "Value"])

        # Loop over tasks
        for task in tasks:
            task_result = task.get("result", [])
            if not isinstance(task_result, list):
                continue

            # Prepare TOP data
            queries_top = []
            topics_top = []
            for r_item in task_result:
                items = r_item.get("items", [])
                for trend_item in items:
                    if trend_item["type"] == "google_trends_queries_list":
                        queries_top = trend_item.get("data", {}).get("top", [])
                    elif trend_item["type"] == "google_trends_topics_list":
                        topics_top = trend_item.get("data", {}).get("top", [])

            # Write TOP section
            csvwriter.writerow(["TOP", "", "TOP", ""])
            max_length = max(len(queries_top), len(topics_top))
            for i in range(max_length):
                query = queries_top[i]["query"] if i < len(queries_top) else ""
                query_value = queries_top[i].get("value", "") if i < len(queries_top) else ""
                topic = (
                    topics_top[i]["topic_title"] + f" - {topics_top[i]['topic_type']}"
                    if i < len(topics_top) and topics_top[i].get("topic_type")
                    else topics_top[i]["topic_title"]
                    if i < len(topics_top)
                    else ""
                )
                topic_value = topics_top[i].get("value", "") if i < len(topics_top) else ""
                csvwriter.writerow([query, query_value, topic, topic_value])

        # Prepare RISING data
        queries_rising = []
        topics_rising = []
        for r_item in task_result:
            items = r_item.get("items", [])
            for trend_item in items:
                if trend_item["type"] == "google_trends_queries_list":
                    queries_rising = trend_item.get("data", {}).get("rising", [])
                elif trend_item["type"] == "google_trends_topics_list":
                    topics_rising = trend_item.get("data", {}).get("rising", [])

        # Write RISING section
        csvwriter.writerow(["RISING", "", "RISING", ""])
        max_length = max(len(queries_rising), len(topics_rising))
        for i in range(max_length):
            query = queries_rising[i]["query"] if i < len(queries_rising) else ""
            query_value = queries_rising[i].get("value", "") if i < len(queries_rising) else ""
            topic = (
                topics_rising[i]["topic_title"] + f" - {topics_rising[i]['topic_type']}"
                if i < len(topics_rising) and topics_rising[i].get("topic_type")
                else topics_rising[i]["topic_title"]
                if i < len(topics_rising)
                else ""
            )
            topic_value = topics_rising[i].get("value", "") if i < len(topics_rising) else ""
            csvwriter.writerow([query, query_value, topic, topic_value])

    return file_path

def main():
    executed_first_part2 = False    
    executed_first_part1 = False
    while True:
        choice = main_menu()
        if choice == '1':
            executed_first_part1 = False
            report_type = google_trends_menu()
            from_date, to_date = get_date_range()  # Keep the original function for Google Trends
            
            confirmation = input("\nNote: This will cost money\nConfirm report Y/N: ").strip().lower()
            if confirmation != 'y':
                print("Report generation cancelled.")
                continue
            
            print("\nGenerating report...")
            file_path, duration, total_tasks, error_count, cost = generate_google_trends_report(report_type, from_date, to_date)
            print("\nGoogle Trends Report generated successfully.")
            print(f"\nFile saved: {file_path}")
            print(f"\nRun Time: {duration:.2f} seconds")
            print(f"Tasks Count: {total_tasks}")
            print(f"Task Error: {error_count}")
            print(f"Cost: ${cost:.4f}")

            #Upload Keywords Report to Sheets
            gtrends_kw_answer = input("Do you want to save Keywords Report in google Sheets? (y/n): ").strip().lower()
            if gtrends_kw_answer == 'y' and report_type == '1':
                try: 
                    subprocess.run(['python', 'C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\upload_googleTrends_keywords_report.py'], check=True)
                    executed_first_part1 = True
                except FileNotFoundError:
                    print("Script upload_googleTrends_keywords_report.py  was not found")
                except subprocess.CalledProcessError as e:
                    print(f"There was an error executing the script: {e}")
            else:
                print("Only generating the report")

            #Upload Brands Report to Sheets
            if not executed_first_part1:
                gtrends_br_answer = input("Do you want to save Brands Report in google Sheets? (y/n): ").strip().lower()
                if gtrends_br_answer == 'y' and report_type == '2':
                    try: 
                        subprocess.run(['python', 'C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\upload_googleTrends_brands_report.py'], check=True)
                    except FileNotFoundError:
                        print("Script was not found")
                    except subprocess.CalledProcessError as e:
                        print(f"There was an error executing the script: {e}")
                else:
                    print("Only generating the report")
        elif choice == '2':
            executed_first_part2 = False
            report_type = historical_search_volume_menu()
            from_date, to_date = get_date_range_ym()  # Use the new function for Historical Search Volume
            
            confirmation = input("\nNote: This will cost money\nConfirm report Y/N: ").strip().lower()
            if confirmation != 'y':
                print("Report generation cancelled.")
                continue
            print("\nGenerating report...")
            generate_historical_search_volume_report(report_type, from_date, to_date)

            #Upload historical Search Keywords Report to Sheets
            gtrends_hskw_answer = input("Do you want to save Historical Volume Keywords Report in google Sheets? (y/n): ").strip().lower()
            if gtrends_hskw_answer == 'y' and report_type == '1':
                try: 
                    subprocess.run(['python', 'C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\upload_googleTrends_Historical_keywords_report.py'], check=True)
                    executed_first_part2 = True
                except FileNotFoundError:
                    print("Script upload_googleTrends_Historical_keywords_report.py  was not found")
                except subprocess.CalledProcessError as e:
                    print(f"There was an error executing the script: {e}")
            else:
                print("Only generating the report")

            #Upload Historical Search Brands Report to Sheets
            if not executed_first_part2:
                gtrends_hsbr_answer = input("Do you want to save Historical Brands Report in google Sheets? (y/n): ").strip().lower()
                if gtrends_hsbr_answer == 'y' and report_type == '2':
                    try: 
                        subprocess.run(['python', 'C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\upload_googleTrends_Historical_brands_report.py'], check=True)
                    except FileNotFoundError:
                        print("Script upload_googleTrends_Historical_brands_report.py was not found")
                    except subprocess.CalledProcessError as e:
                        print(f"There was an error executing the script: {e}")
                else:
                    print("Only generating the report")
        
        #Topics and Queries Report        
        if choice == '3':
            from_date, to_date = get_date_range()  # Keep the original function for Google Trends
            
            confirmation = input("\nNote: This will cost money\nConfirm report Y/N: ").strip().lower()
            if confirmation != 'y':
                print("Report generation cancelled.")
                continue
            
            print("\nGenerating report...")
            generate_topics_queries_report(from_date, to_date)
            print("\nTurkiye Topics & Queries Report generated successfully.")

            #Upload Topics and queries to Sheets
            gtrends_tnq_answer = input("Do you want to save Topics and queries in google Sheets? (y/n): ").strip().lower()
            if gtrends_tnq_answer == 'y':
                try: 
                    subprocess.run(['python', 'C:\\Users\\ChristianBastidasNie\\Desktop\\Monthly_Country_Report\\Subir_Queries_and_Topics.py'], check=True)
                except FileNotFoundError:
                    print("Script Subir_Queries_and_Topics.py  was not found")
                except subprocess.CalledProcessError as e:
                    print(f"There was an error executing the script: {e}")
            else:
                print("Only generating the report")

        elif choice == '4':
            print("Exiting program. Goodbye!")
            break

if __name__ == "__main__":
    main()
