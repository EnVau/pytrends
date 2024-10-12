import time
import requests
import numpy as np
import pandas as pd
from pytrends.request import TrendReq
from pyairtable import Api

# Pandas-Einstellungen anpassen, um zukünftige Änderungen zuzulassen
pd.set_option('future.no_silent_downcasting', True)

# Verbindung zur Google Trends API mit den angegebenen Headern und Cookies
class CustomTrendReq(TrendReq):
    def __init__(self, *args, headers=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = headers or {}  # Wenn keine headers übergeben werden, wird ein leeres Dict verwendet

    def _get_data(self, url, method="get", trim_chars=0, **kwargs):
        kwargs['headers'] = self.headers  # Verwende die Instanzvariable self.headers
        return super()._get_data(url, method=method, trim_chars=trim_chars, **kwargs)

# Header, die von Google Trends verwendet werden
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-DE;q=0.8,en;q=0.7,en-US;q=0.6',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://trends.google.de',
    'priority': 'u=1, i',
    'referer': 'https://trends.google.de/trends/explore?geo=DE&hl=de',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-form-factors': '"Desktop"',
    'sec-ch-ua-full-version': '"128.0.6613.120"',
    'sec-ch-ua-full-version-list': '"Chromium";v="128.0.6613.120", "Not;A=Brand";v="24.0.0.0", "Google Chrome";v="128.0.6613.120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'x-client-data': 'CKq1yQEIjbbJAQiitskBCKmdygEIv+jKAQiTocsBCJr+zAEIhaDNAQjCrM4BCNaszgEI5K/OAQjCts4BCLy5zgEI2L3OAQjNv84BGPbJzQEYnbHOAQ==',
}

# Initialisiere die pytrends Verbindung mit den angepassten Headern
pytrends = CustomTrendReq(hl='de-DE', tz=360, headers=headers)

# Verbindung zu Airtable
AIRTABLE_API_TOKEN = "patCjfpIVYTl6d7NW.fc8e818a0e6507a79470d12a56d1051ad904f8e299eaef79c21d8bea5501752b"
BASE_ID = "appvbgUAxdYfk62Ry"
TABLE_NAME = "NewsPoolSeit11.09"

api = Api(AIRTABLE_API_TOKEN)
table = api.table(BASE_ID, TABLE_NAME)

print("Erfolgreich mit Airtable verbunden.")

# Funktion zum Abrufen des Suchvolumens mit Backoff bei Fehlern
def get_interest_over_time(keyword, retries=5, backoff_factor=60):
    for attempt in range(retries):
        try:
            pytrends.build_payload([keyword], cat=0, timeframe='now 7-d', geo='DE', gprop='')
            time.sleep(30)  # 30 Sekunden Wartezeit zwischen regulären Anfragen
            interest_over_time_df = pytrends.interest_over_time()

            if not interest_over_time_df.empty:
                latest_search_volume = interest_over_time_df[keyword].values[-1]
                return float(latest_search_volume) if latest_search_volume is not None else None
            else:
                return None
        except Exception as e:
            if "429" in str(e):
                print(f"Fehler 429: Zu viele Anfragen. Pausiere für {backoff_factor} Sekunden.")
                time.sleep(backoff_factor)
                backoff_factor *= 2  # Verdopple die Wartezeit bei jedem Fehler
            else:
                print(f"Fehler beim Abrufen des Suchvolumens für {keyword}: {e}")
                break
    return None  # Falls alle Versuche fehlschlagen

# Funktion zum Batch-Hinzufügen von Daten in Airtable
def batch_add_to_airtable(records):
    try:
        if records:
            response = table.batch_create(records)
            print(f"Erfolgreich {len(records)} Datensätze hinzugefügt.")
        else:
            print("Keine Daten zum Hinzufügen vorhanden.")
    except Exception as e:
        print(f"Fehler beim Hinzufügen von Daten: {e}")

# Funktion zum Verarbeiten der Trends und Sammeln der Daten
def process_trends():
    trending_searches_df = pytrends.trending_searches(pn='germany')
    print("Top 25 Trends:")
    print(trending_searches_df.head(25))

    # Liste zum Speichern der gesammelten Datensätze
    records_to_add = []

    for trend in trending_searches_df[0:25].values:
        trend_name = trend[0]

        # Abrufen des Suchvolumens
        search_volume = get_interest_over_time(trend_name)

        if search_volume is None:
            print(f"Kein Suchvolumen für {trend_name} gefunden.")
            continue

        # Daten für den aktuellen Trend sammeln
        record_data = {
            "Trend": trend_name,
            "Suchvolumen": search_volume
        }

        # Füge die Daten der Liste hinzu
        records_to_add.append(record_data)

        # Pause zwischen den Anfragen
        time.sleep(30)

    # Nach dem Sammeln aller Daten werden sie in einem Rutsch zu Airtable hinzugefügt
    batch_add_to_airtable(records_to_add)

# Prozess zur Verarbeitung der Trends wird einmalig gestartet
print("Starte den Prozess zur Verarbeitung der Trends...")
process_trends()
print("Prozess abgeschlossen.")
