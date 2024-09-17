import time
import requests
import numpy as np
import pandas as pd
from pytrends.request import TrendReq
from pyairtable import Api

# Pandas-Einstellungen anpassen, um zukünftige Änderungen zuzulassen
pd.set_option('future.no_silent_downcasting', True)

# Cookies, die von Google Trends übernommen werden sollen
cookies = {
    '__utmc': '72944086',
    '__utma': '72944086.1047843693.1726049423.1726049433.1726152372.2',
    '__utmz': '72944086.1726152372.2.2.utmcsr=trends.google.de|utmccn=(referral)|utmcmd=referral|utmcct=/',
    '__utmt': '1',
    '__utmb': '72944086.2.10.1726152372',
    'CONSENT': 'YES+DE.de+20151018-17-1',
    'SEARCH_SAMESITE': 'CgQIrpsB',
    '__Secure-ENID': '20.SE=F4-dyh5HA4JUmS0I8YlFfGxCJ_S4wTR-n9Y_0qJx2QhwASV1QcsUJKoHUTf4lhOmaX4U8LDT5Cp7VX_sTetdcVAjLCZ7cpoTA2zRnWiw6DuYip7yI0CmAT-xq1yMU2rQkT7VuDX6dzdllBP6XgSPtS5U_iCW_YXvnVmhrPGxAU-WD7_zIXtnYDkx8PhwMu06Rts0uJ-CtE-2oz9IoR1qJFOz9YPxCzW-YQjaTdtzygffbgL2VCSELi7TzpeYK-s9hWCJXOA',
    'NID': '516=dSxKb1SQIOmzBksra2bo1C5UARQA485J9d0eEZZ9n44_PbbO1XPOTVoxnEsncLFD7S9Fio2XpxLt3FIy458Ly3xa0iuKiqbQr_ey9N5s-G2FUdr7q5DNEx2pKwsgb24OLUGOuONC9xv4pysFv7b2_LItbb94N2Xis80G7JF7DM1tnD4sz6ECHIPHlIS9YZD34PtxZxpBV5K0yByIipwZb5iEAcC1FpIsLmvyMGfcP3rfE8HJc3LB-5TJFvhms9-7UK0qe3xFH0MBGyufSiarPXit39Zf_uEDCGdRj9sfX8KR5_W2SI1RbJQPRgkToFVUDr4ZTM5RV1wLvEbzi6kkI2r3XbuPKu-IvUJYG7b3RGvN6SLyatn_bzfqFHLaZjkhMpF1ezUFe85K4QkmrXE1v6v-UIl2Hc7QhKXXLdPhZ_cQjsouRWnYNgFeo7H2DAKrWYQ6-T8KUEaytjeZi75XS8X9_U-D-yNEDLZ_jvWbTUYhbjwZbRcJgL_BuRSlsNaOPpRDTOlfrfxDSPQLA7DbzDja_cSu_4_8cOJg6Vjtqs29R8-PcSLRnjvDhMh0rX6_PNtSmfrGq1Ef2eu3fIcJZ8guldc3WNn4v3WFrHLxfR_Y3nZimSPKyfZvcjy8TLBomQpJoXRmuiVvFhWax9ONlFJB_LBdgzT7DBoQul2r24F7rTekaRM5umU0e9JYiuAYD2RtAnKGEZCsjf2LXoQERfd7KAFN89A89GHELPP370Tvtu3qVA',
}

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

# Verbindung zur Google Trends API mit den angegebenen Headern und Cookies
class CustomTrendReq(TrendReq):
    def _get_data(self, url, method="get", trim_chars=0, **kwargs):
        kwargs['headers'] = headers  # Hier sicherstellen, dass headers ein dict ist
        return super()._get_data(url, method=method, trim_chars=trim_chars, **kwargs)

pytrends = CustomTrendReq(hl='de-DE', tz=360)

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

# Funktion zum Hinzufügen von Daten in Airtable
def add_to_airtable(record_data):
    try:
        if "Suchvolumen" in record_data and record_data["Suchvolumen"] is not None:
            record_data["Suchvolumen"] = float(record_data["Suchvolumen"])

        response = table.create(record_data)
        print(f"Erfolgreich Daten hinzugefügt: {record_data}")
    except Exception as e:
        print(f"Fehler beim Hinzufügen von Daten: {e}")

# Funktion zum Verarbeiten der Trends
def process_trends():
    trending_searches_df = pytrends.trending_searches(pn='germany')
    print("Top 25 Trends:")
    print(trending_searches_df.head(25))

    for trend in trending_searches_df[0:25].values:
        trend_name = trend[0]

        # Abrufen des Suchvolumens
        search_volume = get_interest_over_time(trend_name)

        if search_volume == "429":
            continue  # Wenn ein 429-Fehler auftritt, wird nach der Pause weitergemacht
        elif search_volume is not None:
            add_to_airtable({"Trend": trend_name, "Suchvolumen": search_volume})
        else:
            print(f"Kein Suchvolumen für {trend_name} gefunden.")

        # Pause zwischen den Anfragen n
        time.sleep(30)

# Prozess zur Verarbeitung der Trends wird einmalig gestartet
print("Starte den Prozess zur Verarbeitung der Trends...")
process_trends()
print("Prozess abgeschlossen.")