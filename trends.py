from pytrends.request import TrendReq

pytrends = TrendReq(hl='de-DE', tz=360)

# Realtime trending searches in Germany
trending_searches_df = pytrends.trending_searches(pn='germany')

# Print top 25 trends
print(trending_searches_df.head(25))

from pyairtable import Table



# Verbindung zu Airtable mit Token
AIRTABLE_API_TOKEN = "patCjfpIVYTl6d7NW.fc8e818a0e6507a79470d12a56d1051ad904f8e299eaef79c21d8bea5501752b"
BASE_ID = "appvbgUAxdYfk62Ry"
TABLE_NAME = "NewsPoolSeit11.09"

table = Table(AIRTABLE_API_TOKEN, BASE_ID, TABLE_NAME)

# Daten zur Tabelle hinzufügen
def add_to_airtable(record_data):
    table.create(record_data)

# Beispiel-Aufruf mit den gescrapten Daten
for trend in trending_searches_df[0:25].values:
    add_to_airtable({"Trend": trend[0]})