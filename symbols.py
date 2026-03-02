import pandas as pd
import requests
import certifi
from io import StringIO

url = "https://public.fyers.in/sym_details/NSE_CM.csv"

resp = requests.get(url, verify=certifi.where())
resp.raise_for_status()

df = pd.read_csv(StringIO(resp.text), header=None)

# print("Columns:", df.columns.tolist())
print(df[[2,3,4,8,9,10,11,12]].head(20))
# print(df.head())