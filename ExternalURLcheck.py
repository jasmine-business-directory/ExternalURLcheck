from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests
from requests.exceptions import SSLError, Timeout
from retry import retry
from bs4 import BeautifulSoup
from random import randint

SCRAPEOPS_API_KEY = 'api-key-here'

def get_headers_list():
    response = requests.get(f'http://headers.scrapeops.io/v1/browser-headers?api_key={SCRAPEOPS_API_KEY}')
    json_response = response.json()
    return json_response.get('result', [])

def get_random_header(header_list):
    random_index = randint(0, len(header_list) - 1)
    return header_list[random_index]

header_list = get_headers_list()

@retry(Timeout, tries=2, delay=2, backoff=2)
def make_request(index, row):
    url = row['origurl']
    headers = get_random_header(header_list)
    try:
        response = requests.head(url, headers=headers, allow_redirects=False, timeout=5)
        status_code = response.status_code
        df.at[index, 'headerStatus'] = status_code
        
        if status_code == 200:
            response = requests.get(url, headers=headers, timeout=5)
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else 'No Title'
            df.at[index, 'OutputTitle'] = title
        elif status_code in [301, 302]:
            df.at[index, 'checkRedir'] = status_code
            df.at[index, 'ifRedirWhere'] = response.headers.get('Location', 'Unknown')
        elif status_code == 404:
            df.at[index, 'is404'] = 'YES'
        elif status_code == 403:
            df.at[index, 'isForb'] = 'YES'
        else:
            df.at[index, 'otherIssue'] = status_code
    except SSLError:
        df.at[index, 'invSSL'] = 'YES'
    except Timeout:
        df.at[index, 'headerStatus'] = 'Timeout'
    except Exception as e:
        df.at[index, 'headerStatus'] = 'Error'
        
    df.to_csv('listings_updated.csv', index=False)

# Load the CSV file into a DataFrame
df = pd.read_csv('urls.csv')

# Initialize new columns
df['headerStatus'] = ''
df['OutputTitle'] = ''
df['checkRedir'] = ''
df['ifRedirWhere'] = ''
df['is404'] = ''
df['isForb'] = ''
df['invSSL'] = ''
df['otherIssue'] = ''

# Use ThreadPoolExecutor to execute make_request function concurrently
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(make_request, df.index, df.to_dict('records'))

# DataFrame is saved within make_request, but you could save it here again if needed
# df.to_csv('url_results.csv', index=False)

