import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants
API_KEY = os.getenv('GNEWS_API_KEY', "")
MAX_NUM_REQUESTS = int(os.getenv('MAX_NUM_REQUESTS', 10))
MAX_ARTICLES_PER_REQUEST = int(os.getenv('MAX_ARTICLES_PER_REQUEST', 100))
START_DATE = os.getenv('START_DATE', '2020-01-01')  # Default is '2020-01-01'
START_DATE = datetime.strptime(START_DATE, '%Y-%m-%d') 

# List of relevant Indian news sources
indian_news_sources = [
    'The Times of India',
    'The Hindu',
    'Hindustan Times',
    'Indian Express',
    'Business Standard',
    'Economic Times',
    'India Today',
    'Zee News',
    'NDTV',
    'CNN News18',
    'Livemint'
]