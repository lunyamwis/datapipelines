import requests
import os
from dotenv import load_dotenv

load_dotenv()  # loads OPENAI_API_KEY from .env


VA_API_KEY = os.getenv('VA_API_KEY') # Replace with your API key
headers = {"Authorization": f"Basic {VA_API_KEY}"}

pdf_path = f'YOUR_PATH/TO/YOUR_PDF.pdf'

# Parse the document
parse_response = requests.post(
    url="https://api.va.landing.ai/v1/ade/parse",
    headers=headers,
    files=[("document", open(pdf_path, "rb"))],
    # The selected model defaults to its latest available version.
    # To specify an older version (e.g., 'model-name-date'), modify the model string.
    # Details on all supported model versions: [https://docs.landing.ai/ade/ade-parse-models#model-versions-and-snapshots]
    data={"model": "dpt-2"}
)

print(parse_response.json())
