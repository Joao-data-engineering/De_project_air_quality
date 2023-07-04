import requests
import json

def executeRestApi(verb, url):
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    if verb == "get":
      res = requests.get(url)
    else:
      res = requests.post(url)
  except Exception as e:
    return e
  if res != None and res.status_code == 200:
    return json.dumps(res.json())
  return None