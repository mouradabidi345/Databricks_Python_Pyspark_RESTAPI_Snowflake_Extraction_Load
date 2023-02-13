# Python 3
import math
import numpy as np
import http.client
from io import BytesIO
import pandas as pd
from io import StringIO
import sqlalchemy
import xlrd
import io
import datetime as dt
from pyspark.sql import SparkSession
import mimetypes
import base64
import json
#import keyring
import datetime
import csv
import sys
import pyodbc
import urllib.parse  #need to be installed in databricks
import jwt  #need to be installed in databricks
import snowflake.connector

Snowflake_Password=dbutils.secrets.get(scope="Blobstorage", key="Snowflake_Password")
Snowflake_User=dbutils.secrets.get(scope="Blobstorage", key="Snowflake_User")
# Credentials of the Legacy API ( The Central API)
cxone_vendor=dbutils.secrets.get(scope="Blobstorage", key="cxone_vendor")
cxone_business_unit_no=dbutils.secrets.get(scope="Blobstorage", key="cxone_business_unit_no")
cxone_password=dbutils.secrets.get(scope="Blobstorage", key="cxone_password")
cxone_application=dbutils.secrets.get(scope="Blobstorage", key="cxone_application")
cxone_username=dbutils.secrets.get(scope="Blobstorage", key="cxone_username")

#Credentials of the newest the API ( The Cxone Authentication API with the Backend Application Type)
Cxone_Discovery_Url=dbutils.secrets.get(scope="Blobstorage", key="Cxone_Discovery_Url")
Cxone_client_id=dbutils.secrets.get(scope="Blobstorage", key="Cxone_client_id")
Cxone_client_secret=dbutils.secrets.get(scope="Blobstorage", key="Cxone_client_secret")
Cxone_ACCESS_KEY_ID=dbutils.secrets.get(scope="Blobstorage", key="Cxone_ACCESS_KEY_ID")                                   
Cxone_SECRET_ACCESS_KEY=dbutils.secrets.get(scope="Blobstorage", key="Cxone_SECRET_ACCESS_KEY")                                  
Cxone_AWSALB=dbutils.secrets.get(scope="Blobstorage", key="Cxone_AWSALB")   

options2 = {
  "sfUrl": "https://ba62849.east-us-2.azure.snowflakecomputing.com/",
  "sfUser": Snowflake_User,
  "sfPassword": Snowflake_Password,
  "sfDatabase": "DB_RAW_DATA",
  "sfSchema": "ASSOCIATE_SUPPORT",
  "sfWarehouse": "COMPUTE_MACHINE",
  "truncate_table" : "ON",
  "usestagingtable" : "OFF"
}
 
  



def UrlParser(wholeUrl: str) -> dict:
  dblSlashIdx = wholeUrl.find('//')
  protocol = wholeUrl[: dblSlashIdx - 1]
  domainWithPath = wholeUrl[dblSlashIdx + 2:]
  sglSlashIdx = domainWithPath.find('/')
  domain = domainWithPath[:sglSlashIdx]
  path = domainWithPath[sglSlashIdx:]
  return {
    'protocol' : protocol,
    'domain' : domain,
    'path' : path,
  }

def Extract_Discovery_Output():
      Discovery_Url = Cxone_Discovery_Url
      Parsed_Discovery_Url_Domain = UrlParser(Discovery_Url)['domain']
      Parsed_Discovery_Url_Path = UrlParser(Discovery_Url)['path']

      print(Parsed_Discovery_Url_Domain)
      print(Parsed_Discovery_Url_Path)


      
      conn = http.client.HTTPSConnection(Parsed_Discovery_Url_Domain)
      payload = ''
      headers = {}
      conn.request("GET", Parsed_Discovery_Url_Path, payload, headers)
      res = conn.getresponse()
      print(res.getcode())
      data = res.read()
      #print(data.decode("utf-8"))
      data.decode("utf-8")
      data = json.loads(data)
      return data
      # token_endpoint = data['token_endpoint']
     #print(token_endpoint)




client_id =  Cxone_client_id
client_secret =  Cxone_client_secret

client_id_UrlEncoded = urllib.parse.quote_plus(client_id)
client_secret_UrlEncoded = urllib.parse.quote_plus(client_secret)



AUTHCODE = base64.b64encode(
    (client_id_UrlEncoded  + ":" + client_secret_UrlEncoded).encode()
  ).decode()



FILEPATH = "cxOneToken.json"
API_VERSION = "/v15.0/"
PROGRESS = {
  0: '|',
  1: '/',
  2: '-',
  3: '\\',
}

def CreateNewToken() -> dict:
  """
  Obtains a new token from cxOne, returns the values as a dict.
  """
  print("Generating new token...")
  
  ACCESS_KEY_ID = Cxone_ACCESS_KEY_ID
  username = urllib.parse.quote_plus(ACCESS_KEY_ID)

  SECRET_ACCESS_KEY = Cxone_SECRET_ACCESS_KEY
  password = urllib.parse.quote_plus(SECRET_ACCESS_KEY)

  # Get connection to token base URL
  
  
  Discovery_Output = Extract_Discovery_Output()
  token_Url = Discovery_Output['token_endpoint']
  Base_token_Url = UrlParser(token_Url)['domain']
  Path_token_url = UrlParser(token_Url)['path']
  
  conn = http.client.HTTPSConnection(Base_token_Url)
  

  

  # Create headers and payload
  headers = {
      "Authorization" : "Basic " + AUTHCODE,
      "Content-type" : "application/x-www-form-urlencoded"
      #"Cookie" : "BIGipServerpool_api=",
  }


  payload = f"grant_type=password&username={username}&password={password}"
  
  

  # Send request and process response
  
  
  conn.request("POST", Path_token_url, payload, headers)
  response = conn.getresponse()
  if response.getcode() != 200:
        error = "Error getting new token: " + response.getcode()
        error += "\nHeaders: " + response.getheaders()
        error += "\nBody: " + response.read()
        raise Exception(error)
  data = response.read()
  cxOneToken = ParseSaveTokenResponse(data)
  response.close()
  conn.close()
  return cxOneToken






def RetrieveCheckToken() -> dict:
  cxOneToken = {}
  # Open file that contains previous token. If the token is expired, 
  # CreateNewToken is called. 
  try:
    with open(FILEPATH, 'r') as f:
      cxOneToken = json.load(f)
      expirey = datetime.datetime.strptime(
        cxOneToken['expirey'], '%Y-%m-%d %H:%M:%S.%f')
      now = datetime.datetime.utcnow()
      if now > expirey:
          cxOneToken = CreateNewToken()
            
        # # Token is expired, now checking if refresh token is expired
        # refresh_expirey = expirey + datetime.timedelta(seconds = 3600)
        # if now > refresh_expirey:
        #   # Token and Refresh are both expired, will need to get a new token
        #   cxOneToken = CreateNewToken()
      else:
            # Refresh token is still good, so just needs to be refreshed.
            #cxOneToken = RefreshToken(cxOneToken)
            cxOneToken = CreateNewToken()
  except FileNotFoundError:
    cxOneToken = CreateNewToken()
  return cxOneToken

def ParseSaveTokenResponse(data: str) -> dict:
  """
  Receives response from either a token request or a token refresh in 
  a string. It saves the response as a json file, then returns a dictionary 
  """
  cxOneToken = json.loads(data)
  start = datetime.datetime.utcnow()
  # Set expiration to 5 seconds before to allow for at least 1 API call
  expirey = start + datetime.timedelta(seconds = cxOneToken['expires_in'] - 5)
  
  cxOneToken['start'] = start.strftime('%Y-%m-%d %H:%M:%S.%f')
  cxOneToken['expirey'] = expirey.strftime('%Y-%m-%d %H:%M:%S.%f')
  with open(FILEPATH, 'w') as f:
    json.dump(cxOneToken, f, indent = 4)
  return cxOneToken


def PrintProgress(text: str, endProgress = False) -> str:
  returnText = text
  text = ''
  while len(returnText) > 80:
    text += returnText[:80] + '\n'
    returnText = returnText[80:]
  text += '\r' + returnText
  if endProgress:
    text += '\n'
  sys.stdout.write(text)
  sys.stdout.flush()
  return returnText
  


def FinalEndPointDetermination():
      cxOneToken = RetrieveCheckToken()
      id_token = cxOneToken['id_token']
      return id_token


def StartReportingJob(reportId: str) -> str:
  """
  Starts a reporting job, returns jobId in string format. While the jobId
  appears to be an int, the return type remains a string in case this changes
  in the future.
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  #cxOneToken =CreateNewToken() 
  
  print("Starting job to run report number " + reportId + "...")
  
  
 

  id_token_encoded_jwt = FinalEndPointDetermination()
  #print(id_token_encoded_jwt)
  decoded_id_token_jwt = jwt.decode(id_token_encoded_jwt, options={"verify_signature": False})
  print(decoded_id_token_jwt)

  icDomain = decoded_id_token_jwt['icDomain']
  print(icDomain)
  icClusterId = decoded_id_token_jwt['icClusterId'].lower()
  print(icClusterId)


  
  

  resource_server_base_uri_path = "/inContactAPI/"   
  targetPath = resource_server_base_uri_path  + "services" + API_VERSION + "report-jobs/" + reportId

  

  BaseUrl = "api-" + icClusterId + "." + icDomain
  conn = http.client.HTTPSConnection(BaseUrl)


  filetype =  'CSV'
  includeHeaders = 'true'
  deleteAfter= 7
  
  #payload = 'filetype=CSV&includeHeaders=true&deleteAfter=7'
  payload = f"filetype={filetype}&includeHeaders={includeHeaders}&deleteAfter={deleteAfter}"

  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/x-www-form-urlencoded',
    "Cookie" : "AWSALB=Y8GCOf+CsOdQ6YWuAO7cSGi5hSgkQEe7yAAXYdAh3alv3L4k5I/zAMqFeoJ0M4z9PUsn+Py2B4K0gcpsQAKJYV2QfYX/7Xg8ljYTKLvgwKYsFda+06GoR+WN41aU; AWSALBCORS=Y8GCOf+CsOdQ6YWuAO7cSGi5hSgkQEe7yAAXYdAh3alv3L4k5I/zAMqFeoJ0M4z9PUsn+Py2B4K0gcpsQAKJYV2QfYX/7Xg8ljYTKLvgwKYsFda+06GoR+WN41aU",
     "Accept" : "*/*",
     'Connection' : 'keep-alive'
  }
  conn.request("POST", targetPath, payload, headers)
  

  response = conn.getresponse()
  
  data = response.read()
  data.decode("utf-8")
  
  data1 = json.loads(data)
  
  return data1['jobId']



def GetReportingJobInfo(jobId: str) -> dict:
  """
  Keeps checking running job every 1/10th second until either it returns the report URL or
  it fails. Times out after 10 minutes
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  #cxOneToken =CreateNewToken()
  print("Checking job status...")
  

  
  

  
  
  resource_server_base_uri_path = "/inContactAPI/"   
  targetPath = resource_server_base_uri_path  + "services" + API_VERSION + "report-jobs/" + jobId

  id_token_encoded_jwt = FinalEndPointDetermination()
  decoded_id_token_jwt = jwt.decode(id_token_encoded_jwt, options={"verify_signature": False})
  icDomain = decoded_id_token_jwt['icDomain']
  icClusterId = decoded_id_token_jwt['icClusterId'].lower()
  

  #conn = http.client.HTTPSConnection(parsedUrl['domain'])
  
  
  BaseUrl = "api-" + icClusterId + "." + icDomain
  conn = http.client.HTTPSConnection(BaseUrl)
  


  payload = {}
  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
  }
  jobState = ''
  data = {}
  statusCode = 200
  now = start = datetime.datetime.utcnow()
  timeout = start + datetime.timedelta(minutes = 5)
  outNum = 0
  print("Time\t\t\tStatus Code\t\tJob State")
  while jobState != "Finished" \
    and statusCode < 300 \
    and statusCode >= 200 \
    and now < timeout:
    # First, print out the progress animation, then add 1 the progress number
    PrintProgress(PROGRESS.get(outNum))
    outNum += 1
    outNum = outNum % 4
    
    conn.request("GET", targetPath, payload, headers)

    

    response = conn.getresponse()
    data = response.read()
    data = json.loads(data)
    if jobState != data['jobResult']['state']:
      jobState = data['jobResult']['state']
      consoleStr = now.strftime('%Y-%m-%d %H:%M:%S') + '\t'
      consoleStr += str(response.getcode()) + '\t\t\t' + jobState
      print('\r' + consoleStr)
    # time.sleep(.1)
    now = datetime.datetime.utcnow()
  print("Job Completed")
  return data['jobResult']

def GetFinishedReport(jobResult: dict) -> pd.DataFrame:
  """
  Downloads the report that was just generated. Puts the file into a pandas dataframe,
  removes null columns, and saves the file to the disk. Returns a dataframe that then can
  be used to insert rows into database.
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  #cxOneToken = CreateNewToken()
  print("Retrieving File...")
  parsedUrl = UrlParser(jobResult['resultFileURL'])
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  payload = ''
  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
    }
  conn.request("GET", parsedUrl['path'], payload, headers)
  response = conn.getresponse()
  #pp.pprint(res.getheaders())
  rawData = response.read()
  rawData = json.loads(rawData)
  fileName = rawData['files']['fileName'].replace(' ', '_')
  rawFile = rawData['files']['file']
  rawFile = rawFile.encode()
  rawFile = base64.decodebytes(rawFile)
  rawFile = rawFile.decode('utf-8-sig')
  rawFile = rawFile.replace('\r\n\r\n', '\r\n').replace('\r\n', '\n')
  rawFile = rawFile[:-1]
  rawFile = rawFile[:rawFile.rfind('\n')]
  rawFile = StringIO(rawFile)
  fileDf = pd.read_csv(rawFile)
  fileDf = fileDf.dropna(axis = 'columns', how = 'all')
  fileDf.columns = fileDf.columns.str.replace(' ', '_')
  d= datetime.datetime.today()
  sun_offset = (d.weekday() - 6) % 7
  sunday_same_week = d - datetime.timedelta(days=sun_offset)
  td = datetime.timedelta(days=7)
  Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
  fileDf.insert(0,"Report_Start_Date",Report_Start_Date)
  fileNameList = fileName.split("_")
  fileNameList[-1] = (sunday_same_week - td).strftime("%Y-%m-%d.csv")
  fileName = '_'.join(fileNameList)
  with open(fileName, 'w') as f:
    fileDf.to_csv(f, line_terminator = '\n', index = False)
  print("File retrieval success")
  return fileDf

  

  

if __name__ == "__main__":
  reportId = '1100'
  jobId = StartReportingJob(reportId)
  print("Job started, Job ID: " + jobId)
  jobResult = GetReportingJobInfo(jobId)
  print(jobResult)
  jobResult['fileName'] = jobResult['fileName'].replace(" ", "_")
  print("Filename: " + jobResult['fileName'])
  fileDf = GetFinishedReport(jobResult)
  fileDf.columns = map(str.upper, fileDf.columns)
  #print(fileDf['DATE'])
  fileDf.columns = fileDf.columns.str.replace(' ', '_')
  fileDf.insert(0, 'LOAD_DATE', pd.to_datetime('now').replace(microsecond=0))
  print(fileDf.dtypes['LOAD_DATE'])
  
  
  fileDf = fileDf[fileDf['AGENT_NAME'].notna()]
  
  NamesList = fileDf['AGENT_NAME'].tolist()
  
  l1 = []
  for i in NamesList:
        indexes = [x for x, v in enumerate(i) if v == ',']
        if len(indexes) > 1:
              Position = indexes[1]
              new_character = ' '
              i1 = i[:Position] + new_character + i[Position+1:]
              l1.append(i1)
        else:
          i1 = i
          l1.append(i1)      
  print(l1)
       
  fileDf['AGENT_NAME'] = l1
  print(fileDf['AGENT_NAME'])
  
  print(fileDf) 
  fileDf.insert(4, "MEDIA_TYPE_NAME", "") 
  print(fileDf)
  fileDf1 = fileDf[["LOAD_DATE","REPORT_START_DATE","AGENT_NAME", "DIRECTION","MEDIA_TYPE_NAME", "CONTACT_ID","HOLD_TIME", "HOLDS"]]
  


  spark = SparkSession.builder.appName(
     "pandas to spark").getOrCreate()

  #SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  df_spark = spark.createDataFrame(fileDf1)
  df_spark.write.format("net.snowflake.spark.snowflake") \
  .options(**options2) \
  .option("dbtable", "WFM_WEEKLY_CXONE_RAW_DATA_HOLD_TIME") \
  .mode('append') \
  .options(header=False) \
  .save()