#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import requests
import pandas as pd
import numpy as np
from datetime import timedelta, date
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip install webdriver-manager


# In[ ]:


from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# In[ ]:


config = pd.read_csv("config.csv")
exception_found = False


# In[ ]:


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/gmail.compose',
         'https://www.googleapis.com/auth/gmail.send']

# Gmail scopes: https://developers.google.com/identity/protocols/oauth2/scopes#gmail
# Sheets scopes: https://developers.google.com/identity/protocols/oauth2/scopes#sheets

def build_serv():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    sheets = build('sheets', 'v4', credentials=creds)
    mail = build('gmail', 'v1', credentials=creds)
    service = {'sheets': sheets,
              'mail': mail}
    return(service)

service = build_serv()
mail_service = service['mail']
sheets_service = service['sheets']


# In[ ]:


from apiclient import errors
from httplib2 import Http
from email.mime.text import MIMEText
import base64
from google.oauth2 import service_account

def create_message(sender, to, subject, message_text):
    """Create a message for an email.
    Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.
    Returns:
    An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

def send_message(service, user_id, message):
    """Send an email message.
    Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.
    Returns:
    Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                   .execute())
        print('Message Id: %s' % message['id'])
        return message
    except errors.HttpError as error:
        print('An error occurred: %s' % error)

def send_error(email, error_msg, mail_service):
    EMAIL_FROM = email
    EMAIL_TO = email
    EMAIL_SUBJECT = 'CT COVID-19 Error Found'
    EMAIL_CONTENT = error_msg
    # Call the Gmail API
    message = create_message(EMAIL_FROM, EMAIL_TO, EMAIL_SUBJECT, EMAIL_CONTENT)
    sent = send_message(mail_service,'me', message)
    
def read_data(service, spreadsheet, sheet_range):
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet, 
                                range=sheet_range).execute()
    values = result.get('values', [])
    return(values)

def send_data(service, dat, spreadsheet_id, sheet_range):
    send_dat = dat.copy()
#     send_dat[date_col] = send_dat[date_col].dt.strftime('%Y-%m-%d')
    send_dat.fillna('', inplace=True)
    
    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # USER_ENTERED to mimic Sheets entry, RAW to place data as-is

    # How the input data should be inserted.
    insert_data_option = 'OVERWRITE'  # not being used at the moment, function overwrites by default

    dfHeaders = send_dat.columns.values.tolist()
    dfHeadersArray = [dfHeaders]
    dfData = send_dat.values.tolist()

    value_range_body = {
            "majorDimension": "ROWS",
            "values": dfHeadersArray + dfData
        }

    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, 
                                                     range=sheet_range, 
#                                                      insertDataOption=insert_data_option,
                                                     valueInputOption=value_input_option, 
                                                     body=value_range_body)
    response = request.execute()

    return(response)


# In[ ]:


if not exception_found:
    try:
        resp = requests.get('https://data.ct.gov/resource/rf3k-f8fg.json')
        txt = resp.json()
        ct_last_update = pd.to_datetime(resp.headers['Last-Modified']).strftime('%Y-%m-%d')
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 3: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        meta_data = read_data(sheets_service, config['data_sheet'][0], 'meta')
        meta_data = pd.DataFrame(meta_data)
        meta_header = meta_data.iloc[0] #grab the first row for the header
        meta_data = meta_data[1:] #take the data less the header row
        meta_data.columns = meta_header
        if pd.to_datetime(ct_last_update) > pd.to_datetime(meta_data['ct_last_update'][1]):
            exception_found = False
        else:
            exception_found = True
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 3: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# exception_found = False
# exception_found


# In[ ]:


if not exception_found:
    try:
        dat_orig = pd.DataFrame(txt)
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 4: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        dat = dat_orig[dat_orig['state']=='CONNECTICUT'].copy()

        dat = dat[['date', 'totalcases', 'totaldeaths', 'cases_age0_9', 'covid_19_pcr_tests_reported']]

        cols = dat.columns.drop('date')

        dat[cols] = dat[cols].apply(pd.to_numeric, errors='coerce')

        dat['date'] = dat['date'].apply(pd.to_datetime).dt.floor("D")
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 5: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        sort_dat = dat.sort_values(by=['date'], inplace=False, ascending=True).copy()
        sort_dat['daily_new_cases'] = sort_dat['totalcases'].diff()
        sort_dat['daily_new_deaths'] = sort_dat['totaldeaths'].diff()
        sort_dat['daily_new_child_cases'] = sort_dat['cases_age0_9'].diff()
        sort_dat['daily_new_tests'] = sort_dat['covid_19_pcr_tests_reported'].diff()
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 6: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        # sort_dat['new_cases_roll3'] = sort_dat['daily_new_cases'].rolling(3).mean().round()
        sort_dat['rolling_new_cases'] = sort_dat['daily_new_cases'].rolling(7).mean().round()
        # sort_dat['new__child_cases_roll3'] = sort_dat['daily_new_child_cases'].rolling(3).mean().round()
        sort_dat['rolling_new_child_cases'] = sort_dat['daily_new_child_cases'].rolling(7).mean().round()
        # sort_dat['new_deaths_roll3'] = sort_dat['daily_new_deaths'].rolling(3).mean().round()
        sort_dat['rolling_new_deaths'] = sort_dat['daily_new_deaths'].rolling(7).mean().round()
        #Add positive test rate
        sort_dat['positive_test_rate'] = sort_dat['daily_new_cases']/sort_dat['daily_new_tests']
        sort_dat['rolling_test_rate'] = sort_dat['positive_test_rate'].rolling(7).mean().round(4)
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 7: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# Modeling

if not exception_found:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    import warnings
    from statsmodels.tools.sm_exceptions import ConvergenceWarning
    warnings.simplefilter('ignore', ConvergenceWarning)


    try:
        ignore_first_obs = 7
        test_obs = 10
        forecast_len = 7

        train = sort_dat.set_index('date').iloc[ignore_first_obs:-test_obs, :]
        test = sort_dat.set_index('date').iloc[-test_obs:, :]
        forecast_range = pd.DataFrame(index = pd.date_range(sort_dat.iloc[0]['date'],
                                       sort_dat.iloc[-1]['date'] + timedelta(days=forecast_len), 
                                       freq='D'))

        extended_dat = pd.merge(forecast_range, 
                                sort_dat.set_index('date'), 
                                left_index = True, 
                                right_index = True, 
                                how = 'left').copy()

    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 8: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        model_test_rate = ExponentialSmoothing(np.asarray(train['positive_test_rate'].clip(0.0000000000001)), trend='multiplicative', seasonal=None)

        #Not sure if this is needed...broke the code
        # model._index = pd.to_datetime(train.index)

        test_rate_fit = model_test_rate.fit()
        test_rate_pred = test_rate_fit.forecast(test_obs + forecast_len)

        # Dampned Trend Fit
        # model2 = ExponentialSmoothing(np.asarray(train['positive_test_rate']), trend='mul', seasonal=None, damped=True)
        # fit2 = model2.fit()
        # pred2 = fit2.forecast(10)

        model_deaths = ExponentialSmoothing(np.asarray(train['daily_new_deaths'].clip(0.0000000000001)), trend='multiplicative', seasonal=None)

        test_deaths_fit = model_deaths.fit()
        test_deaths_pred = test_deaths_fit.forecast(test_obs + forecast_len)

        model_cases = ExponentialSmoothing(np.asarray(train['daily_new_cases'].clip(0.0000000000001)), trend='multiplicative', seasonal=None)

        test_cases_fit = model_cases.fit()
        test_cases_pred = test_cases_fit.forecast(test_obs + forecast_len)

        model_child_cases = ExponentialSmoothing(np.asarray(train['daily_new_child_cases'].clip(0.0000000000001)), trend='multiplicative', seasonal=None)

        test_child_cases_fit = model_child_cases.fit()
        test_child_cases_pred = test_child_cases_fit.forecast(test_obs + forecast_len)

    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 9: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    try:
        extended_dat['modeled_test_rate'] = np.append(np.append(np.repeat(np.nan, ignore_first_obs), 
                                                                test_rate_fit.fittedvalues.round(4)), 
                                                      test_rate_pred.round(4)).tolist()

        extended_dat['modeled_deaths'] = np.append(np.append(np.repeat(np.nan, ignore_first_obs), 
                                                                test_deaths_fit.fittedvalues.round(4)), 
                                                      test_deaths_pred.round(4)).tolist()

        extended_dat['modeled_cases'] = np.append(np.append(np.repeat(np.nan, ignore_first_obs), 
                                                                test_cases_fit.fittedvalues.round(4)), 
                                                      test_cases_pred.round(4)).tolist()

        extended_dat['modeled_child_cases'] = np.append(np.append(np.repeat(np.nan, ignore_first_obs), 
                                                                test_child_cases_fit.fittedvalues.round(4)), 
                                                      test_child_cases_pred.round(4)).tolist()

        extended_dat['date'] = extended_dat.index
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 10: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# Get Current Projection Data
if not exception_found:
    try:
        mit = read_data(sheets_service, config['data_sheet'][0], 'projections')
        mit = pd.DataFrame(mit)
        mit_header = mit.iloc[0] #grab the first row for the header
        mit = mit[1:] #take the data less the header row
        mit.columns = mit_header
        mit['Day'] = mit['Day'].apply(pd.to_datetime).dt.floor("D")
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 11: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# Get Latest Projection Data
warnings.simplefilter('ignore', FutureWarning)

# mit_new = pd.read_csv('https://raw.githubusercontent.com/COVIDAnalytics/DELPHI/master/predicted/Global.csv', error_bad_lines=False)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.request import Request, urlopen

# mit_attempts = 0

if not exception_found:
    for mit_attempt in range(3):
        try:
            # prepare the option for the firefox driver
            from webdriver_manager.firefox import GeckoDriverManager
            driver_options = webdriver.FirefoxOptions()
            driver_options.add_argument('headless')

            # start firefox browser
            driver = webdriver.Firefox(executable_path=GeckoDriverManager(path="webdriver").install(), options=driver_options)
            url = "https://www.covidanalytics.io/projections"
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.ID, 'download-link')))
            data_uri = wait._driver.find_element_by_id("download-link").get_attribute("href")
            driver.quit()

            with urlopen(data_uri) as response:
                data = response
                mit_table = pd.read_table(data, sep=',', index_col=False, error_bad_lines=False, encoding='utf-8')

            mit_new = mit_table[mit_table['Province']=='Connecticut'].copy()
            mit_new['Day'] = mit_new['Day'].apply(pd.to_datetime).dt.floor("D")
            mit_new = mit_new[mit_new['Day'] > (pd.to_datetime('today').floor('D') + pd.Timedelta(1, unit='D'))]
            print('MIT Successfully completed on attempt: ' + str(mit_attempt + 1))
#             mit_attempts = 3
#             break
        except Exception as e:
            print(str(e))
            continue
        else:
            break
    else:
        error_msg = "Unexpected error in COVID Munge, Block 12: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise
            

#   for attempt in range(10):
#     try:
#       # do thing
#     except:
#       # perhaps reconnect, etc.
#     else:
#       break
#   else:
#     # we failed all the attempts - deal with the consequences.


# In[ ]:


# Join Projection Data
if not exception_found:
    try:
        mit_old = mit[mit['Day'] < pd.to_datetime('today').floor('D') + pd.Timedelta(2, unit='D')].copy()
        mit_latest = mit_old.append(mit_new).copy()
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 13: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# Calculate Projection Diffs
if not exception_found:
    try:
        mit_latest_work = mit_latest[['Day', 'Total Detected', 'Total Detected Deaths']].copy()

        mit_cols = mit_latest_work.columns.drop('Day')

        mit_latest_work[mit_cols] = mit_latest_work[mit_cols].apply(pd.to_numeric, errors='coerce')
        mit_latest_work['projected_new_cases'] = mit_latest_work['Total Detected'].diff()
        mit_latest_work['projected_new_deaths'] = mit_latest_work['Total Detected Deaths'].diff()
        mit_latest_work = mit_latest_work.rename(columns={"Total Detected": "projected_case_total", 
                                                          "Total Detected Deaths": "projected_death_total", 
                                                          "Day": "date"})
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 14: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise
    
# mit_latest_work.set_index('date')
# mit_latest_work['date'] = mit_latest_work.index


# In[ ]:


if not exception_found:
    try:
        mega_dat = pd.merge(extended_dat, mit_latest_work, on='date', how = 'outer').copy()
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 15: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# Update Google Sheet with latest data
if not exception_found:
    try:
        send_mega = mega_dat.copy()
        send_mega['date'] = send_mega['date'].dt.strftime('%Y-%m-%d')
        send_data(sheets_service, send_mega, config['data_sheet'][0], 'data')
        
        send_mit = mit_latest.copy()
        send_mit['Day'] = send_mit['Day'].dt.strftime('%Y-%m-%d')
        send_data(sheets_service, send_mit, config['data_sheet'][0], 'projections')
        
        meta_data['ct_last_update'][1] = ct_last_update
        send_data(sheets_service, meta_data, config['data_sheet'][0], 'meta')
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 16: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


if not exception_found:
    plt.style.use('seaborn')

    fig, ax1 = plt.subplots(figsize=(15,10))

    color = 'tab:grey'
    ax1.set_xlabel('Day')
    ax1.set_ylabel('Daily New Cases', color=color)
    ax1.plot(mega_dat['date'], mega_dat['daily_new_cases'], color=color)
    ax1.plot(mega_dat['date'], mega_dat['modeled_cases'], color='blue')
    ax1.plot(mega_dat['date'], mega_dat['projected_new_cases'], color='green')
    ax1.plot(mega_dat['date'], mega_dat['rolling_new_cases'], color='red')
    ax1.tick_params(axis='y', labelcolor=color)
    
    max_y = mega_dat[mega_dat['date'].between(pd.to_datetime('today').floor('D') - pd.Timedelta(30, unit='D'), 
                                              pd.to_datetime('today').floor('D') + pd.Timedelta(30, unit='D'))].daily_new_cases.max() + 50
    ax1.set_ylim([0, max_y])
    
    ax1.set_xlim([pd.to_datetime('today').floor('D') - pd.Timedelta(30, unit='D'), 
                                              pd.to_datetime('today').floor('D') + pd.Timedelta(30, unit='D')])
    

    # ax2 = ax1.twinx()

    # color = 'tab:blue'
    # ax2.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
    # ax2.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
    # ax2.tick_params(axis='y', labelcolor=color)
    # ax2.set_ylim([0, 120])

    ax1.set_yscale('linear')
    ax1.set_yticks(np.arange(0, max_y, step=50))
    # pyplot.xscale('log', nonposx='clip')

    fig.tight_layout()
    plt.show()


# In[ ]:


if not exception_found:
    plt.style.use('seaborn')

    fig, ax1 = plt.subplots(figsize=(15,10))

    color = 'tab:grey'
    ax1.set_xlabel('Day')
    ax1.set_ylabel('Daily New Cases', color=color)
    ax1.plot(mega_dat['date'], mega_dat['daily_new_cases'], color=color)
    ax1.plot(mega_dat['date'], mega_dat['modeled_cases'], color='blue')
    ax1.plot(mega_dat['date'], mega_dat['projected_new_cases'], color='green')
    ax1.plot(mega_dat['date'], mega_dat['rolling_new_cases'], color='red')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_ylim([0, 1500])

    # ax2 = ax1.twinx()

    # color = 'tab:blue'
    # ax2.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
    # ax2.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
    # ax2.tick_params(axis='y', labelcolor=color)
    # ax2.set_ylim([0, 120])

    ax1.set_yscale('linear')
    ax1.set_yticks(np.arange(0, 1500, step=100))
    # pyplot.xscale('log', nonposx='clip')

    fig.tight_layout()
    plt.show()


# In[ ]:


if not exception_found:
    plt.style.use('seaborn')

    fig, ax1 = plt.subplots(figsize=(15,10))

    color = 'tab:grey'
    ax1.set_xlabel('Day')
    ax1.set_ylabel('Daily New Deaths', color=color)
    ax1.plot(mega_dat['date'], mega_dat['daily_new_deaths'], color=color)
    ax1.plot(mega_dat['date'], mega_dat['modeled_deaths'], color='blue')
    ax1.plot(mega_dat['date'], mega_dat['projected_new_deaths'], color='green')
    ax1.plot(mega_dat['date'], mega_dat['rolling_new_deaths'], color='red')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_ylim([0, 150])

    # ax2 = ax1.twinx()

    # color = 'tab:blue'
    # ax2.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
    # ax2.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
    # ax2.tick_params(axis='y', labelcolor=color)
    # ax2.set_ylim([0, 120])

    fig.tight_layout()
    plt.show()


# In[ ]:


# Load alerts from Google Sheet
def pull_alerts(service, spreadsheet_id, sheet_range):
    alerts = read_data(service, spreadsheet_id, sheet_range)
    alerts = pd.DataFrame(alerts)
    new_header = alerts.iloc[0] #grab the first row for the header
    alerts = alerts[1:] #take the data less the header row
    alerts.columns = new_header #set the header row as the df header
    return(alerts)

carriers = {'verizon':'@vtext.com',
            'xfinity':'@vtext.com', 
            'google_fi':'@msg.fi.google.com', 
            'tmobile':'@tmomail.net',
           'virgin':'@vmobl.com', 
           'att':'@txt.att.net',
           'sprint':'@messaging.sprintpcs.com',
           'boost':'@sms.myboostmobile.com',
           'metro':'@mymetropcs.com',
           'us_cellular':'@email.uscc.net',
           'cricket':'@mms.cricketwireless.net',
           'republic':'@text.republicwireless.com',
           'straight_talk':'@vtext.com'}


# In[ ]:


if not exception_found:
    try:
        alerts = pull_alerts(sheets_service, config['data_sheet'][0], 'alerts')
        
        latest_day = sort_dat.iloc[len(sort_dat)-1]['date']
        latest_day_info = mega_dat[mega_dat['date']==latest_day]
        
        day_before = sort_dat.iloc[len(sort_dat)-2]['date']
        day_before_info = mega_dat[mega_dat['date']==day_before]

        EMAIL_FROM = config['email_from'][0]
        
        alert_changes = 0
        
        for idx, row in alerts.iterrows():
            if row.alerted != 'sent':
                if row.direction == 'decreasing' and                    day_before_info[row.metric].tolist()[0] > latest_day_info[row.metric].tolist()[0] and                    latest_day_info[row.metric].tolist()[0] <= float(row.threshold):
                        EMAIL_TO = row.phone + carriers[row.carrier]
                        EMAIL_SUBJECT = 'CT COVID-19 Alert System'
                        EMAIL_CONTENT = row.metric + ' is now at ' +                                         str(latest_day_info[row.metric].tolist()[0]) + ', below your alert threshold of ' +                                         str(row.threshold) + '. Visit the site to add a new alert.'
                        # Call the Gmail API
                        message = create_message(EMAIL_FROM, EMAIL_TO, EMAIL_SUBJECT, EMAIL_CONTENT)
                        sent = send_message(mail_service,'me', message)
                        alerts.iloc[idx - 1,]['alerted'] = 'sent'
                        alert_changes += 1
                elif row.direction == 'increasing' and                    day_before_info[row.metric].tolist()[0] < latest_day_info[row.metric].tolist()[0] and                    latest_day_info[row.metric].tolist()[0] >= float(row.threshold):
                        EMAIL_TO = row.phone + carriers[row.carrier]
                        EMAIL_SUBJECT = 'CT COVID-19 Alert System'
                        EMAIL_CONTENT = row.metric + ' is now at ' +                                         str(latest_day_info[row.metric].tolist()[0]) + ', above your alert threshold of ' +                                         str(row.threshold) + '. Visit the site to add a new alert.'
                        # Call the Gmail API
                        message = create_message(EMAIL_FROM, EMAIL_TO, EMAIL_SUBJECT, EMAIL_CONTENT)
                        sent = send_message(mail_service,'me', message)
                        alerts.iloc[idx - 1,]['alerted'] = 'sent'
                        alert_changes += 1
        if alert_changes > 0:
            send_data(sheets_service, alerts, config['data_sheet'][0], 'alerts')
    except Exception as e:
        error_msg = "Unexpected error in COVID Munge, Block 17: \n" + str(sys.exc_info()[0]) + "\n" + str(e)
        send_error(config['email_from'][0], error_msg, mail_service)
        exception_found = True
        raise


# In[ ]:


# plt.style.use('seaborn')

# fig, ax1 = plt.subplots(figsize=(15,10))

# color = 'tab:red'
# ax1.set_xlabel('Day')
# ax1.set_ylabel('Daily New Cases (Rolling 7-Day Average)', color=color)
# # ax1.plot(extended_dat['date'], extended_dat['rolling_new_cases'], color=color)
# ax1.plot(extended_dat['date'], extended_dat['modeled_cases'], color='blue')
# ax1.plot(extended_dat['date'], extended_dat['daily_new_cases'], color='green')
# ax1.tick_params(axis='y', labelcolor=color)
# ax1.set_ylim([-50, 2200])

# # ax2 = ax1.twinx()

# # color = 'tab:blue'
# # ax2.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
# # ax2.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
# # ax2.tick_params(axis='y', labelcolor=color)
# # ax2.set_ylim([0, 120])

# fig.tight_layout()
# plt.show()




# plt.style.use('seaborn')

# fig, ax1 = plt.subplots(figsize=(15,10))

# color = 'tab:red'
# ax1.set_xlabel('Day')
# ax1.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
# # ax1.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
# ax1.plot(extended_dat['date'], extended_dat['modeled_deaths'], color='blue')
# ax1.plot(extended_dat['date'], extended_dat['daily_new_deaths'], color='green')
# ax1.tick_params(axis='y', labelcolor=color)
# ax1.set_ylim([-50, 215])

# # ax2 = ax1.twinx()

# # color = 'tab:blue'
# # ax2.set_ylabel('Daily New Deaths (Rolling 7-Day Average)', color=color)
# # ax2.plot(extended_dat['date'], extended_dat['rolling_new_deaths'], color=color)
# # ax2.tick_params(axis='y', labelcolor=color)
# # ax2.set_ylim([0, 120])

# fig.tight_layout()
# plt.show()


# In[ ]:




