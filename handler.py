import os
from datetime import datetime
import logging
import requests as rq
import pandas as pd
import numpy as np
import boto3
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with open('securities.json') as jsonfile:
    securities_dict = json.load(jsonfile)

securities_list = list(securities_dict.keys())


def get_single_item(security):
        
    try:
        
        user_agent_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) \
                                         AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 \
                                         Safari/537.36'}
            
        resp = rq.get('https://query2.finance.yahoo.com/v8/finance/chart/{}'.format(security),
                      headers=user_agent_headers)
        
        currency = resp.json()['chart']['result'][0]['meta']['currency']
        
        last_price = resp.json()['chart']['result'][0]['meta']['regularMarketPrice']
        
        prev_close = resp.json()['chart']['result'][0]['meta']['previousClose']

        ret = (last_price - prev_close) / prev_close

        rounded_ret = round(ret*100,2)
        
        last_price = currency + ' ' + str(round(last_price,2))
    
    except:
        logger.info('Missing: ' + security)
        last_price, rounded_ret = np.NaN,np.NaN
    
    return (security, last_price, rounded_ret, securities_dict[security])



def get_bulk_items(sec_list):
    
    res_pd = pd.DataFrame(columns=['price','return','name'])

    for sec in sec_list:

        res_pd.loc[sec,:] = get_single_item(sec)[1:]

    res_pd.sort_values(by='return',ascending=False, inplace=True)

    res_pd['return'] = pd.concat(['+'+res_pd['return'][res_pd['return']>0].astype(str)+'%',
                                      res_pd['return'][res_pd['return']<=0].astype(str)+'%',
                                      res_pd['return'][res_pd['return'].isnull()].fillna('-')
                                  ])
    res_pd.fillna('-',inplace=True)
    
    return res_pd

def convert_pd_to_html(df):
    
    out = """"""
    
    for ind in df.index:
        
        is_number = '.' in df.loc[ind,'return']
        
        if df.loc[ind,'return'][0] == "+":
            color = "green"
        
        elif (df.loc[ind,'return'][0] == "-") & is_number:
            color = "red"
            
        else:
            color = "grey"
            
        out+= """<tr>
                <td>{ticker}</td>
                <td>{price}</td>
                <td style="color:{clr}">{ret1d}</td>
                <td>{name}</td>
                </tr>""".format(ticker  = ind,
                                price   = df.loc[ind,'price'],
                                ret1d   = df.loc[ind,'return'],
                                name    = df.loc[ind,'name'],
                                clr     = color)
    
    return out



def produce_content():

    df_to_convert = get_bulk_items(securities_list)

    content = """

            <html>
                <head>
                    <style>
                        table, th, td {
                            border: 1.3px dotted grey;
                            font-size:11pt;
                            font-family: Trebuchet MS;
                          }
                    </style>
                </head>
                <body>

                <p></p>

                <table cellpadding="10">
                    <tbody>
                        <tr>
                        <td>ETF Ticker</td>
                        <td>Last Price</td>
                        <td>1D Ret %</td>
                        <td>Name</td>
                        </tr>
                        """+convert_pd_to_html(df_to_convert)+"""
                    </tbody>
                </table>

                <p>With love from lambda</p>

               </body> 
            </html>
        """
        
    return content

    

def run(event, context):
    current_time = datetime.now().time()
    name = context.function_name

    email_acc = os.environ['EMAIL']

    logger.info("Your cron function " + name + " ran at " + str(current_time))
    logger.info("environment variable: " + email_acc)
    logger.info("Requests version: " + rq.__version__)
    logger.info("Pandas version: " + pd.__version__)
    logger.info("NumPy version: " + np.__version__)


    try:
        ses_client = boto3.client("ses", region_name="eu-central-1")

        CHARSET = "UTF-8"

        HTML_EMAIL_CONTENT = produce_content()

        response = ses_client.send_email(
            Destination={
                "ToAddresses": [email_acc],
            },
            Message={
                "Body": {
                    "Html": {
                        "Charset": CHARSET,
                        "Data": HTML_EMAIL_CONTENT,
                    }
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": "Daily funds review, " + datetime.now().strftime("%d/%m/%y"),
                },
            },
            Source=email_acc,
        )
    
    except:
        logger.info('Could not send email')