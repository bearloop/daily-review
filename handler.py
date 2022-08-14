import os
from datetime import datetime
import logging
import requests as rq
import pandas as pd
import numpy as np
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

securities_dict = {
     'IUSN.DE': 'iShares MSCI World Small Cap UCITS ETF USD Acc',
     'SWDA.MI': 'iShares Core MSCI World UCITS ETF USD (Acc)',
     'WCLD.MI': 'WisdomTree Cloud Computing UCITS ETF USD Acc',
     'WTAI.MI': 'WT Art Intelligence UCITS ETF USD Acc',
     'VOLT.MI': 'WisdomTree Battery Solutions UCITS ETF USD Acc',
     '2B70.DE': 'iShares Nasdaq US Biotechnology ETF USD Acc',
     'DGTL.MI': 'iShares Digitalisation UCITS ETF USD (Acc)',
     '2B77.DE': 'iShares Ageing Population UCITS ETF USD (Acc)',
     'RBOT.MI': 'iShares Automation & Robotics UCITS ETF USD A',
     'HEAL.MI': 'iShares Healthcare Innovation UCITS ETF USD A',
     'IH2O.MI': 'iShares Global Water UCITS ETF USD (Dist)',
     'ECAR.MI': 'iShares Elctrc Vehcls andDrivngTch UCITS ETF USD A',
     'CEMG.DE': 'iShares MSCI EM Consumer Growth UCITS ETF USD Acc',
     'INRG.MI': 'iShares Global Clean Energy UCITS ETF USD (Dist)',
     'L0CK.DE': 'iShares Digital Security UCITS ETF USD Acc',
     'ESP0.DE': 'VanEck Vectors VideoGaming&eSports UCITS ETF USD A',
     'SMH.MI': 'VanEck Vectors Semiconductor UCITS ETF',
     'AGGH.MI': 'iShares Core Gl Aggregate Bd UCITS ETF EUR Hgd Acc',
     'LQDE.MI': 'iShares $ Corp Bond UCITS ETF USD Dist',
     'HYLD.MI': 'iShares Global HY Corp Bond UCITS ETF USD Dist',
     'WELL.MI': 'HAN-GINS Indxx Hlthcr Megatrend Eql Wgt U ETF Acc',
     'ITEK.MI': 'HAN-GINS Tech Megatrend Equal Weight UCITS ETF Acc',
     'UNIC.MI': 'Lyx Msci Disrup Etf',
     'MOAT.MI': 'VanEck Vectors Morningstar US Sustainable Wide Moat UCITS ETF',
     'CBUF.DE': 'iShares MSCI Wrld Hlthcr Sector UCITS ETF USD Dist',
     'AYEW.DE': 'iShares MSCI World Inf Tech Sect UCITS ETF USD Dis',
     'IWMO.MI': 'iShares Edge MSCI WrldMmtFactor UCITS ETF USD Acc',
     'IWVL.MI': 'iShares Edge MSCI Wld ValFactor UCITS ETF USD A',
     'MVOL.MI': 'iShares Edge MSCI Wld Min Vol UCITS ETF USD A',
     'IUSA.MI': 'iShares Core S&P 500 UCITS ETF USD (Dist)',
     'EUN2.DE': 'iShares Core EURO STOXX 50 UCITS ETF EUR (Dis)',
     'CORP.MI': 'iShares Global Corp Bond UCITS ETF USD Dist',
     'QDVE.DE': 'iShares S&P 500 Inf Tech Sector UCITS ETF USD Acc',
     'WFIN.AS': 'SPDR MSCI World Financials UCITS ETF',
     'WCOS.AS': 'SPDR MSCI World Consumer Staples UCITS ETF',
     'WCOD.AS': 'SPDR MSCI World Consumer Discretionary UCITS ETF',
     'WTEL.AS': 'SPDR MSCI World Communication Services UCITS ETF',
     'WUTI.AS': 'SPDR MSCI World Utilities UCITS ETF',
     'WMAT.AS': 'SPDR MSCI World Materials UCITS ETF',
     'WTCH.AS': 'SPDR MSCI World Technology UCITS ETF',
     'WNRG.AS': 'SPDR MSCI World Energy UCITS ETF',
     'WHEA.AS': 'SPDR MSCI World Health Care UCITS ETF',
     'SPYY.DE': 'SPDR MSCI ACWI UCITS ETF',
     'VWCE.MI': 'Vanguard FTSE All-World UCITS ETF USD Acc',
     'EXSA.DE': 'iShares STOXX Europe 600 UCITS ETF (DE)',
     'EXH1.DE': 'iShares STOXX Europe 600 Oil & Gas UCITS ETF (DE)',
     'EXV9.DE': 'iShares STOXX Europe 600 Travel & Leisure (DE)',
     'QDVG.DE': 'iShares S&P500 HealthCareSector UCITS ETF USD Acc',
     'EXS1.DE': 'iShares Core DAX UCITS ETF (DE)',
     'ISF.MI': 'iShares Core FTSE 100 UCITS ETF GBP Dist',
     'EIMI.MI': 'iShares Core MSCI EM IMI UCITS ETF USD Acc',
     'EXXT.DE': 'iShares NASDAQ-100  UCITS ETF (DE)',
     'CSNKY.MI': 'iShares Nikkei 225 UCITS ETF JPY (Acc)',
     'EUNI.DE': 'iShares MSCI EM Small Cap UCITS ETF USD (Dist)',
     'IUS3.DE': 'iShares S&P Small Cap 600 UCITS ETF USD (Dist)',
     'DJSC.MI': 'iShares EURO STOXX Small UCITS ETF EUR (Dist)',
     'IWQU.MI': 'iShares Edge MSCI Wld Qual Factor UCITS ETF USD A',
     'IWSZ.MI': 'iShares Edge MSCI Wld Size Factor UCITS ETF USD A',
     'IWDP.MI': 'iShares Dvlp Mrkts Prop Yld UCITS ETF USD Dist',
     'IEAC.MI': 'iShares Core   Corp Bond UCITS ETF EUR Dist',
     'IHYU.MI': 'iShares $ HY Corp Bond UCITS ETF USD Dis',
     'IEAG.MI': 'iShares   Aggregate Bond UCITS ETF EUR (Dist)',
     'SEGA.MI': 'iShares Core   Govt Bond UCITS ETF EUR Dist',
     'WIND.AS': 'SPDR MSCI World Industrials UCITS ETF',
     'IGLT.MI': 'iShares Core UK Gilts UCITS ETF GBP (Dist)',
     'EUNX.DE': 'iShares US Aggregate Bond UCITS ETF USD (Dist)',
     'IBTS.MI': 'iShares $ Treasury Bd 1-3yr UCITS ETF USD Dist',
     'IBTM.MI': 'iShares $ Treasury Bd 7-10y UCITS ETF USD Dist',
     'IEMB.MI': 'iShares JP Morgan $ EM Bond UCITS ETF USD Dis',
     'EMBE.MI': 'iShares J.P. Morgan $ EM Bd EUR Hgd UCITS ETF Dist',
     'DAVV.DE': 'VanEck Vectors Digital Assets Equity UCITS ETF',
     'HDRO.MI': 'VanEck Vectors Hydrogen Economy UCITS ETF A USD',
     'WCBR.MI': 'WisdomTree Cybersecurity UCITS ETF USD Acc',
     'QDVK.DE': 'iSharesS&P500 Cons Discr Sector UCITS ETF USD(Acc)',
     'QDVH.DE': 'iShares S&P 500 Financials Sector UCITS ETF USD Acc',
     'EXV5.DE': 'iShares STOXX Europe 600 Automobiles & Parts (DE)',
     'EXV1.DE': 'iShares STOXX Europe 600 Banks UCITS ETF (DE)',
     'GMVM.DE': 'VanEck Vectors Morningstar US Sustainable Wide Moat UCITS ETF',
     'XLES.MI': 'Invesco Energy S&P US Select Sector UCITS ETF Acc',
     'XLIS.MI': 'Invesco Industrials S&P US Sel Sect UCITS ETF Acc',
     'XLBS.MI': 'Invesco Materials S&P US Sel Sector UCITS ETF Acc',
     'XLFS.MI': 'Invesco Financials S&P US Sel Sector UCITS ETF Acc',
     'XLKS.MI': 'Invesco Technology S&P US Sel Sector UCITS ETF Acc',
     'XLPS.MI': 'Invesco Cnsmr Staples S&P US Sel Sec UCITS ETF Acc',
     'XLVS.MI': 'Invesco Health Care S&P US Sel Sect UCITS ETF Acc',
     'XLUS.MI': 'Invesco Utilities S&P US Sel Sector UCITS ETF Acc',
     'XLYS.MI': 'Invesco Consumer Discr S&P US Sel Sec UCITS ETFAcc',
     'IU5C.DE': 'iShares S&P 500 Communication Sector USD Acc',
     'EXV3.DE': 'iShares STOXX Europe 600 Technology UCITS ETF (DE)'}

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