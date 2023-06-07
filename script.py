from sshtunnel import SSHTunnelForwarder
import pymysql

import schedule
import pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import FR, relativedelta
from time import sleep

import pandas as pd
import numpy as np

from sqlalchemy import create_engine
import textwrap 
import logging
import configparser
from binance.client import Client
from quarter_functions import get_quarter_time_left, get_quarter_symbol

from send_telegram_message import send_telegram_message

logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

config = configparser.ConfigParser()
config.read('api_data.ini')
client = Client(config['binance']['api_key'], config['binance']['api_secret'])

def run_sql_script(script):
    mysql_connect()
    cursor = connection.cursor()
    cursor.execute(script)
    connection.close()
    return cursor.fetchall()

def connect_ssh_tunnel(ssh_address, ssh_username, ssh_password):
    try:
        global tunnel
        tunnel = SSHTunnelForwarder((ssh_address),
            ssh_username = ssh_username,
            ssh_password = ssh_password,
            ssh_proxy_enabled = True,
            remote_bind_address = ('localhost', 3306))
        tunnel.start()
    except BaseException as e:
        logging.info('ERROR: {}'.format(e))
    finally:
        if tunnel:
            tunnel.close

def mysql_connect():
    global connection
    connection = pymysql.connect(
        host = config['mysql']['host'],
        user = config['mysql']['user'],
        passwd = config['mysql']['passwd'],
        port = tunnel.local_bind_port 
    )

def create_sql_engine():
    global engine
    engine = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/exchange data".format(
        user = config['mysql']['user'], 
        passwd = config['mysql']['passwd'], 
        host = config['mysql']['host'], 
        port = tunnel.local_bind_port))

def load_historical_data():
    df_perpetual = pd.DataFrame()
    df_cq = pd.DataFrame()
    futures_history = pd.DataFrame()

    endTime = run_sql_script('SELECT MIN(timestamp) FROM `exchange data`.BTC_perpetual')
    endTime = endTime[0][0] * 1000 - 30000

    endTime_iterator = endTime
    shape_before, shape_after = -1, 0
    while(shape_before != shape_after):
        for i in range (20):
            shape_before = df_perpetual.shape[0]
            df_perpetual = df_perpetual.append(pd.DataFrame(
                client.futures_continous_klines(
                    pair='BTCUSDT', 
                    contractType='PERPETUAL',
                    interval='1m', 
                    endTime = endTime_iterator,
                    limit = 1500
                )
            ).iloc[::-1])
            shape_after = df_perpetual.shape[0]
            endTime_iterator = df_perpetual.iloc[-1,0] - 30000
        print(shape_after, end = '>')
    print('\n')
    endTime_iterator = endTime
    shape_before, shape_after = 0, 1
    while(shape_before != shape_after):
        for i in range (20):
            shape_before = df_cq.shape[0]
            df_cq = df_cq.append(pd.DataFrame(
                client.futures_continous_klines(
                    pair='BTCUSDT', 
                    contractType='CURRENT_QUARTER',
                    interval='1m', 
                    endTime = endTime_iterator,
                    limit = 1500
                )
            ).iloc[::-1])
            shape_after = df_cq.shape[0]
            endTime_iterator = df_cq.iloc[-1,0] - 30000
        print(shape_after, end = '>')
    print('\n')
    # параметр endTime не работает, пришлось немного подругому
    startTime = 1568102400000
    shape_before, shape_after = 1, 0
    while(shape_before != shape_after):
        shape_before = futures_history.shape[0]
        futures_history = futures_history.append(
            pd.DataFrame.from_dict(client.futures_funding_rate(
                symbol = 'btcusdt', 
                startTime = startTime, 
                limit = 1000
            )).iloc[:,1:]
        )
        shape_after = futures_history.shape[0]
        startTime = futures_history.iloc[-1,0] + 30000
        print(shape_after, end = '>')
    print('\n')
    df_perpetual.drop(columns = [6,7,9,10,11], inplace = True)
    df_perpetual.reset_index(drop = True, inplace= True)
    df_perpetual.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    df_perpetual['timestamp']//=1000

    df_cq.drop(columns = [6,7,9,10,11], inplace = True)
    df_cq.reset_index(drop = True, inplace= True)
    df_cq.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    df_cq['timestamp']//=1000
    
    futures_history['fundingTime']//=1000

    futures_history = pd.DataFrame(np.repeat(futures_history.values, 480, axis=0), columns= ['timestamp','funding_rate'])
    futures_history['timestamp'] = futures_history.apply(lambda row: row['timestamp'] - 60 * (480-row.name%480-1), axis= 1)

    df_perpetual = df_perpetual.merge(futures_history, how='left', on='timestamp')
    df_cq['time_left'] = df_cq['timestamp'].apply(lambda x: get_quarter_time_left(x))

    try:
        df_cq.to_sql('BTC_current_quarter', engine, if_exists = 'append', index = False)
        df_perpetual.to_sql('BTC_perpetual', engine, if_exists = 'append', index = False)
    except BaseException as e:
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['heorhii'], config['telegram']['token_error'])
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['nikita'], config['telegram']['token_error'])
        logging.info('ERROR: {}'.format(e))

def backup_from_last_record():
    df_perpetual = pd.DataFrame()
    df_cq = pd.DataFrame()

    startTime_perp = run_sql_script('SELECT MAX(timestamp) FROM `exchange data`.BTC_perpetual')[0][0]
    startTime_cq = run_sql_script('SELECT MAX(timestamp) FROM `exchange data`.BTC_current_quarter')[0][0]

    if ((datetime.now().timestamp() - min(startTime_perp, startTime_cq)) < 60):
        logging.info('No data to backup')
        return

    startTime_perp = (startTime_perp + 30) * 1000
    startTime_cq = (startTime_cq + 30) * 1000

    logging.info('Starting restoring data from: {}/{}'.format(startTime_perp, startTime_cq) )

    startTime_iterator = startTime_perp
    while startTime_iterator/1000 < datetime.today().timestamp():
        df_perpetual = df_perpetual.append(
            pd.DataFrame(client.futures_continous_klines(
                pair='BTCUSDT', 
                contractType='PERPETUAL', 
                interval='1m', 
                startTime = startTime_iterator, 
                limit = 1500)
            )
        )
        startTime_iterator = df_perpetual.iloc[-1,0] + 30000

    startTime_iterator = startTime_cq
    while startTime_iterator/1000 < datetime.today().timestamp():
        df_cq = df_cq.append(
            pd.DataFrame(client.futures_continous_klines(
                pair='BTCUSDT', 
                contractType='CURRENT_QUARTER', 
                interval='1m', 
                startTime = startTime_iterator, 
                limit = 1500)
            )
        )
        startTime_iterator = df_perpetual.iloc[-1,0] + 30000


    logging.info('Data retrived, {}/{} records successfully loaded'.format(df_perpetual.shape[0], df_cq.shape[0]))

    df_perpetual.drop(columns = [6,7,9,10,11], inplace = True)
    df_perpetual.reset_index(drop = True, inplace = True)
    df_perpetual.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    df_perpetual['timestamp']//=1000

    df_cq.drop(columns = [6,7,9,10,11], inplace = True)
    df_perpetual.reset_index(drop = True, inplace = True)
    df_cq.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    df_cq['timestamp']//=1000

    try:
        futures_history = pd.DataFrame.from_dict(
            client.futures_funding_rate(
                symbol = 'btcusdt', 
                startTime = startTime_perp, 
                limit = 1000)
        ).iloc[:,1:]
        futures_history['fundingTime']//=1000
        futures_history = pd.DataFrame(np.repeat(futures_history.values, 480, axis=0), columns= ['timestamp','funding_rate'])
        futures_history['timestamp'] = futures_history.apply(lambda row: row['timestamp'] - 60 * (480-row.name%480-1), axis= 1)

        df_perpetual = pd.merge(df_perpetual, futures_history, how = 'left', on="timestamp")
        df_perpetual.fillna(client.futures_mark_price(symbol = 'BTCUSDT')['lastFundingRate'], inplace= True)
    except BaseException:
        df_perpetual['funding_rate'] = client.futures_mark_price(symbol = 'BTCUSDT')['lastFundingRate']
    finally:
        pass

    df_cq['time_left'] = df_cq['timestamp'].apply(lambda x: get_quarter_time_left(x))

    try:
        df_cq.to_sql('BTC_current_quarter', engine, if_exists = 'append', index = False)
    except BaseException as e:
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['heorhii'], config['telegram']['token_error'])
        logging.info('ERROR: {}'.format(e))

    try:
        df_perpetual.to_sql('BTC_perpetual', engine, if_exists = 'append', index = False)
    except BaseException as e:
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['heorhii'], config['telegram']['token_error'])
        logging.info('ERROR: {}'.format(e))
    
    logging.info('data transfered successfully {}/{} records added'.format(df_perpetual.shape[0], df_cq.shape[0]))

def stream():
    last_perp = pd.DataFrame(client.futures_continous_klines(pair='BTCUSDT', contractType='PERPETUAL', interval='1m', limit=1))
    last_cq = pd.DataFrame(client.futures_continous_klines(pair='BTCUSDT', contractType='CURRENT_QUARTER', interval='1m', limit=1))

    last_perp.drop(columns = [6,7,9,10,11], inplace = True)
    last_perp.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    last_perp['timestamp']//=1000
    last_perp['funding_rate'] = client.futures_mark_price(symbol = 'BTCUSDT')['lastFundingRate']

    last_cq.drop(columns = [6,7,9,10,11], inplace = True)
    last_cq.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades']
    last_cq['timestamp']//=1000
    last_cq['time_left'] = last_cq['timestamp'].apply(lambda x: get_quarter_time_left(x))

    try:
       last_perp.to_sql('BTC_perpetual', engine, if_exists = 'append', index = False)
       last_cq.to_sql('BTC_current_quarter', engine, if_exists = 'append', index = False)
    except BaseException as e:
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['heorhii'], config['telegram']['token_error'])

        logging.info('ERROR: {}'.format(e))

def daily_notification():
    def format_timestamp(timestamp):
        x = datetime.fromtimestamp(int(timestamp))
        return '{}/{} {}:{}'.format(str(x.day).zfill(2), str(x.month).zfill(2), str(x.hour).zfill(2), str(x.minute).zfill(2))

    last_three_fr = pd.DataFrame.from_dict(client.futures_funding_rate(symbol = 'btcusdt', limit = 3)).iloc[:,1:]
    last_three_fr['fundingTime'] = (last_three_fr['fundingTime']//1000).apply(format)

    cq_bid_ask = run_sql_script('SELECT timestamp, bidPrice, askPrice FROM `exchange data`.BTC_cq_bid_ask ORDER BY timestamp DESC LIMIT 1454')
    cq_bid_ask = pd.DataFrame(cq_bid_ask, columns = ['timestamp', 'bidPrice', 'askPrice'])

    perp_bid_ask = run_sql_script('SELECT timestamp, bidPrice, askPrice FROM `exchange data`.BTC_perp_bid_ask ORDER BY timestamp DESC LIMIT 1454')
    perp_bid_ask = pd.DataFrame(perp_bid_ask, columns = ['timestamp', 'bidPrice', 'askPrice'])

    df = pd.merge (perp_bid_ask, cq_bid_ask, on = 'timestamp')
    df['askP-bidQ'] = df['askPrice_x'] - df['bidPrice_y']
    df['askQ-bidP'] = df['askPrice_y'] - df['bidPrice_x']

    df['askP-bidQ_perc'] = (df['askPrice_x'] - df['bidPrice_y'])/df['bidPrice_y']
    df['askQ-bidP_perc'] = (df['askPrice_y'] - df['bidPrice_x'])/df['bidPrice_y']

    first_max_spread_perc = df['askP-bidQ_perc'].max()
    first_max_spread_abs = df[df['askP-bidQ_perc'] == first_max_spread_perc].iloc[0,5]
    first_max_spread_timestamp = format_timestamp(df[df['askP-bidQ_perc'] == first_max_spread_perc].iloc[0,0])

    first_min_spread_perc = df['askP-bidQ_perc'].min()
    first_min_spread_abs = df[df['askP-bidQ_perc'] == first_min_spread_perc].iloc[0,5]
    first_min_spread_timestamp = format_timestamp(df[df['askP-bidQ_perc'] == first_min_spread_perc].iloc[0,0])

    second_max_spread_perc = df['askQ-bidP_perc'].max()
    second_max_spread_abs = df[df['askQ-bidP_perc'] == second_max_spread_perc].iloc[0,6]
    second_max_spread_timestamp = format_timestamp(df[df['askQ-bidP_perc'] == second_max_spread_perc].iloc[0,0])

    second_min_spread_perc = df['askQ-bidP_perc'].min()
    second_min_spread_abs = df[df['askQ-bidP_perc'] == second_min_spread_perc].iloc[0,6]
    second_min_spread_timestamp = format_timestamp(df[df['askQ-bidP_perc'] == second_min_spread_perc].iloc[0,0])
    message = textwrap.dedent('''\
    funding:
    {:>8f}  -  {}
    {:>8f}  -  {}
    {:>8f}  -  {}
    {:.8f} - total

    askP-bidQ spread:
    max:{:>9}% ({:>5}) - {}
    min:{:>9}% ({:>5}) - {}

    askQ-bidP spread:
    max:{:>9}% ({:>5}) - {}
    min:{:>9}% ({:>5}) - {}'''.format(
                                        float(last_three_fr['fundingRate'][0]), format_timestamp(last_three_fr['fundingTime'][0]),
                                        float(last_three_fr['fundingRate'][1]), format_timestamp(last_three_fr['fundingTime'][1]),
                                        float(last_three_fr['fundingRate'][2]), format_timestamp(last_three_fr['fundingTime'][2]),
                                        round(last_three_fr['fundingRate'].astype(float).sum(),8),
                                        round(first_max_spread_perc, 6), round (first_max_spread_abs, 1), first_max_spread_timestamp,
                                        round(first_min_spread_perc, 6), round (first_min_spread_abs, 1), first_min_spread_timestamp,
                                        round(second_max_spread_perc, 6), round (second_max_spread_abs, 1), second_max_spread_timestamp,
                                        round(second_min_spread_perc, 6), round (second_min_spread_abs, 1), second_min_spread_timestamp,
                                        ))
    send_telegram_message(message, config['telegram']['nikita'], config['telegram']['token_daily'])
    send_telegram_message(message, config['telegram']['heorhii'], config['telegram']['token_daily'])

def weekly_notification():
    BTC_perpetual = pd.DataFrame(run_sql_script('SELECT * FROM `exchange data`.BTC_perpetual ORDER BY timestamp DESC limit 10090'))
    BTC_perpetual.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades', 'funding_rate']

    BTC_current_quarter = pd.DataFrame(run_sql_script('SELECT * FROM `exchange data`.BTC_current_quarter ORDER BY timestamp DESC limit 10090'))
    BTC_current_quarter.columns = ['timestamp', 'open', 'max', 'min', 'close', 'volume', 'number_of_trades', 'time_left']

    data = BTC_perpetual.merge(BTC_current_quarter, on='timestamp', how='left', suffixes=('_p','_q')).sort_values('timestamp', ascending=False)
    data = data.astype(float)
    data['spread'] = (
        (data['min_q']/data['min_p']
        + data['max_q']/data['max_p'])/2 - 1)*100
    message = '\n'.join(str(data['spread'].describe()).split('\n')[1:-1])
    
    send_telegram_message(message, config['telegram']['nikita'], config['telegram']['token_daily'])
    send_telegram_message(message, config['telegram']['heorhii'], config['telegram']['token_daily'])

def get_bid_ask_data():
    def get_quarter_symbol():
        time = datetime.now()
        delivery = time + relativedelta(month = ((time.month-1)//3+1)*3) + relativedelta(day= 31, weekday=FR(-1)) + relativedelta(hour=8, minute=0, second=0)
        delivery = pytz.utc.localize(delivery)

        if (delivery.timestamp() < time.timestamp()):
            time = (time + timedelta(weeks=2))
            delivery = time + relativedelta(month = ((time.month-1)//3+1)*3) + relativedelta(day= 31, weekday=FR(-1)) + relativedelta(hour=8, minute=0, second=0)

        quarter_symbol = 'BTCUSDT_{}{}{}'.format(delivery.year%100, str(delivery.month).zfill(2), delivery.day)
        return quarter_symbol

    BTC_perp_bid_ask = pd.DataFrame.from_dict([client.futures_orderbook_ticker(symbol = 'BTCUSDT')]).iloc[:,1:-1]
    BTC_perp_bid_ask['timestamp'] = int(datetime.now().timestamp()//60*60)
    BTC_perp_bid_ask['funding_rate'] = client.futures_mark_price(symbol = 'BTCUSDT')['lastFundingRate']

    BTC_cq_bid_ask = pd.DataFrame.from_dict([
        client.futures_orderbook_ticker(
            symbol = get_quarter_symbol(datetime.now().timestamp())
        )
    ]).iloc[:,1:-1]
    BTC_cq_bid_ask['timestamp'] = int(datetime.now().timestamp()//60*60)
    BTC_cq_bid_ask['time_left'] = get_quarter_time_left(BTC_cq_bid_ask['timestamp'][0])

    try:
       BTC_perp_bid_ask.to_sql('BTC_perp_bid_ask', engine, if_exists = 'append', index = False)
       BTC_cq_bid_ask.to_sql('BTC_cq_bid_ask', engine, if_exists = 'append', index = False)
    except BaseException as e:
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('ERROR: {}'.format(e), config['telegram']['heorhii'], config['telegram']['token_error'])
        logging.info('ERROR: {}'.format(e))

def run_backup_and_stream():
    backup_from_last_record()
    try:
        schedule.every().minute.at(":00").do(get_bid_ask_data)
        schedule.every().day.at("08:10:30").do(daily_notification)
        schedule.every().minute.at(":10").do(stream)
        # schedule.every.friday.at("08:10:45").do(weekly_notification)
        while True:
            schedule.run_pending()
    except KeyboardInterrupt:
        pass
    finally:
        send_telegram_message('Script stopped!', config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('Script stopped!', config['telegram']['heorhii'], config['telegram']['token_error'])

        logging.info("stopping scheduler")
        schedule.clear()

def main():
    try:
        send_telegram_message('Script started!', config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('Script started!', config['telegram']['heorhii'], config['telegram']['token_error'])
        connect_ssh_tunnel(config['ssh']['address'], config['ssh']['username'], config['ssh']['password'])
        create_sql_engine()
        run_backup_and_stream()
    except:
        send_telegram_message('Script stopped!, restarting in 300 seconds', config['telegram']['nikita'], config['telegram']['token_error'])
        send_telegram_message('Script stopped!, restarting in 300 seconds', config['telegram']['heorhii'], config['telegram']['token_error'])
        sleep(300)
        main()

if __name__ == '__main__':
    main()
