from datetime import datetime, timezone
from dateutil.relativedelta import weekday, FR, relativedelta

def ceil_3(x):
    return (x + 2) // 3 * 3

def get_delivery(timestamp, type='timestamp'):
    types = ['timestamp', 'date']
    if type not in types:
        raise ValueError("Invalid type. Expected 'timestamp' or 'date'")
    
    time_from_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)

    delivery_date = time_from_timestamp + \
        relativedelta(month = ceil_3(time_from_timestamp.month)) + \
        relativedelta(day=31, weekday=FR(-1), hour=8, minute=0, second=0)
    
    if(delivery_date < time_from_timestamp):
        time_from_timestamp += relativedelta(months=2)
        delivery_date = time_from_timestamp + \
            relativedelta(month = ceil_3(time_from_timestamp.month)) + \
            relativedelta(day=31, weekday=FR(-1), hour=8, minute=0, second=0)
        
    if(delivery_date < time_from_timestamp):
        raise Exception
    
    if type=='timestamp':
        return int(delivery_date.timestamp())
    if type=='date':
        return delivery_date

def get_quarter_time_left(timestamp):
    return get_delivery(timestamp) - timestamp

def get_quarter_symbol(pair, timestamp):
    delivery = get_delivery(timestamp, type='date')
    return f"{pair}_{delivery.year%100}{str(delivery.month).zfill(2)}{delivery.day}"