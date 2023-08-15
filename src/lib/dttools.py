import datetime

def date_range(start, stop, period='monthly'):
    dt_start = datetime.datetime.strptime(start, '%Y-%m-%d')
    dt_stop = datetime.datetime.strptime(stop, '%Y-%m-%d')
    dates = []
    
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += datetime.timedelta(days=1)

    if period=='monthly':
        return [i for i in dates if i.endswith("01")]
    
    elif period=='yearly':
        return [i for i in dates if i.endswith("01") and i.split('-')[-2]=='01']
    
    return dates