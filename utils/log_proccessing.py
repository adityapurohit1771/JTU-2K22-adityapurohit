import urllib.request
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import restapi.views_constants as consts

def sort_by_time_stamp(logs:list)->list:
    '''
    Sorts log data by time stamp
    '''
    data = []
    for log in logs:
        data.append(log.split(" "))
    # print(data)
    data = sorted(data, key=lambda elem: elem[1])
    return data

def response_format(raw_data):
    response = []
    for timestamp, data in raw_data.items():
        entry = {'timestamp': timestamp}
        logs = []
        data = {k: data[k] for k in sorted(data.keys())}
        for exception, count in data.items():
            logs.append({'exception': exception, 'count': count})
        entry['logs'] = logs
        response.append(entry)
    return response

def aggregate(cleaned_logs:list)->list:
    '''
    Aggregate the log data
    '''
    data = {}
    for log in cleaned_logs:
        [key, text] = log
        value = data.get(key, {})
        value[text] = value.get(text, 0)+1
        data[key] = value
    return data


def transform(logs:list)->list:
    '''
    Transforms log data by adding timestamp
    '''
    result:list = []
    for log in logs:
        [_, timestamp, text] = log
        text:str = text.rstrip()
        timestamp = datetime.utcfromtimestamp(int(int(timestamp)/1000))
        hours, minutes = timestamp.hour, timestamp.minute
        key = ''

        if minutes >= 45:
            if hours == 23:
                key = f"{hours}:45-00:00"
            else:
                key = f"{hours}:45-{hours+1}:00"
        elif minutes >= 30:
            key = f"{hours}:30-{hours}:45"
        elif minutes >= 15:
            key = f"{hours}:15-{hours}:30"
        else:
            key = f"{hours}:00-{hours}:15"

        result.append([key, text])
        print(key)

    return result


def reader(url:str, timeout:int)->str:
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        data:str = conn.read()
        data = data.decode('utf-8')
        return data


def multi_thread_reader(urls:list, num_threads:int)->list:
    """
        Read multiple files through HTTP
    """
    result = []
    with ThreadPoolExecutor(max_workers=min(32, num_threads)) as executor:
        logs:list = [executor.submit(reader, url, consts.EXECUTOR_TIMEOUT) for url in urls]
    for log in as_completed(logs):
        result.extend(log.split("\n"))
    result = sorted(result, key=lambda elem:elem[1])
    return result