from grab import Grab
import json

from mysql.connector import DatabaseError
from mysql.connector import ProgrammingError
import repair_data as rd
from datetime import datetime, timedelta
import grab_data
import data_params as dp
import argparse
import mysql.connector as db
from itertools import islice
import Generation_final_repair as gfr
import download_transmission as dt
import download_generation as dg
import report_missing


argparser = argparse.ArgumentParser()
argparser.add_argument("--data_type", default="generation-update", type=str, help="data type")
argparser.add_argument("--days_back", default="1", type=int, help="days back")
argparser.add_argument("--start_date", default="2016-01-01", type=str, help="Time format Y-m-d")
argv = argparser.parse_args()


global saved_data
saved_data = {}

def main():
    parse_arguments()
    params = {}
    if argv.data_type == 'transmission-update':
        dt.download_transmission()
    elif argv.data_type == 'generation-update':
        downloader = dg.Download_generation()
        downloader.download_generation()
        # gfr.repair()
    elif argv.data_type == 'find-missing':
        report_missing.Find_missing_data()
    elif argv.data_type == 'generation-forecast':
        params['type'] = 'generation_forecast'
        for country in dp.get_all_generation_countries():
            print 'Begin country: ' + country
            params['country'] = country
            params['start'] = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=argv.days_back)
            params['end'] = datetime.now().replace(microsecond=0, second=0, minute=0)#datetime(2016, 6, 2, 0, 0)  # datetime.now().replace(microsecond=0, second=0, minute=0) #params['start'] + timedelta(days=2)
            params['type_secondary'] = '-'
            get_all_dates_for_params(params)
            save_all_data(params)
    elif argv.data_type == 'load-actual':
        params['type'] = 'load_actual'
        for country in dp.get_all_generation_countries():
            print 'Begin country: ' + country
            params['country'] = country
            params['start'] = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(
                days=argv.days_back)
            params['end'] = datetime.now().replace(microsecond=0, second=0,minute=0)
            params['type_secondary'] = '-'
            get_all_dates_for_params(params)
            save_all_data(params)

def get_all_dates_for_params(params):
    temp_start = params['start']
    while params['start'] < params['end']:
        pars = params.copy()
        pars['end'] = params['start'] + timedelta(days=1) if (params['start'] + timedelta(days=1)) < params['end'] else \
        params['end']
        params['start'] += timedelta(days=1)
        params['start'] = params['start'].replace(hour=0)
        get_and_append_data(pars)
    params['start'] = temp_start

def get_and_append_data(params):
    g = grab_data.Grab_data()
    data = g.fetch_data(params)
    data = rd.repair_data(params, data)
    add_data(params,data)
    print "\t\t\tDone! - " + params['type']+ "-"+params['country'] + " -> " + params['start'].strftime('%Y-%m-%d') + " " + params['type_secondary']

def add_data(params, data):
    content = {}
    is_15_minutes = len([x for x in data if x['start'].minute != 0])>0
    for point in data:
        if is_15_minutes:
            key = str(point["start"].hour) +":"+str(point["start"].minute)
            content[key] = point["quantity"];
        else:
            content[point["start"].hour] = point["quantity"];
    if not content:
        return
    part = {}
    part['data_type'] = params["type"]
    part['country'] = params["country"]
    part['data_type_secondary'] = params["type_secondary"]
    part['date'] = params["start"].strftime('%Y-%m-%d')
    part['content'] = content
    part['approximation'] = []
    saved_data[len(saved_data)] = part

def save_all_data(params):
    global saved_data
    for x in saved_data:
        print x

def chunks(data, SIZE=10000):
    it = iter(data)
    for i in xrange(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

def parse_arguments():
    if argv.start_date != "-":
        delta = datetime.now() - datetime.strptime(argv.start_date,'%Y-%m-%d')
        argv.days_back = delta.days

def get_argv():
    return argv

if __name__ == "__main__":
    main()
