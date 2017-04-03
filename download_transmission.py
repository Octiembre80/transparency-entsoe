
import data_params as dp
from datetime import datetime,timedelta
import main as mn


def download_transmission():
    mn.parse_arguments()
    params = {}
    params['type'] = 'transmission'
    params['country'] = 'CZE'
    for k in dp.get_all_transmission_areas():
        params['country_in'] = k['in']
        params['country_out'] = k['out']
        params['start'] = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=mn.get_argv().days_back)
        params['end'] = datetime.now().replace(minute=0)  # params['start'] + timedelta(days=2)
        params['type_secondary'] = params['country_out'] + '-' + params['country_in']
        mn.get_all_dates_for_params(params)
    mn.save_all_data(params)


