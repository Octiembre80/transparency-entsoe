
from grab import Grab
import data_params as params
from datetime import datetime, timedelta
import xmltodict
import pytz
import repair_data as rd
import xml.parsers.expat as ex

class Grab_data():
    def __init__(self):
        pass

    def fetch_data(self, params):
        self.params = params
        self.dst = self.is_dst(params['start'])
        settings = self.create_settings(params)
        data = self.get_data(settings)
        self.fd = Format_data(self.params,self.dst)
        data = self.fd.format_data(data)
        data = self.strip_data_if_needed(data, params)
        return data

    def fetch_and_repair_data(self,params):
        data = self.fetch_data(params)
        return rd.repair_data(params,data)

    def create_settings(self, data_type):
        settings = {}
        if data_type['type'] is "load_actual":
            settings = params.get_load_actual(data_type['country'])

        elif data_type['type'] is "load_day_forecast":
            settings = params.get_load_day_forecast(data_type['country'])

        elif data_type['type'] is 'transmission':
            settings = params.get_transmission(data_type['country_in'], data_type['country_out'])

        elif data_type['type'] is 'generation':
            settings = params.get_generation(data_type['country'],data_type['type_secondary'])

        elif data_type['type'] is 'generation_forecast':
            settings = params.get_generation_forecast(data_type['country'])

        elif data_type['type'] is 'generation_wind_solar_forecast':
            settings = params.get_generation_wind_solar_forecast(data_type['country'])
            if 'psrType' in data_type:
                settings['psrType'] = data_type['psrType']

        dst = 1 if self.dst else 0
        settings["periodStart"] = (data_type['start'] + timedelta(hours=-1 - dst)).strftime('%Y%m%d%H%M')
        settings["periodEnd"] = (data_type['end'] + timedelta(hours=0 - dst)).strftime('%Y%m%d%H%M')
        return settings

    def get_data(self, settings):
        g = Grab()
        url = self.generate_api_url(settings)
        # print url
        while (g.doc.code != 200 and g.doc.code != 400):
            try:
                g = Grab()
                response = g.go(url)
            except:
                print "ENTSOE grab data except:  code: " + str(g.doc.code)
        try:
            return xmltodict.parse(response.body)
        except ex.ExpatError:
            print url
            print 'expat error'

    def generate_api_url(self, settings):
        url = 'https://transparency.entsoe.eu/api?securityToken=451f590b-2a85-48b9-9845-760ef22239d3';
        for val in settings.keys():
            url += "&" + val + "=" + settings[val]
        return url

    def strip_data_if_needed(self, data, params):
        return [x for x in data if params['end'] >= x['end'] and params['start'] <= x['start']]

    def is_dst(self, date):
        local_time = pytz.timezone('Europe/Prague')
        return local_time.localize(date).dst().seconds > 1000


class Format_data:

    def __init__(self,params,dst):
        self.dst = dst
        self.params = params


    def format_data(self, data):
        response = []
        periods = self.find('Period', data)
        for single_period in periods:
            self.time_step = self.__get_time_step(single_period)
            response += self.format_period(single_period)
        response.sort(key=lambda d: d['start'])
        return response

    def format_period(self, period):
        lst = []
        start = datetime.strptime(period['timeInterval']['start'], '%Y-%m-%dT%H:%MZ')
        if self.time_step == 15:
            start += timedelta(minutes=3*self.time_step)
        for point in self.find("Point", period):
            for single in point:
                if not isinstance(single, dict):
                    single = point
                    single_point_dict = self.__get_single_point_dict(single, start)
                    lst.append(single_point_dict)
                    break
                single_point_dict = self.__get_single_point_dict(single, start)
                lst.append(single_point_dict)
        return lst

    def __get_single_point_dict(self, single, start):
        single_point_dict = {}
        dst_shift = 1 if self.is_dst(start) else 0
        time_shift_from_start = int(single['position']) * self.time_step
        single_point_dict['start'] = start + timedelta(hours=(dst_shift),minutes=time_shift_from_start)
        single_point_dict['end'] = start + timedelta(hours=(dst_shift),minutes=time_shift_from_start + self.time_step)
        single_point_dict['quantity'] = single['quantity']
        single_point_dict['type'] = self.params['type']
        return single_point_dict

    def __get_time_step(self, period):
        start = ''
        end = ''
        for x in self.find('timeInterval',period):
            for y in self.find('start',x):
                start = datetime.strptime(y,'%Y-%m-%dT%H:%MZ')
            for y in self.find('end',x):
                end = datetime.strptime(y,'%Y-%m-%dT%H:%MZ')
        minutes_diff =(end-start).days*24*60 + (end-start).seconds/60
        point_count = [x for x in self.find('Point',period)]
        return minutes_diff/len(point_count[0])

    def find(self, key, value):
        for k, v in value.iteritems():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in self.find(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in self.find(key, d):
                        yield result

    def is_dst(self, date):
        local_time = pytz.timezone('Europe/Prague')
        return local_time.localize(date).dst().seconds > 1000