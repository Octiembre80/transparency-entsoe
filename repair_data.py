

from datetime import timedelta
import grab_data

def repair_data(data_input,data):
    if data_input['type'] is 'load_actual':
        rp = Load_actual_repair(data,data_input)
        return rp.repair()
    elif data_input['type'] is 'transmission':
        return data
    elif data_input['type'] == 'generation':
        rg = Generation_repair(data_input,data)
        return rg.repair()
    elif data_input['type'] == 'generation_forecast':
        rg = Generation_forecast_repair(data_input,data)
        return rg.repair()
    elif data_input['type'] == 'generation_wind_solar_forecast':
        rg = Generation_repair(data_input,data)
        return rg.repair_wind_solar_forecast()
    else:
        print "Repair not possible, data type not recognized"

class Load_actual_repair(object):

    def __init__(self,data,data_input):
        self.data = data
        self.data_input = data_input
        self.g = grab_data.Grab_data()

    def repair(self):
        if self.some_data_missing():
            self.fill_missing_data()
        self.remove_quarter_hours()

        return self.data

    def some_data_missing(self):
        if not self.data:
            return True
        diff = self.data_input['end'] - self.data_input['start']
        return diff.seconds/3600 != len(self.data)

    def fill_missing_data(self):
        missing = self.find_missing_data()
        for x in missing:
            date = self.data_input['start'].replace(hour=x)
            data = self.g.fetch_data(self.get_params(date))
            self.data += data
        self.data.sort(key=lambda d: d['start'])

    def find_missing_data(self):
        hours_have = [x['start'].hour for x in self.data]
        start = self.data_input['start']
        end = self.data_input['end']
        delta = timedelta(hours=1)
        hours_need = []
        while start < end:
            hours_need.append(start.hour)
            start += delta
        return [x for x in hours_need if x not in hours_have]

    def remove_quarter_hours(self):
        new_data = list()

        hs = self.get_single_hours()
        for x in hs:
            ls = [y for y in self.data
                  if (x['start'].day == y['start'].day
                      and x['start'].month == y['start'].month
                      and x['start'].year == y['start'].year
                      and x['start'].hour == y['start'].hour
                      and x is not y)]
            count = 1
            for y in ls:
                x['quantity'] = int(x['quantity'])+int(y['quantity'])
                count+=1
            x['quantity'] = int(x['quantity'])/count
            x['end'] = x['start'] + timedelta(hours=1)
            new_data.append(x)
        self.data = new_data
        self.data.sort(key=lambda d: d['start'])

    def get_single_hours(self):
        hs = [x for x in self.data if x['start'].minute == 0]
        for x in self.data:
            if x['start'].hour not in [y['start'].hour for y in hs]:
                x['start'] = x['start'].replace(minute=0)
                hs.append(x)
        return hs

    def get_params(self,date):
        input_data = {}
        input_data['type'] = 'load_day_forecast'
        input_data['country'] = self.data_input['country']
        input_data['start'] = date
        input_data['end'] = date + timedelta(hours=1)
        return input_data

class Generation_repair(object):

    def __init__(self,data_input,data):
        self.data_input = data_input
        self.data = data

    def repair(self):
        self.delete_consumption()
        self.remove_quarter_hours()
        self.check_wind_solar()
        return self.data

    def repair_wind_solar_forecast(self):
        self.delete_consumption()
        self.remove_quarter_hours()
        return self.data

    def delete_consumption(self):
        repaired_data = list()
        for x in self.data:
            duplicates = [y for y in repaired_data if x['start'] == y['start'] and y['end'] == x['end']]
            if len(duplicates) == 0:
                repaired_data.append(x)
        self.data = repaired_data

    def remove_quarter_hours(self):
        new_data = list()

        hs = self.get_single_hours()
        for x in hs:
            ls = [y for y in self.data if (x['start'].day == y['start'].day and x['start'].month == y['start'].month and x['start'].year == y['start'].year and x['start'].hour == y['start'].hour and x is not y)]
            count = 1
            for y in ls:
                x['quantity'] = int(x['quantity'])+int(y['quantity'])
                count+=1
            x['quantity'] = int(x['quantity'])/count
            x['end'] = x['start'] + timedelta(hours=1)
            new_data.append(x)
        self.data = new_data
        self.data.sort(key=lambda d: d['start'])

    def get_single_hours(self):
        hs = [x for x in self.data if x['start'].minute == 0]
        for x in self.data:
            if x['start'].hour not in [y['start'].hour for y in hs]:
                x['start'] = x['start'].replace(minute=0)
                hs.append(x)
        return hs

    def check_wind_solar(self):
        if self.data_input['country'] == 'CZE':
            return
        psrType = ''
        if self.data_input['type_secondary'] == 'solar':
            psrType = 'B16'
        elif self.data_input['type_secondary'] == 'wind_offshore':
            psrType = 'B18'
        elif self.data_input['type_secondary'] == 'wind_onshore':
            psrType = 'B19'
        else:
            return
        miss = [self.data_input['start'].replace(hour=x) for x in range(0,24,1) if x not in [y['start'].hour for y in self.data]]
        if len(miss)>0:
            params = {'psrType':psrType,'type':'generation_wind_solar_forecast','country':self.data_input['country'],'start':self.data_input['start'],'end':self.data_input['end']}
            gb = grab_data.Grab_data()
            dat = gb.fetch_and_repair_data(params)
            replacements = [x for x in dat if x['start'] in miss]
            self.data += replacements

class Generation_forecast_repair(object):

    def __init__(self,data_input,data):
        self.data_input = data_input
        self.data = data

    def repair(self):
        self.remove_empty_duplicate_lines()
        self.remove_quarter_hours()

        return self.data

    def remove_empty_duplicate_lines(self):
        lines_with_non_zero_quantity = [y['start'] for y in self.data if int(y['quantity']) !=0]
        non_duplicate_lines  = [x for x in self.data if int(x['quantity']) != 0 and x['start'] in lines_with_non_zero_quantity]
        self.data = non_duplicate_lines

    def remove_quarter_hours(self):
        new_data = list()

        hs = self.get_single_hours()
        for x in hs:
            ls = [y for y in self.data if (x['start'].day == y['start'].day and x['start'].month == y['start'].month and x['start'].year == y['start'].year and x['start'].hour == y['start'].hour and x is not y)]
            count = 1
            for y in ls:
                x['quantity'] = int(x['quantity'])+int(y['quantity'])
                count+=1
            x['quantity'] = int(x['quantity'])/count
            x['end'] = x['start'] + timedelta(hours=1)
            new_data.append(x)
        self.data = new_data
        self.data.sort(key=lambda d: d['start'])

    def get_single_hours(self):
        hs = [x for x in self.data if x['start'].minute == 0]
        for x in self.data:
            if x['start'].hour not in [y['start'].hour for y in hs]:
                x['start'] = x['start'].replace(minute=0)
                hs.append(x)
        return hs