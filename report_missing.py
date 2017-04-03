

import main as mn
import data_params as dp
from datetime import datetime,timedelta


class Find_missing_data(object):

    def __init__(self):
        self.data = []
        pass

    def find_missing(self):
        self.params = {}
        self.params['type'] = 'generation'
        self.__download_data()
        # self.__find_missing_data()

    def __download_data(self):
        for country in dp.get_all_generation_countries():
            print 'Begin country: ' + country
            self.params['country'] = country
            for gen_type in dp.get_all_generation_types():
                print ' == Begin type: ' + dp.psr_types[gen_type]
                self.params['type_secondary'] = dp.psr_types[gen_type]
                self.params['start'] = datetime(2016,1,1,0,0)#datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=105)
                self.params['end'] = datetime.now().replace(microsecond=0, second=0, minute=0)  # params['start'] + timedelta(days=2)
                mn.get_all_dates_for_params(self.params)
            self.data = mn.saved_data
            self.__find_missing_data()
            mn.saved_data = {}

    def __find_missing_data(self):
        self.__get_all_sources()
        self.__find_missing_sources()
        self.__find_missing_hours()
        self.__format_missing_data()
        self.__print_missing_overview()

    def __get_all_sources(self):
        self.all_sources = list(set([v['data_type_secondary'] for k,v in self.data.iteritems()]))

    def __find_missing_sources(self):
        self.all_dates = list(set([v['date'] for k,v in self.data.iteritems()]))
        self.full_miss_source_day =[]
        for src in self.all_sources:
            for dt in self.all_dates:
                if 0 == len([k for k,v in self.data.iteritems() if v['date'] == dt and v['data_type_secondary'] == src]):
                    self.full_miss_source_day += [{'date':dt,'source':src}]



    def __find_missing_hours(self):
        self.miss_hours_source_day =[]

        for src in self.all_sources:
            for dt in self.all_dates:
                if dt == datetime.now().replace(hour=0,minute=0).strftime('%Y-%m-%d'):
                    continue
                if 0 == len([k for k, v in self.data.iteritems()
                             if v['date'] == dt
                             and v['data_type_secondary'] == src
                             and len(v['content']) ==24]):
                    false_data = [v for k,v in self.data.iteritems() if v['date'] ==dt and v['data_type_secondary']==src]
                    if len(false_data)==0:
                        continue
                    false_data = false_data[0]
                    miss_hours = [hr for hr in range(0,24,1) if hr not in false_data['content']]
                    self.miss_hours_source_day += [{'date':dt,'source':src,'missing_hours':miss_hours}]

    def __format_missing_data(self):
        self.miss_hours_source_day += [{'date':x['date'],'source':x['source'],'missing_hours':range(0,24,1)} for x in self.full_miss_source_day]
        self.miss_source = {}
        self.miss_intervals = {}
        for x in self.miss_hours_source_day:
            for hr in x['missing_hours']:
                dt = datetime.strptime(x['date'] + ' '+ str(hr)+':00','%Y-%m-%d %H:%M')
                if x['source'] not in self.miss_source:
                    self.miss_source[x['source']] = [dt]
                else:
                    self.miss_source[x['source']] += [dt]
        for k,v in self.miss_source.iteritems():
            starts = [x for x in v if (x-timedelta(hours=1)) not in v]
            for start in starts:
                end = start
                while (end +timedelta(hours=1) in v):
                    end +=+timedelta(hours=1)
                if k in self.miss_intervals:
                    self.miss_intervals[k] += [{'source':k,'start':start,'end':end}]
                else:
                    self.miss_intervals[k] = []
                    self.miss_intervals[k] += [{'source': k, 'start': start, 'end': end}]
        for k,v in self.miss_intervals.iteritems():
            sorted_data = sorted(v, key=lambda k: k['start'])
            self.miss_intervals[k] = sorted_data

    def __print_missing_overview(self):
        self.overview =  ''
        self.overview += self.params['country']+"\n"
        for k,v in self.miss_intervals.iteritems():
            self.overview += "\nProduction type: "+ k
            for interval in v:
                self.overview += "\n\tFrom: "+ interval['start'].strftime('%Y-%m-%d %H:%M')
                self.overview += "\tTo: " + (interval['end'] + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')
        with open('miss_reports/miss_report'+self.params['country']+'.txt', 'w') as file:
            file.write(self.overview)
        print self.overview

def main():
    fmd = Find_missing_data()
    fmd.find_missing()

if __name__ == '__main__':
    main()
