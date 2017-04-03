
import main as mn
import data_params as dp
from datetime import datetime,timedelta
import ML

class Download_generation(object):

    def __init__(self):
        self.data = []
        pass

    def download_generation(self):
        params = {}
        mn.parse_arguments()
        params['type'] = 'generation'
        for country in dp.get_all_generation_countries():
            print 'Begin country: ' + country
            params['country'] = country
            # ml = ML.ML(country)
            # self.data = ml.data
            # self.approx_data = ml.approximated_data
            # self.all_sources = ml.all_sources
            for gen_type in dp.get_all_generation_types():
                print ' == Begin type: ' + dp.psr_types[gen_type]
                params['type_secondary'] = dp.psr_types[gen_type]
                params['start'] = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=mn.get_argv().days_back)
                params['end'] = datetime.now().replace(microsecond=0, second=0, minute=0)  # params['start'] + timedelta(days=2)
                self.__get_all_dates_for_params(params)
            # self.__append_aproximation_data(mn.saved_data)
            # ml.close_database_connection()
            mn.save_all_data(params)

    def __get_all_dates_for_params(self,params):
        temp_start = params['start']
        while params['start'] < params['end']:
            pars = params.copy()
            pars['end'] = params['start'] + timedelta(days=1) if (params['start'] + timedelta(days=1)) < params['end'] else \
            params['end']
            params['start'] += timedelta(days=1)
            params['start'] = params['start'].replace(hour=0)
            # if self.__is_already_downloaded(pars) or (pars['type_secondary'] not in self.all_sources and len(self.all_sources)>0):
            #     continue
            mn.get_and_append_data(pars)
        params['start'] = temp_start

    def __is_already_downloaded(self,pars):
        dt = pars['start'].strftime('%Y-%m-%d')
        vals = [k for k,v in self.data.iteritems() if dt in k and pars['type_secondary'] in v]
        return len(vals) == 24

    def __append_aproximation_data(self,to_save):
        for k,v in to_save.iteritems():
            from_approx = {k_:v_ for k_,v_ in self.approx_data.iteritems() if v['data_type_secondary'] in v_ and v['date'] in k_}
            approximation = [datetime.strptime(k_,'%Y-%m-%d %H:%M').hour for k_,v_ in from_approx.iteritems()]
            v['approximation'] = approximation
            addit_content = {datetime.strptime(k_,'%Y-%m-%d %H:%M').hour:v_[v['data_type_secondary']] for k_,v_ in from_approx.iteritems()}
            for k_,v_ in addit_content.iteritems():
                if k_ not in v['content']:
                    v['content'][k_] = v_
        mn.saved_data = to_save
