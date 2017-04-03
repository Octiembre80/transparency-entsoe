import collections

import ML
from grab import Grab
from datetime import datetime, timedelta
import xmltodict
import copy
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.svm import SVR
import main as mn
import time


class Ceps_data():
    def __init__(self, params):
        self.params = params
        self.url = 'http://www.ceps.cz/_layouts/15/Ceps/_Pages/GraphData.aspx?mode=xml&from={start}&to={end}&hasinterval=False&sol=13&lang=CZE&agr=HR&fnc=AVG&ver=RT&para1=all&'
        self.mapping = {}
        self.data = []
        self.sources = {'PE': '', "PPE": '', 'JE': 'nuclear', 'VE': ['hydro_run', 'hydro_reservoir'],
                        'PVE': ['hydro_pumped'], 'AE': '', 'ZE': '', 'VTE': ['wind_onshore'], 'FVE': ['solar']}

    def get_data(self):
        self.__grab_data()
        self.__create_mapping()
        self.__format_data()
        return self.data

    def __grab_data(self):
        g = Grab()
        url = self.url.format(start=self.params['start'], end=self.params['end'])
        resp = g.go(url)
        self.grab_output = xmltodict.parse(resp.body)

    def __create_mapping(self):
        for x in self.__find('serie', self.grab_output):
            for y in x:
                self.mapping[y['@id']] = y['@name'][:y['@name'].index(' ')]
        print self.mapping

    def __format_data(self):
        for k in self.__find('item', self.grab_output):
            for x in k:
                item = {}
                item['start'] = datetime.strptime(x['@date'][:-6], '%Y-%m-%dT%H:%M:%S')
                item['end'] = item['start'] + timedelta(hours=1)
                item['type'] = 'generation'
                self.__create_part_from_date(item, x)

    def __create_part_from_date(self, item, x):
        for y, z in x.iteritems():
            if 'value' in y and self.mapping[y[1:]] != 'ZE':
                part = copy.deepcopy(item)
                part['type_secondary'] = self.mapping[y[1:]]
                part['quantity'] = int(round(float(z)))
                # print str(self.mapping[y[1:]]) + '   '+ str(z)
                self.data.append(part)

    def __find(self, key, value):
        for k, v in value.iteritems():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in self.__find(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in self.__find(key, d):
                        yield result


class Ceps_data_Simple(object):

    def __init__(self):
        self.simple_append = {'wind_onshore': 'VTE', 'hydro_pumped': 'PVE', 'hydro_run': 'VE', #'nuclear': 'JE',
                              'solar': 'FVE', 'hydro_reservoir': ''}
        self.simple_append_reverse = {v: k for k, v in self.simple_append.iteritems()}
        self.ml_object = ML.ML('CZE')
        self.entsoe_data = self.ml_object.data
        self.missing_dates = {}
        self.__get_ceps_data()
        self.ml_object.close_database_connection()

    def fill_missing_where_possible(self):
        self.__find_missing()
        self.__filter_entsoe_data()
        self.__save_simple_data()
        return

    def __find_missing(self):
        self.all_country_sources = [x for x in self.ml_object.all_sources]
        self.missing_dates = {k: v for k, v in self.entsoe_data.iteritems() if
                              len([x for x in self.all_country_sources if x not in v]) > 0}
        for k, v in self.missing_dates.iteritems():
            for y in [x for x in self.all_country_sources if x not in v]:
                self.missing_dates[k][y] = 'NaN'

    def __save_simple_data(self):
        data = self.__format_missing_data()
        simple_data = [x for x in data if x['data_type_secondary'] in self.simple_append]
        for x in simple_data:
            value = self.__find_in_ceps_data(x)
            known_values = self.__find_other_day_values(x)
            self.__update_data_to_db(x, value, known_values)

        params = {'country': self.ml_object.country, 'type': 'generation','type_secondary':'mish-mash'}
        mn.save_all_data(params)

    def __find_in_ceps_data(self, x):
        # print x
        hrs = [k for k, v in x['content'].iteritems() if v == 'NaN']
        res = []
        for hour in hrs:
            dict_append = {hour: ceps['quantity'] for ceps in self.ceps_data
                           if x['date'].replace(hour=hour) == ceps['start']
                           and self.simple_append[x['data_type_secondary']] == ceps['type_secondary']}
            if self.simple_append[x['data_type_secondary']] == '':
                dict_append = {hour:0}
            res.append(dict_append)
        # print 'res' + str(res)
        return res

    def __find_other_day_values(self, x):
        return {datetime.strptime(k, '%Y-%m-%d %H:%M').hour: v[x['data_type_secondary']]
                for k, v in self.entsoe_data.iteritems()
                if x['date'].replace(hour=0) == datetime.strptime(k, '%Y-%m-%d %H:%M').replace(hour=0)}

    def __update_data_to_db(self, x, new_val, known_val):
        for item in new_val:
            for k, v in item.iteritems():
                known_val[k] = v
        data = {}
        data['date'] = x['date'].strftime('%Y-%m-%d')
        data['country'] = x['country']
        data['data_type_secondary'] = x['data_type_secondary']
        data['data_type'] = x['data_type']
        data['approximation'] = []
        data['content'] = known_val
        mn.saved_data[len(mn.saved_data)] = data

    def __format_missing_data(self):
        data = []
        for k, v in self.missing_dates.iteritems():
            hour = datetime.strptime(k, '%Y-%m-%d %H:%M').hour
            for k_, v_ in v.iteritems():
                if 'NaN' != v_:
                    continue
                item_data = {'date': datetime.strptime(k, '%Y-%m-%d  %H:%M').replace(hour=0)}
                same_item_data = [x for x in data if x['date'] == item_data['date'] and x['data_type_secondary'] == k_]
                if len(same_item_data) == 1:
                    item_data = same_item_data[0]
                    data.remove(item_data)
                    dct = item_data['content']
                    dct[hour] = v_
                    item_data['content'] = dct
                    data.append(item_data)
                    continue
                item_data['country'] = self.ml_object.country
                item_data['data_type_secondary'] = k_
                item_data['content'] = {hour: v_}
                item_data['data_type'] = 'generation'
                data.append(item_data)
        return data

    def __get_ceps_data(self):
        input = {}
        input['start'] = datetime(2016, 1, 1, 0, 0).strftime('%d. %m. %Y %H:%M')
        input['end'] = datetime.now().strftime('%d. %m. %Y %H:%M')
        input['type'] = 'generation'
        cd = Ceps_data(input)
        self.ceps_data = cd.get_data()

    def __filter_entsoe_data(self):
        dates = [datetime.strptime(k, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d') for k, v in self.missing_dates.iteritems()]
        dates = list(set(dates))
        self.entsoe_data = {k: v for k, v in self.entsoe_data.iteritems()
                            if datetime.strptime(k, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d') in dates}

    def update_nuclear_from_ceps(self):
        mn.parse_arguments()
        nuclear_only = [x for x in self.ceps_data if
                        x['type_secondary'] == 'JE' and x['start'] > datetime.now() - timedelta(days=mn.get_argv().days_back)]
        for ceps in nuclear_only:
            curr_item = {k: v for k, v in mn.saved_data.iteritems() if v['date'] == ceps['start'].strftime('%Y-%m-%d')}
            if len(curr_item) != 0:
                v = curr_item.values()[0]
                v['content'][ceps['start'].hour] = ceps['quantity']
                mn.saved_data[curr_item.keys()[0]] = v
            else:
                data = {}
                data['date'] = ceps['start'].strftime('%Y-%m-%d')
                data['country'] = self.ml_object.country
                data['data_type_secondary'] = 'nuclear'
                data['data_type'] = 'generation'
                data['content'] = {ceps['start'].hour: ceps['quantity']}
                data['approximation'] = []
                mn.saved_data[len(mn.saved_data)] = data
                # print data
        params = {'country': self.ml_object.country, 'type': 'generation','type_secondary':'nuclear'}
        mn.save_all_data(params)


class Ceps_data_ML(object):
    def __init__(self, cd_simple):
        self.need_to_ML = ['biomass', 'gas', 'coal_derived_gas', 'hard_coal', 'lignite', 'other', 'other_res', 'waste']
        # self.need_to_ML = ['hard_coal']
        self.cd_simple = cd_simple
        self.entsoe_data = cd_simple.entsoe_data
        self.ceps_data = cd_simple.ceps_data
        self.ml_object = cd_simple.ml_object

    def fill_missing_ML(self):
        self.__find_missing()
        self.__save_ml_data()

    def __save_ml_data(self):
        for source in self.need_to_ML:
            # print '---===---'
            print 'CEPS => Source: ' +source
            self.ceps_data = self.cd_simple.ceps_data
            self.__get_ML_data(source)
            self.__fit()
            self.__filter_ceps_data()
            self.__predict_missing(source)


    def __find_missing(self):
        self.all_country_sources = [x for x in self.ml_object.all_sources]
        self.missing_dates = {k: v for k, v in self.entsoe_data.iteritems() if
                              len([x for x in self.all_country_sources if x not in v]) > 0}
        for k, v in self.missing_dates.iteritems():
            for y in [x for x in self.all_country_sources if x not in v]:
                self.missing_dates[k][y] = 'NaN'

    def __get_ML_data(self, source):
        relevant_target_data = {k: {source + '-target': v[source]} for k, v in self.entsoe_data.iteritems()
                                if k not in self.missing_dates
                                and source in v
                                and v[source] != 'NaN'}
        data_with_target = {}
        for x in self.ceps_data:
            dt = x['start'].strftime('%Y-%m-%d %H:%M')
            if dt in relevant_target_data:
                prev_dict = relevant_target_data[dt]
                prev_dict[x['type_secondary']] = x['quantity']
                data_with_target[dt] = prev_dict
        # adding week day and hour
        for k, v in data_with_target.iteritems():
            date = datetime.strptime(k, '%Y-%m-%d %H:%M')
            data_with_target[k]['weekday'] = date.weekday()
            data_with_target[k]['hour'] = date.hour
            data_with_target[k]['month'] = date.month

        self.data, self.target = [], []
        check = True
        for k, v in data_with_target.iteritems():
            v = collections.OrderedDict(sorted(v.items()))
            if check == True:
                check = False
            self.target.append([int(v_) for k_, v_ in v.iteritems() if '-target' in k_][0])
            self.data.append([int(v_) for k_, v_ in v.iteritems() if '-target' not in k_])

        self.target = np.array(self.target)
        self.data = np.array(self.data)

    def __filter_ceps_data(self):
        dates = [datetime.strptime(k, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d') for k, v in self.missing_dates.iteritems()]
        dates = list(set(dates))
        self.ceps_data = [x for x in self.ceps_data if x['start'].strftime('%Y-%m-%d') in dates]

    def __fit(self):
        X_train, X_test, y_train, y_test = train_test_split(self.data, self.target, test_size=0.33, random_state=42)
        self.lr = linear_model.LassoCV()
        self.lr.fit(X_train, y_train)

    def __predict_missing(self, source):
        data_to_save = []
        source_missing = {k: v for k, v in self.missing_dates.iteritems() if v[source] == 'NaN'}
        for k, v in source_missing.iteritems():
            data = self.__create_predict_input_data(k)
            data = np.array(data)
            new_val = self.lr.predict([data])
            x = {'date': datetime.strptime(k, '%Y-%m-%d %H:%M'), 'data_type_secondary': source}
            hour = x['date'].hour
            other_data = self.__find_other_day_values(x)
            data_to_save = self.append_other_data(data_to_save, x, other_data)
            same_day_source = [x for x in data_to_save if
                               x['data_type_secondary'] == source and x['date'].replace(hour=0) == datetime.strptime(k,
                                                                                                                     '%Y-%m-%d %H:%M').replace(
                                   hour=0)]
            if len(same_day_source) == 0:
                content = {datetime.strptime(k, '%Y-%m-%d %H:%M').hour: new_val}
                x = {'data_type_secondary': source, 'date': datetime.strptime(k, '%Y-%m-%d %H:%M').replace(hour=0),
                     'content': content, 'data_type': 'generation', 'country': self.ml_object.country,'approximation':[]}
                data_to_save.append(x)
            elif len(same_day_source) == 1:
                same_day_source[0]['content'][hour] = int(round(new_val[0], 0))
            else:
                print 'bad_values __predict_missing'
                exit()
        for x in data_to_save:
            x['date'] = x['date'].strftime('%Y-%m-%d')
            print x
            mn.saved_data[len(mn.saved_data)] = x
        params = {'country': self.ml_object.country, 'type': 'generation','type_secondary':source}
        mn.save_all_data(params)

    def __find_other_day_values(self, x):
        return {datetime.strptime(k, '%Y-%m-%d %H:%M').hour: v[x['data_type_secondary']]
                for k, v in self.entsoe_data.iteritems()
                if x['date'].replace(hour=0) == datetime.strptime(k, '%Y-%m-%d %H:%M').replace(hour=0)}

    def __create_predict_input_data(self, k):
        rel = [x for x in self.ceps_data if datetime.strptime(k, '%Y-%m-%d %H:%M') == x['start']]
        dats = {x['type_secondary']: x['quantity'] for x in rel}
        # print dats
        dats['weekday'] = datetime.strptime(k, '%Y-%m-%d %H:%M').weekday()
        dats['hour'] = datetime.strptime(k, '%Y-%m-%d %H:%M').hour
        dats['month'] = datetime.strptime(k, '%Y-%m-%d %H:%M').month
        dats = collections.OrderedDict(sorted(dats.items()))
        return [int(v_) for k_, v_ in dats.iteritems() if '-target' not in k_]

    def append_other_data(self, data_to_save, input_x, other_data):
        not_empty = {k: v for k, v in other_data.iteritems() if v != 'NaN'}
        relevant_data = [x for x in data_to_save if x['date'] == input_x['date'].replace(hour=0)
                         and x['data_type_secondary'] == input_x['data_type_secondary']]
        if len(relevant_data) == 0:
            content = not_empty
            x = {'data_type_secondary': input_x['data_type_secondary'], 'date': input_x['date'].replace(hour=0),
                 'content': content, 'data_type': 'generation', 'country': self.ml_object.country}
            data_to_save.append(x)
        elif len(relevant_data) == 1:
            for k, v in not_empty.iteritems():
                relevant_data[0]['content'][k] = v
        else:
            print '__append_other_data'
            exit()
        return data_to_save


if __name__ == '__main__':
    main()
