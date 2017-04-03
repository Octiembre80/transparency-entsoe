import mysql.connector
from mysql.connector import DatabaseError
from mysql.connector import ProgrammingError
import json
from datetime import datetime,timedelta
import collections
import copy
from sklearn.model_selection import train_test_split
from sklearn import linear_model
import numpy as np
from random import shuffle
import main as mn



baseload = ['lignite','biomass','hard_coal','coal_derived_gas','gas','others','geothermal','waste','other_res','nuclear','hydro_run','hydro_reservoir']
wind = ['wind_onshore','wind_offshore']
missing_index = 0

def main():
    input = {}
    input['country'] = 'GER'
    input['looking_for'] = 'lignite'
    input['date'] = datetime(2017,2,28,2,0,0,0)
    input['known'] = {'wind_onshore':23965,'wind_offshore':2992,'solar':0,'nuclear':6367,'hydro_run':1148,
                      'generation':55529,'hour':input['date'].hour}
    ml = ML(input['country'])
    print ml.get_approx_value(input)

class ML(object):

    def __init__(self,country):
        self.cursor, self.link = self.__connect_db()
        self.data = {}
        self.approximated_data = {}
        self.country = country
        self.__get_all_country_data()
        self.all_sources = self.__get_all_sources()
        self.relevant = {}
        self.input_params = {}
        self.baseload = ['lignite','biomass','hard_coal','coal_derived_gas','gas','others','geothermal','waste','other_res','nuclear','hydro_run','hydro_reservoir']
        self.wind = ['wind_onshore','wind_offshore']

    def __connect_db(self):
        config = {
            'user': 'root',
            'password': 'root',
            'host': 'localhost',
            'database': 'entsoe',
            'raise_on_warnings': True,
            'port': '8889',
        }
        link = mysql.connector.connect(**config)
        cursor = link.cursor(dictionary=True)
        return cursor,link

    def __get_all_country_data(self):
        self.__get_country_generation_forecast_data()
        self.__get_country_generation_data()
        self.__get_country_missing_dates()


    def __get_country_generation_forecast_data(self):
        query_generation = 'select date, content from ' + self.country + '_generation_forecast order by date asc'
        self.cursor.execute(query_generation)
        for x in self.cursor:
            jsn = json.loads(x['content'])
            for k, v in jsn.iteritems():
                try:
                    key = datetime.strptime(str(x['date']),'%Y-%m-%d').replace(hour=int(k))
                except:
                    print k
                    print v
                    exit()
                self.data[key.strftime('%Y-%m-%d %H:%M')] = {'generation': int(v)}

    def __get_country_generation_data(self):
        query_sources = 'select date,content,type_secondary,approximation from '+self.country +'_generation order by date asc'
        try:
            self.cursor.execute(query_sources)
        except (ProgrammingError, DatabaseError):
            'Already exists'
        for x in self.cursor:
            jsn = json.loads(x['content'])
            approx = json.loads(x['approximation'])
            for k, v in jsn.iteritems():
                try:
                    key = datetime.strptime(str(x['date']), '%Y-%m-%d').replace(hour=int(k))
                except:
                    print x
                    print datetime.strptime(str(x['date']),'%Y-%m-%d')
                # save approximated data if there are some
                if int(k) in approx:
                    if key.strftime('%Y-%m-%d %H:%M') in self.approximated_data:
                        self.approximated_data[key.strftime('%Y-%m-%d %H:%M')][x['type_secondary']] = int(v)
                    else:
                        self.approximated_data[key.strftime('%Y-%m-%d %H:%M')] = {}
                        self.approximated_data[key.strftime('%Y-%m-%d %H:%M')][x['type_secondary']] = int(v)
                    continue
                if key.strftime('%Y-%m-%d %H:%M') in self.data:
                    self.data[key.strftime('%Y-%m-%d %H:%M')][x['type_secondary']] = int(v)
                else:
                    self.data[key.strftime('%Y-%m-%d %H:%M')] = {}
                    self.data[key.strftime('%Y-%m-%d %H:%M')][x['type_secondary']] = int(v)

    def __get_country_missing_dates(self):
        lowest = datetime.strptime(min(self.data.keys()),'%Y-%m-%d %H:%M')
        while (lowest <= (datetime.now()-timedelta(hours=1,minutes=30))):
            if lowest.strftime('%Y-%m-%d %H:%M') not in self.data.keys():
                self.data[lowest.strftime('%Y-%m-%d %H:%M')] = {}
            lowest = lowest + timedelta(hours=1)

    def get_approx_value(self,input_params):
        self.input_params = input_params
        self.__get_only_relevant_data()
        self.relevant = self.__merge_data(self.relevant)
        data,target,columns = self.__create_input_data()
        X_train,X_test,y_train,y_test = train_test_split(data, target, test_size = 0.1,random_state=42)
        regr = linear_model.Lasso()
        regr.fit(X_train, y_train)
        val = self.__get_predict_input()
        return regr.predict(val)

    def __get_only_relevant_data(self):
        self.relevant = {}
        for k, v in self.data.iteritems():
            add = True
            for known_k, known_v in self.input_params['known'].iteritems():
                if known_k not in v and known_k not in ['hour','weekday','month']:
                    add = False
                    break
            # print v
            if add and self.input_params['looking_for'] in v:
                self.relevant[k] = v
        new_relevant = {}
        for k,v in self.relevant.iteritems():
            new_v = {}
            for val_k,val_v in v.iteritems():
                if val_k in self.input_params['known'] or val_k == self.input_params['looking_for']:
                    new_v[val_k] = val_v
            new_v['hour'] = datetime.strptime(k,'%Y-%m-%d %H:%M').hour
            new_v['month'] = datetime.strptime(k,'%Y-%m-%d %H:%M').month
            new_v['weekday'] = datetime.strptime(k,'%Y-%m-%d %H:%M').weekday()
            new_relevant[k] = new_v
        self.relevant = new_relevant

    def __merge_data(self, input_data):
        res = {}
        for rel_k,rel_v in input_data.iteritems():
            baseload_value = 0
            wind_value = 0
            for bsl in baseload:
                if bsl in rel_v and bsl != self.input_params['looking_for']:
                    baseload_value += rel_v[bsl]
                    rel_v = remove_element_from_dict(rel_v, bsl)
            for w in wind:
                if w in rel_v and w != self.input_params['looking_for']:
                    wind_value = wind_value + rel_v[w]
                    rel_v = remove_element_from_dict(rel_v, w)
            rel_v['baseload'] = baseload_value
            rel_v['wind'] = wind_value
            res[rel_k] = collections.OrderedDict(sorted(rel_v.items()))
        input_data = res
        return input_data

    def __create_input_data(self):
        data = []
        target = []
        keys = []
        for rel_k,rel_v in self.relevant.iteritems():
            data_list = []
            for k in rel_v.keys():
                if k !=self.input_params['looking_for']:
                    data_list.append(rel_v[k])
            target.append(rel_v[self.input_params['looking_for']])
            data.append(data_list)
            keys = rel_v.keys()
        keys.remove(self.input_params['looking_for'])
        return np.array(data),target, keys

    def __evaluate(self, regr, X_test, y_test):
        # The coefficients
        print('Coefficients: \n', regr.coef_)
        # The mean squared error
        print("Mean squared error: %.2f"
              % np.mean((regr.predict(X_test) - y_test) ** 2))
        # Explained variance score: 1 is perfect prediction
        print('Variance score: %.2f' % regr.score(X_test, y_test))
        print len(X_test)
        count = 0
        for x in range(0,len(X_test)-1,1):
            prd = regr.predict([X_test[x]]) - y_test[x]
            if abs(prd) > 4000:
                # print prd
                count = count +1
        print count

    def __get_predict_input(self):
        merged_input = self.__merge_data({'1':self.input_params['known']})
        lst = []
        for k,v in merged_input['1'].iteritems():
            lst.append(v)
        return [np.array(lst)]

    def __get_all_sources(self):
        query = 'select distinct(type_secondary) from '+self.country+'_generation'
        self.cursor.execute(query)
        lst=  [x['type_secondary'] for x in self.cursor]
        return lst

    def close_database_connection(self):
        self.link.close()

class ML_Generation(object):

    def __init__(self,params):
        print 'params: ' + str(params)
        self.ml_object = ML(params['country'])
        self.params = params
        self.approximated_dates = {}

    def fill_missing(self):
        self.__get_all_country_sources()
        self.__get_missing_dates()
        self.__get_model_types()
        self.__fit_all_models()
        self.__predict_missing()
        self.__save_predicted_data()

    def __get_all_country_sources(self):
        self.relevant_sources = self.ml_object.all_sources

    def __get_missing_dates(self):
        self.missing_dates = {source: self.__find_missing_dates_for_source(source) for source in self.relevant_sources}

    def __find_missing_dates_for_source(self,source):
        start_date = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=mn.get_argv().days_back)
        return {k:v for k,v in self.ml_object.data.iteritems() if source not in v and datetime.strptime(k,'%Y-%m-%d %H:%M') > start_date}

    def __get_model_types(self):
        model_list = self.__create_basic_models()
        model_list = self.__optimize_models_by_relevant_training_data(model_list)
        self.model_types = model_list

    def __create_basic_models(self):
        model_list = []
        for k,v in self.missing_dates.iteritems():
            for k_,v_ in v.iteritems():
                model = [x for x in model_list
                         if x['missing'] == k
                         and len(x['known']) == len(v_.keys())
                         and len([src for src in x['known'] if src not in v_.keys()])==0
                         ]
                if len(model)==1:
                    model_list.remove(model[0])
                    model[0]['dates'].append(k_)
                    model[0]['dates'].sort()
                    model_list.append(model[0])
                elif len(model)==0:
                    model = {}
                    model['missing'] = k
                    model['known'] = v_.keys()
                    model['dates'] = [k_]
                    model_list.append(model)
                else:
                    print '__get_model_types except ==> more than one items'
                    exit()
        return model_list

    def __optimize_models_by_relevant_training_data(self,model_list):
        new_model_list = []
        for model in model_list:
            model_relevant_training_data = self.__get_model_relevant_training_data(model)

            while len(model_relevant_training_data) < self.__number_of_occurence_in_data(model['missing'])*0.3:
                model = self.__remove_least_occurent_source(model)
                model_relevant_training_data = self.__get_model_relevant_training_data(model)

            model['relevant_data'] = model_relevant_training_data
            new_model_list.append(model)
        return new_model_list

    def __remove_least_occurent_source(self,model):
        current_low = len(self.ml_object.data)+1
        source_to_remove = ''
        for source in model['known']:
            count = self.__number_of_occurence_in_data(source)
            if count < current_low:
                current_low = count
                source_to_remove = source
        print 'removing: '+source_to_remove
        model['known'].remove(source_to_remove)
        return model

    def __number_of_occurence_in_data(self,source):
        count = 0
        for k, v in self.ml_object.data.iteritems():
            if source in v:
                count += 1
        return count

    def __fit_all_models(self):
        for model in self.model_types:
            model_relevant_training_data = model['relevant_data']
            data,target,dates,columns = self.__get_ML_data_target(model,model_relevant_training_data)
            model['fit'] = self.__get_fitted_model(data,target)

    def __predict_missing(self):
        self.separated_missing = self.__get_sorted_missing()
        count = 0
        for item in self.separated_missing:

            predict_input = self.__get_predict_input(item['model'],item['date'])
            predicted_value = item['model']['fit'].predict(predict_input)

            self.__append_prediction_to_data_and_prediction(item, predicted_value)
            content = self.__get_content_from_day(item,predicted_value)
            approximation = self.__get_other_approximated_dates(item)

            self.__append_to_data_to_save(item,content,approximation)
            count = count + 1
            print 'DONE: '+str(count) + ' / '+str(len(self.separated_missing))+'\t'+ str(item['date']) + ' \t'+ str(item['missing'])


            # print 'DONE: \t'+str(count)+" / "+ str(len(self.separated_missing))

    def __get_model_relevant_training_data(self,model):
        return {k: v for k, v in self.ml_object.data.iteritems()
             if len([src for src in model['known'] if src not in v]) == 0
             and model['missing'] in v
             and self.__has_previous(k, model['missing'])
             }

    def __has_previous(self,date_string, missing_source):
        prev_date = (datetime.strptime(date_string,'%Y-%m-%d %H:%M') - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')
        return prev_date in self.ml_object.data \
               and missing_source in self.ml_object.data[prev_date]

    def __get_ML_data_target(self,model,relevant_data):
        data,target,dates = [],[],[]
        for k,v in relevant_data.iteritems():
            dt = datetime.strptime(k,'%Y-%m-%d %H:%M')
            data_item = [v[src] for src in model['known']] + [dt.hour,dt.weekday(),dt.month] + self.__get_previous_value(dt,model['missing'])
            data.append(data_item)
            target.append(v[model['missing']])
            dates.append(k)
        columns = (model['known']+['hour','weekday','month','prev_value'])
        columns.append(model['missing'])
        return np.array(data),np.array(target),dates,columns

    def __get_previous_value(self,date,missing_source):
        prev_date = (date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')
        if '2015-' in prev_date:
            return [self.__get_average_value(missing_source)]
        while missing_source not in self.ml_object.data[prev_date]:
            date = date - timedelta(hours=1)
            prev_date = (date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')
        return [self.ml_object.data[prev_date][missing_source]]

    def __get_average_value(self,source):
        all_values = [v[source] for k,v in self.ml_object.data.iteritems() if source in v]
        return sum(all_values)/len(all_values)

    def __get_fitted_model(self,data,target):
        # X_train, X_test, y_train, y_test = train_test_split(data, target, test_size=0.01, random_state=42)
        lasso = linear_model.Lasso()
        lasso.fit(data, target)
        return lasso

    def __get_predict_input(self, model, predict_date):
        current_data = self.ml_object.data[predict_date]
        dt = datetime.strptime(predict_date,'%Y-%m-%d %H:%M')
        predict_input = [current_data[src] for src in model['known']] + [dt.hour,dt.weekday(),dt.month] + self.__get_previous_value(dt,model['missing'])
        return [np.array(predict_input)]

    def __get_sorted_missing(self):
        self.separated_missing = []
        for model in self.model_types:
            self.separated_missing = self.separated_missing +\
                                     [{'date':x,'missing':model['missing'],'model':model} for x in model['dates']]
        return sorted(self.separated_missing , key=lambda k: k['date'])

    def __append_prediction_to_data_and_prediction(self, item, predicted_value):
        if item['date'] in self.ml_object.data:
            self.ml_object.data[item['date']][item['missing']] = int(predicted_value[0])
        else:
            self.ml_object.data[item['date']] = {}
            self.ml_object.data[item['date']][item['missing']] = int(predicted_value[0])

        if item['date'] in self.approximated_dates:
            self.approximated_dates[item['date']].append(item['missing'])
        else:
            self.approximated_dates[item['date']] = []
            self.approximated_dates[item['date']].append(item['missing'])

    def __get_content_from_day(self,item,predicted_value):
        res = {}
        item_date = datetime.strptime(item['date'],'%Y-%m-%d %H:%M')
        for k,v in self.ml_object.data.iteritems():
            k_date = datetime.strptime(k,'%Y-%m-%d %H:%M')
            if k_date.strftime('%Y-%m-%d') == item_date.strftime('%Y-%m-%d') and item['missing'] in v:
                res[k_date.hour] = v[item['missing']]
        res[str(datetime.strptime(item["date"], '%Y-%m-%d %H:%M').hour)] = int(predicted_value[0])
        return res

    def __get_other_approximated_dates(self,item):
        dt_without_hours = datetime.strptime(item['date'],'%Y-%m-%d %H:%M').strftime('%Y-%m-%d')
        approx_in_day = {k:v for k,v in self.approximated_dates.iteritems() if dt_without_hours in k and item['missing'] in v}
        # current_approx = datetime.strptime(item['date'],'%Y-%m-%d %H:%M').hour
        return [datetime.strptime(k,'%Y-%m-%d %H:%M').hour for k,v in approx_in_day.iteritems()]

    def __append_to_data_to_save(self,item,content,approximation):
        part = {}
        part['data_type'] = 'generation'
        part['country'] = self.params['country']
        part['data_type_secondary'] = item["missing"]
        part['date'] = datetime.strptime(item["date"], '%Y-%m-%d %H:%M').strftime('%Y-%m-%d')
        part['content'] = content
        part['approximation'] = approximation
        mn.saved_data[len(mn.saved_data)] = part

    def __save_predicted_data(self):
        save_params = {'country': self.params['country'], 'type': 'generation','type_secondary':'all'}
        mn.save_all_data(save_params)

def remove_element_from_dict(subject,removal):
    res = copy.deepcopy(subject)
    res.pop(removal,None)
    return res

if __name__ == '__main__':
    main()









