import ceps_data as cd
import time
import datetime
import ML
import data_params as dp


def repair():
    # update_ceps()
    update_entsoe()

def update_ceps():
    cd_simple = cd.Ceps_data_Simple()
    cd_simple.update_nuclear_from_ceps()
    cd_simple.fill_missing_where_possible()
    cd_ml = cd.Ceps_data_ML(cd_simple)
    cd_ml.fill_missing_ML()

def update_entsoe():
    for country in dp.countries_generation.keys():
        if country =='CZE':
            continue
        params = {}
        params['country'] = country
        ml = ML.ML_Generation(params)
        ml.fill_missing()







