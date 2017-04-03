import time
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
import copy

transmission = {'documentType':'A11'}
countries = {"CZE":"10YCZ-CEPS-----N",'50H':'10YDE-VE-------2','A':'10YAT-APG------L','PL':'10YPL-AREA-----S',
             'SVK':'10YSK-SEPS-----K','TenneT':'10YDE-EON------1','CH':'10YCH-SWISSGRIDZ','BE':'10YBE----------2',
             'TransnetBW':'10YDE-ENBW-----N','HU':'10YHU-MAVIR----U','IT':'10YIT-GRTN-----B','SLO':'10YSI-ELES-----O',
             'Amprion':'10YDE-RWENET---I','UA':'10Y1001A1001A869','DK':'10Y1001A1001A796','FR':'10YFR-RTE------C',
             'LU':'10YLU-CEGEDEL-NQ','NL':'10YNL----------L','SE':'10YSE-1--------K','LT':'10YLT-1001A0008Q',
             'RO':'10YRO-TEL------P','HR':'10YHR-HEP------M','SERB':'10YCS-SERBIATSOV'}

countries_generation = {
                        "CZE":"10YCZ-CEPS-----N",
                        'A':'10YAT-APG------L',
                        'PL':'10YPL-AREA-----S',
                        'GER':'10Y1001A1001A83F',
                        'SVK':'10YSK-SEPS-----K',
                        'CH':'10YCH-SWISSGRIDZ',
                        'BE':'10YBE----------2',
                        'HU':'10YHU-MAVIR----U',
                        'IT':'10YIT-GRTN-----B',
                        'SLO':'10YSI-ELES-----O',
                        'FR':'10YFR-RTE------C',
                        'NL':'10YNL----------L',
                        'BG':'10YCA-BULGARIA-R',
                        'DK':'10Y1001A1001A65H',
                        'EE':'10Y1001A1001A39I',
                        'FI':'10YFI-1--------U',
                        'GR':'10YGR-HTSO-----Y',
                        'IR':'10YIE-1001A00010',
                        'LV':'10YLV-1001A00074',
                        'LT':'10YLT-1001A0008Q',
                        'GB':'10YGB----------A',
                        'NO':'10YNO-0--------C',
                        'PT':'10YPT-REN------W',
                        'RO':'10YRO-TEL------P',
                        'SB':'10YCS-SERBIATSOV',
                        'ES':'10YES-REE------0',
                        'SE':'10YSE-1--------K'
                        # 'Amprion': '10YDE-RWENET---I',
                        # 'TenneT GER': '10YDE-EON------1',
                        # 'TransnetBW': '10YDE-ENBW-----N',
                        # '50Hertz': '10YDE-VE-------2'
                        }


transmission_countries = ['CZE-50H','CZE-A','CZE-PL','CZE-SVK','CZE-TenneT',
                          'A-CH','A-TenneT','A-TransnetBW','A-HU','A-IT','A-SLO',
                          'SVK-PL','SVK-HU',
                          'PL-50H','PL-LT','PL-SE',
                          '50H-DK',
                          'Amprion-CH','Amprion-FR','Amprion-NL',
                          'TenneT-DK','TenneT-NL','TenneT-SE',
                          'TransnetBW-CH','TransnetBW-FR',
                          'FR-BE','FR-CH','FR-IT',
                          'HU-RO','HU-SERB','HU-HR']

generation = {'documentType':'A75','processType':'A16'}

generation_forecast = {'documentType':'A71','processType':'A01'}

generation_wind_solar_forecast = {'documentType':'A69','processType':'A01'}

load_actual = {"documentType": "A65", "processType": "A16"}

load_day_forecast = {"documentType": "A65", "processType": "A01"}

psr_types = {
            'B01':'biomass',
            'B02':'lignite',
            'B03':'coal_derived_gas',
            'B04':'gas',
            'B05':'hard_coal',
            'BO6':'oil',
            'B07':'oil_shale',
            'B08':'peat',
            'B09':'geothermal',
            'B10':'hydro_pumped',
            'B11':'hydro_run',
            'B12':'hydro_reservoir',
            'B13':'Marine',
            'B14':'nuclear',
            'B15':'other_res',
            'B16':'solar',
            'B17':'waste',
            'B18':'wind_offshore',
            'B19':'wind_onshore',
            'B20':'other'
             }

def get_load_actual(country):
    res = copy.deepcopy(load_actual)
    res["outBiddingZone_Domain"] = countries_generation[country]
    return res

def get_load_day_forecast(country):
    res = copy.deepcopy(load_day_forecast)
    res["outBiddingZone_Domain"] = countries_generation[country]
    return res

def get_transmission(country_in,country_out):
    res = copy.deepcopy(transmission)
    res['in_Domain'] = countries[country_in]
    res['out_Domain'] = countries[country_out]
    return res

def get_generation(country,psr_type):
    res = copy.deepcopy(generation)
    res['in_Domain'] = countries_generation[country]
    psr_code = [k for k,v in psr_types.iteritems() if v == psr_type]
    res['psrType'] = psr_code[0]
    return res

def get_generation_forecast(country):
    res = copy.deepcopy(generation_forecast)
    res['in_Domain'] = countries_generation[country]
    return res

def get_generation_wind_solar_forecast(country):
    res = copy.deepcopy(generation_wind_solar_forecast)
    res['in_Domain'] = countries_generation[country]
    return res

def get_all_transmission_areas():
    res = []
    for k in transmission_countries:
        splt = k.split('-')
        res.append({'in':splt[0],'out':splt[1]})
        res.append({'in':splt[1],'out':splt[0]})
    return res

def get_all_generation_countries():
    return [k for k in countries_generation]



def get_all_generation_types():
    return [psr for psr in psr_types]

