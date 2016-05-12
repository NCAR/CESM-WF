import string


class toolTemplate(object):

    def __init__(self, tool_type, env):

        self.specs = self.tool_specs(tool_type, env)

    def tool_specs(self, tool_type, env):

        if (tool_type == 'cesm'):
            specs = self.cesm_specs(env)

        elif (tool_type == 'sta'):
            specs = self.sta_specs(env)

        elif (tool_type == 'lta'):
            specs = self.lta_specs(env)
        
        elif (tool_type == 'tseries'):
            specs = self.tseries_specs(env)
        
        elif (tool_type == 'avg_atm'):
            specs = self.avg_atm_specs(env)
        
        elif (tool_type == 'diag_atm'):
            specs = self.diag_atm_specs(env)

        elif (tool_type == 'avg_ocn'):
            specs = self.avg_ocn_specs(env)

        elif (tool_type == 'diag_ocn'):
            specs = self.diag_ocn_specs(env)

        elif (tool_type == 'avg_lnd'):
            specs = self.avg_lnd_specs(env)

        elif (tool_type == 'diag_lnd'):
            specs = self.diag_lnd_specs(env)

        elif (tool_type == 'avg_ice'):
            specs = self.avg_ice_specs(env)

        elif (tool_type == 'diag_ice'):
            specs = self.diag_ice_specs(env)

        else:
            print 'Error: Tool Type Not Recognized ', tool_type

        return specs



    def adjust_date(self, year, month, day):
    
        days = {1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}

        if month > 12:
            month = month - 12
            year = year + 1                
        if day > days[month]:
            day = day - days[month]
            month = month + 1
        if month > 12:
            month = month - 12
            year = year + 1
        
        return year, month, day


    def next_date(self, last_date, n, tper):
  
 
        date_split = last_date.split('-')
        year = int(date_split[0])
        month = int(date_split[1])
        day = int(date_split[2])

        if 'nday' in tper:
            day = day + int(n)
        elif 'nmonth' in tper:
            month = month + int(n)
        elif 'nyear' in tper:
            year = year + int(n)

        # Make the date correct if it went over boundaries
        year, month, day = self.adjust_date(year, month, day)

        return string.zfill(str(year),4)+'-'+string.zfill(str(month),2)+'-'+string.zfill(str(day),2)

    def find_last(self, env):

        # Last date
        last = (env['RUN_STARTDATE'])
        for i in range(0,int(env['RESUBMIT'])+1):
            last = self.next_date(last,  env['STOP_N'],  env['STOP_OPTION'])

        return last 

    def cesm_specs(self, env):

        specs = {}
        date_queue = []
        date_queue.append(self.next_date(env['RUN_STARTDATE'],env['STOP_N'],  env['STOP_OPTION']))
        for i in range(0,int(env['RESUBMIT'])):
            date_queue.append(self.next_date(date_queue[i], env['STOP_N'],  env['STOP_OPTION']))

        if env['DOUT_S'] == 'TRUE':
            dependancy = 'sta'
        else:
            dependancy = 'cesm' 
        
        specs['date_queue'] = date_queue
        specs['dependancy'] = dependancy
        return specs


    def sta_specs(self, env):

        specs = {}
        if env['DOUT_S'] == 'TRUE':
            date_queue = []
            date_queue.append(self.next_date(env['RUN_STARTDATE'],env['STOP_N'],  env['STOP_OPTION']))
            for i in range(0,int(env['RESUBMIT'])):
                date_queue.append(self.next_date(date_queue[i], env['STOP_N'],  env['STOP_OPTION']))

            dependancy = 'cesm'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs


    def lta_specs(self, env):

        specs = {}
        if env['DOUT_L_MS'] == 'TRUE':
            date_queue = []
            date_queue.append(self.find_last(env))  

            dependancy = 'all'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy    
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs



    def tseries_specs(self, env):

        specs = {}
        if env['GENERATE_TIMESERIES'] == 'TRUE':
            date_queue = []
            date_queue.append(self.find_last(env))

            dependancy = 'sta'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs



    def  avg_atm_specs(self, env):

        specs = {}
        if env['GENERATE_AVGS_ATM'] == 'TRUE':
            # Get the last year needed (this will be the yr+1 to make sure jan and feb 
            # for the next year have been calculated). 
            year = int(env['ATMDIAG_test_first_yr']) + int(env['ATMDIAG_test_nyrs']) + 1 
            date_s = str(year)+'-01-01'
            date_queue = [date_s]

            if env['ATMDIAG_TEST_TIMESERIES'] == 'True':
                dependancy = 'tseries'
            else:
                dependancy = 'sta'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def diag_atm_specs(self, env):

        specs = {}
        if env['GENERATE_DIAGS_ATM'] == 'TRUE' and env['GENERATE_AVGS_ATM'] == 'TRUE':
            # Get the last year needed (this will be the yr+1 to make sure jan and feb 
            # for the next year have been calculated). 
            year = int(env['ATMDIAG_test_first_yr']) + int(env['ATMDIAG_test_nyrs']) + 1
            date_s = str(year)+'-01-01'
            date_queue = [date_s]

            dependancy = 'avg_atm'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def avg_ocn_specs(self, env):

        specs = {}
        if env['GENERATE_AVGS_OCN'] == 'TRUE':
            # Get the last year needed
            if int(env['OCNDIAG_YEAR1']) > int(env['OCNDIAG_TSERIES_YEAR1']): 
                year = env['OCNDIAG_YEAR1']
            else:
                year = env['OCNDIAG_TSERIES_YEAR1'] 
            date_s = year+'-01-01'
            date_queue = [date_s]

            if env['OCNDIAG_MODELCASE_INPUT_TSERIES'] == 'TRUE':
                dependancy = 'tseries'
            else:
                dependancy = 'sta'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def  diag_ocn_specs(self, env):

        specs = {}
        if env['GENERATE_DIAGS_OCN'] == 'TRUE' and env['GENERATE_AVGS_OCN'] == 'TRUE':
            # Get the last year needed 
            if int(env['OCNDIAG_YEAR1']) > int(env['OCNDIAG_TSERIES_YEAR1']):
                year = env['OCNDIAG_YEAR1']
            else:
                year = env['OCNDIAG_TSERIES_YEAR1']
            date_s = year+'-01-01'
            date_queue = [date_s]

            dependancy = 'avg_ocn'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs



    def avg_lnd_specs(self, env):

        specs = {}
        if env['GENERATE_AVGS_LND'] == 'TRUE':
            # Get the last year needed
            climYear = int(env['LNDDIAG_clim_first_yr_1']) + int(env['LNDDIAG_clim_num_yrs_1']) + 1
            trendYear = int(env['LNDDIAG_trends_first_yr_1']) + int(env['LNDDIAG_trends_num_yrs_1']) + 1
            if climYear > trendYear:
                year = climYear
            else:
                year = trendYear
            date_s = str(year)+'-01-01'
            date_queue = [date_s]

            if env['LNDDIAG_CASE1_TIMESERIES'] == 'True':
                dependancy = 'tseries'
            else:
                dependancy = 'sta'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs



    def  diag_lnd_specs(self, env):

        specs = {}
        if env['GENERATE_DIAGS_LND'] == 'TRUE' and env['GENERATE_AVGS_LND'] == 'TRUE':
            # Get the last year needed 
            climYear = int(env['LNDDIAG_clim_first_yr_1']) + int(env['LNDDIAG_clim_num_yrs_1']) + 1
            trendYear = int(env['LNDDIAG_trends_first_yr_1']) + int(env['LNDDIAG_trends_num_yrs_1']) + 1
            if climYear > trendYear:
                year = climYear
            else:
                year = trendYear
            date_s = str(year)+'-01-01'
            date_queue = [date_s]

            dependancy = 'avg_lnd'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

    def avg_ice_specs(self, env):

        specs = {}
        if env['GENERATE_AVGS_ICE'] == 'TRUE':
            year = env['ICEDIAG_ENDYR_DIFF']
            date_s = year+'-01-01'
            date_queue = [date_s]

            if env['ICEDIAG_DIFF_TIMESERIES'] == 'True':
                dependancy = 'tseries'
            else:
                dependancy = 'sta'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

    def  diag_ice_specs(self, env):

        specs = {}
        if env['GENERATE_DIAGS_ICE'] == 'TRUE' and env['GENERATE_AVGS_ICE'] == 'TRUE':
            year = env['ICEDIAG_ENDYR_DIFF']
            date_s = year+'-01-01'
            date_queue = [date_s]

            dependancy = 'avg_ice'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

def align_dates(control, test, tool):

    test['actual_dates'] = {}
    for d in test['date_queue']:
        if d not in control['date_queue']:
            # break apart test date to get year
            d_parts = d.split('-')
            tyear = int(d_parts[0])
            found = False
            for c_d in control['date_queue']: 
                if found == False:
                    # break apart control date to get year
                    c_d_parts = c_d.split('-') 
                    cyear = int(c_d_parts[0])
                    if cyear > tyear:
                        test['actual_dates'][c_d] = d
                        i = test['date_queue'].index(d)
                        test['date_queue'].pop(i)
                        test['date_queue'].insert(i,c_d)                
                        found = True
            if found is False:
                print tool,': Could not find a CESM run that will have data for: ',d,'.  It will not be executed.'
                i = test['date_queue'].index(d)
                test['date_queue'].pop(i)  
        else:
            test['actual_dates'][d] = d
