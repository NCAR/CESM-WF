import string


class toolTemplate(object):

    def __init__(self, tool_type, env):

        self.specs = self.tool_specs(tool_type, env)

    def tool_specs(self, tool_type, env):

        # Loop through all tools and see if they will be ran and when  

        if (tool_type == 'case_run'):
            specs = self.cesm_specs(env)

        elif (tool_type == 'case_st_archive'):
            specs = self.sta_specs(env)

        elif (tool_type == 'case_lt_archive'):
            specs = self.lta_specs(env)
        
        elif (tool_type == 'timeseries'):
            specs = self.tseries_specs(env)
       
        elif (tool_type == 'timeseriesL'):
            specs = self.tseriesL_specs(env)

        elif (tool_type == 'xconform'):
            specs = self.xconform_specs(env)
 
        elif (tool_type == 'atm_averages'):
            specs = self.avg_atm_specs(env)
        
        elif (tool_type == 'atm_diagnostics'):
            specs = self.diag_atm_specs(env)

        elif (tool_type == 'ocn_averages'):
            specs = self.avg_ocn_specs(env)

        elif (tool_type == 'ocn_diagnostics'):
            specs = self.diag_ocn_specs(env)

        elif (tool_type == 'lnd_averages'):
            specs = self.avg_lnd_specs(env)

        elif (tool_type == 'lnd_diagnostics'):
            specs = self.diag_lnd_specs(env)

        elif (tool_type == 'ice_averages'):
            specs = self.avg_ice_specs(env)

        elif (tool_type == 'ice_diagnostics'):
            specs = self.diag_ice_specs(env)

        else:
            print 'Error: Tool Type Not Recognized ', tool_type

        return specs



    def adjust_date(self, year, month, day):
   
        # Make sure we have an actual calendar day and adjust if needed
 
        days = {1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}

        # Check and adjust months greater than 12
        if month > 12:
            month = month - 12
            year = year + 1                
        # Check and adjust days that are greater than the total for that month
        if day > days[month]:
            day = day - days[month]
            month = month + 1
        # Check again to make sure we didn't just go over the month count
        if month > 12:
            month = month - 12
            year = year + 1
        
        return year, month, day


    def next_date(self, last_date, n, tper):
  
        # Find the next day in the CESM cycle

        # Split apart the date to get year, month, and day 
        date_split = last_date.split('-')
        year = int(date_split[0])
        month = int(date_split[1])
        day = int(date_split[2])

        # Adjust the correct portion of the date based on tper
        if 'day' in tper:
            day = day + int(n)
        elif 'month' in tper:
            if n >= 12:
                m = int(n)%12
                year = year + (int(n)/12)
            else:
                m = n
            month = month + int(m)
        elif 'year' in tper:
            year = year + int(n)

        # Make the date correct if it went over boundaries
        year, month, day = self.adjust_date(year, month, day)

        return string.zfill(str(year),4)+'-'+string.zfill(str(month),2)+'-'+string.zfill(str(day),2)

    def find_last(self, env):

        # Last date

        # First get the start date
        last = (env['RUN_STARTDATE'])

        # Since we are using the last date for each CESM simulation, we need to loop 
        # through resubmit+1
        for i in range(0,int(env['RESUBMIT'])+1):
            last = self.next_date(last,  env['STOP_N'],  env['STOP_OPTION'])

        return last

    def check_djf(self, dyr, dfirst, env):

        # Check to see if we are creating enough data for a djf climatology
        # This is used for the atm and lnd diags

        # Get the last date that will be simulated
        last = self.find_last(env)

        # If the last year is longer than what we're creating diags for, we're okay
        if dyr < last:
            return True
        # If we need more years than CESM is simulating, we're not okay
        elif dyr > last:
            return False
        # If the diags need the last year simulated, check to see if we creating at least
        # one year more than needed on the front half.
        elif dyr == last:
            first = env['RUN_STARTDATE'].split("-")[0]
            if first > dfirst:
                return True 

    def cesm_specs(self, env):

        # Add all of the last dates for each CESM run.  Set the dependancy based on wether or not sta
        # will be ran.
        specs = {}
        date_queue = []
        date_queue.append(self.next_date(env['RUN_STARTDATE'],env['STOP_N'],  env['STOP_OPTION']))
        for i in range(0,int(env['RESUBMIT'])):
            date_queue.append(self.next_date(date_queue[i], env['STOP_N'],  env['STOP_OPTION']))

        if env['DOUT_S'] :
            dependancy = 'case_st_archive'
        else:
            dependancy = 'case_run' 
        
        specs['date_queue'] = date_queue
        specs['dependancy'] = dependancy
        return specs


    def sta_specs(self, env):

        # If the short term archiver will be ran, it will be dependant on CESM and will be invoked
        # after each CESM run
        specs = {}
        if 'TRUE' in env['DOUT_S'] :
            date_queue = []
            date_queue.append(self.next_date(env['RUN_STARTDATE'],env['STOP_N'],  env['STOP_OPTION']))
            for i in range(0,int(env['RESUBMIT'])):
                date_queue.append(self.next_date(date_queue[i], env['STOP_N'],  env['STOP_OPTION']))

            dependancy = 'case_run'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs


    def lta_specs(self, env):

        # If the long term archiver will be ran, it will be the last item inserted in the dependancy chain
        specs = {}
        if 'TRUE' in env['DOUT_L_MS'] :
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
        if 'TRUE' in env['GENERATE_TIMESERIES'] :
            date_queue = []
            date_queue.append(self.next_date(env['RUN_STARTDATE'],env['TIMESERIES_N'],env['TIMESERIES_TPER']))
        
            for i in range(0,int(env['RESUBMIT'])-1,int(env['TIMESERIES_RESUBMIT'])):
                date_queue.append(self.next_date(date_queue[i], env['TIMESERIES_N'], env['TIMESERIES_TPER']))    

            dependancy = 'case_st_archive'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        print specs
        return specs


    def tseriesL_specs(self, env):

        specs = {}
        if 'TRUE' in env['GENERATE_TIMESERIES'] :
            date_queue = []
            date_queue.append(self.find_last(env))

            dependancy = 'case_st_archive'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs


    def xconform_specs(self, env):

        specs = {}
        if 'TRUE' in env['STANDARDIZE_TIMESERIES']:        
            date_queue = []
            date_queue.append(self.find_last(env))

            dependancy = 'timeseriesL'
           
            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''
        return specs


    def  avg_atm_specs(self, env):

        # If the atm averages will be ran, it will depend on either tseries or sta, based on
        # the tseries variable in the diag_atm file.  It will be inserted based on the last day 
        # needed for the diags. 
        specs = {}
        if 'TRUE' in env['GENERATE_AVGS_ATM'] and env['ATMDIAG_test_first_yr'].isdigit() and env['ATMDIAG_test_nyrs'].isdigit():
            # Get the last year needed (this will be the yr+1 to make sure jan and feb 
            # for the next year have been calculated). 
            year = int(env['ATMDIAG_test_first_yr']) + int(env['ATMDIAG_test_nyrs'])+1  
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if self.check_djf(year, int(env['ATMDIAG_test_first_yr']), env):
                if 'TRUE' in env['ATMDIAG_TEST_TIMESERIES'] :
                    dependancy = 'timeseries'
                else:
                    dependancy = 'case_st_archive'

                specs['date_queue'] = date_queue
                specs['dependancy'] = dependancy
            else:
                specs['date_queue'] = []
                specs['dependancy'] = ''
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def diag_atm_specs(self, env):

        # If the atm diags will be ran, it will depend on avg_atm. It will be inserted after avg_atm.
        specs = {}
        if 'TRUE' in env['GENERATE_DIAGS_ATM']  and 'TRUE' in env['GENERATE_AVGS_ATM'] and env['ATMDIAG_test_first_yr'].isdigit() and env['ATMDIAG_test_nyrs'].isdigit():
            # Get the last year needed (this will be the yr+1 to make sure jan and feb 
            # for the next year have been calculated). 
            year = int(env['ATMDIAG_test_first_yr']) + int(env['ATMDIAG_test_nyrs'])+1 
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if self.check_djf(year, int(env['ATMDIAG_test_first_yr']), env):
                dependancy = 'atm_averages'

                specs['date_queue'] = date_queue
                specs['dependancy'] = dependancy
            else:
                specs['date_queue'] = []
                specs['dependancy'] = ''
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def avg_ocn_specs(self, env):

        # If the ocn averages will be ran, it will depend on either tseries or sta, based on
        # the tseries variable in the diag_ocn file.  It will be inserted based on the last day 
        # needed for the diags. 
        specs = {}
        if 'TRUE' in env['GENERATE_AVGS_OCN'] and env['OCNDIAG_YEAR1'].isdigit() and env['OCNDIAG_TSERIES_YEAR1'].isdigit():
            # Get the last year needed
            if int(env['OCNDIAG_YEAR1']) > int(env['OCNDIAG_TSERIES_YEAR1']): 
                year = int(env['OCNDIAG_YEAR1'])+1 
            else:
                year = int(env['OCNDIAG_TSERIES_YEAR1'])+1 
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if 'TRUE' in env['OCNDIAG_MODELCASE_INPUT_TSERIES'] :
                dependancy = 'timeseries'
            else:
                dependancy = 'case_st_archive'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs


    def  diag_ocn_specs(self, env):

        # If the atm diags will be ran, it will depend on avg_ocn. It will be inserted after avg_ocn. 
        specs = {}
        if 'TRUE' in env['GENERATE_DIAGS_OCN']  and 'TRUE' in env['GENERATE_AVGS_OCN'] and env['OCNDIAG_YEAR1'].isdigit() and env['OCNDIAG_TSERIES_YEAR1'].isdigit():
            # Get the last year needed 
            if int(env['OCNDIAG_YEAR1']) > int(env['OCNDIAG_TSERIES_YEAR1']):
                year = int(env['OCNDIAG_YEAR1'])+1 
            else:
                year = int(env['OCNDIAG_TSERIES_YEAR1'])+1 
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            dependancy = 'ocn_averages'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs



    def avg_lnd_specs(self, env):

        # If the lnd averages will be ran, it will depend on either tseries or sta, based on
        # the tseries variable in the diag_lnd file.  It will be inserted based on the last day 
        # needed for the diags. 
        specs = {}
        if 'TRUE' in env['GENERATE_AVGS_LND'] and env['LNDDIAG_clim_first_yr_1'].isdigit() and env['LNDDIAG_clim_num_yrs_1'].isdigit() and env['LNDDIAG_trends_first_yr_1'].isdigit() and env['LNDDIAG_trends_num_yrs_1'].isdigit():
            # Get the last year needed
            climYear = int(env['LNDDIAG_clim_first_yr_1']) + int(env['LNDDIAG_clim_num_yrs_1'])+1 
            trendYear = int(env['LNDDIAG_trends_first_yr_1']) + int(env['LNDDIAG_trends_num_yrs_1'])+1 
            if climYear > trendYear:
                year = climYear
                firstyr = int(env['LNDDIAG_clim_first_yr_1'])
            else:
                year = trendYear
                firstyr = int(env['LNDDIAG_trends_first_yr_1'])
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if self.check_djf(year, firstyr, env):
                if 'TRUE' in env['LNDDIAG_CASE1_TIMESERIES'] :
                    dependancy = 'timeseries'
                else:
                    dependancy = 'case_st_archive'

                specs['date_queue'] = date_queue
                specs['dependancy'] = dependancy
            else:
                specs['date_queue'] = []
                specs['dependancy'] = ''
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs



    def  diag_lnd_specs(self, env):

        # If the atm diags will be ran, it will depend on avg_lnd. It will be inserted after avg_lnd.
        specs = {}
        if 'TRUE' in env['GENERATE_DIAGS_LND']  and 'TRUE' in env['GENERATE_AVGS_LND'] and env['LNDDIAG_clim_first_yr_1'].isdigit() and env['LNDDIAG_clim_num_yrs_1'].isdigit() and env['LNDDIAG_trends_first_yr_1'].isdigit() and env['LNDDIAG_trends_num_yrs_1'].isdigit():
            # Get the last year needed 
            climYear = int(env['LNDDIAG_clim_first_yr_1']) + int(env['LNDDIAG_clim_num_yrs_1'])+1 
            trendYear = int(env['LNDDIAG_trends_first_yr_1']) + int(env['LNDDIAG_trends_num_yrs_1'])+1 
            if climYear > trendYear:
                year = climYear
                firstyr = int(env['LNDDIAG_clim_first_yr_1'])
            else:
                year = trendYear
                firstyr = int(env['LNDDIAG_trends_first_yr_1'])
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if self.check_djf(year, firstyr, env):
                dependancy = 'lnd_averages'
         
                specs['date_queue'] = date_queue
                specs['dependancy'] = dependancy
            else:
                specs['date_queue'] = []
                specs['dependancy'] = ''
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

    def avg_ice_specs(self, env):

        # If the ice averages will be ran, it will depend on either tseries or sta, based on
        # the tseries variable in the diag_ice file.  It will be inserted based on the last day 
        # needed for the diags.
        specs = {}
        if 'TRUE' in env['GENERATE_AVGS_ICE'] and env['ICEDIAG_ENDYR_DIFF'].isdigit():
            year = int(env['ICEDIAG_ENDYR_DIFF'])+1 
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            if 'TRUE' in env['ICEDIAG_DIFF_TIMESERIES'] :
                dependancy = 'timeseries'
            else:
                dependancy = 'case_st_archive'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

    def  diag_ice_specs(self, env):

        # If the atm diags will be ran, it will depend on avg_ice. It will be inserted after avg_ice. 
        specs = {}
        if 'TRUE' in env['GENERATE_DIAGS_ICE']  and 'TRUE' in env['GENERATE_AVGS_ICE'] and env['ICEDIAG_ENDYR_DIFF'].isdigit():
            year = int(env['ICEDIAG_ENDYR_DIFF'])+1
            date_s = string.zfill(str(year),4)+'-01-01'
            date_queue = [date_s]

            dependancy = 'ice_averages'

            specs['date_queue'] = date_queue
            specs['dependancy'] = dependancy
        else:
            specs['date_queue'] = []
            specs['dependancy'] = ''

        return specs

def align_dates(control, test, tool):

    # Go through the dates for the tool and CESM and create another list that will
    # be used to insert the task into the dependency chain at a time that CESM runs.
    # This list is internal to the system to get correct alignment and is not used for 
    # anything else.
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
