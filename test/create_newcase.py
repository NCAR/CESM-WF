#! /usr/bin/env python


def create_cesm_case(cesm_code_base, new_case):

    email = os.environ['USER']+'@ucar.edu'

    cesm_xml={
	     'RESUBMIT': '10',
	     'STOP_N': '10',
	     'STOP_OPTION': 'nyears',
	     'DOUT_S': 'TRUE'
	     }

    pp_xml={
	   'GENERATE_TIMESERIES': 'FALSE',
	   'GENERATE_AVGS_ATM': 'FALSE',
	   'GENERATE_DIAGS_ATM': 'FALSE',
	   'ATMDIAG_test_first_yr': '5',
	   'ATMDIAG_test_nyrs': '20',
	   'ATMDIAG_TEST_TIMESERIES': 'FALSE',
	   'GENERATE_AVGS_OCN': 'FALSE',
	   'GENERATE_DIAGS_OCN': 'FALSE',
	   'OCNDIAG_YEAR0': '10',
	   'OCNDIAG_YEAR1': '30',
	   'OCNDIAG_TSERIES_YEAR0': '10',
	   'OCNDIAG_TSERIES_YEAR1': '30',
	   'OCNDIAG_MODELCASE_INPUT_TSERIES': 'FALSE',
	   'GENERATE_AVGS_LND': 'FALSE',
	   'GENERATE_DIAGS_LND': 'FALSE',
	   'LNDDIAG_clim_first_yr_1': '40',
	   'LNDDIAG_clim_num_yrs_1': '20',
	   'LNDDIAG_trends_first_yr_1': '40',
	   'LNDDIAG_trends_num_yrs_1': '20',
	   'LNDDIAG_CASE1_TIMESERIES': 'FALSE',
	   'GENERATE_AVGS_ICE': 'FALSE',
	   'GENERATE_DIAGS_ICE': 'FALSE',
	   'ICEDIAG_BEGYR_DIFF': '40',
	   'ICEDIAG_ENDYR_DIFF': '65',
	   'ICEDIAG_BEGYR_CONT': '40',
	   'ICEDIAG_ENDYR_CONT': '65',
	   'ICEDIAG_DIFF_TIMESERIES': 'FALSE',
	   'ICEDIAG_YRS_TO_AVG': '20'
	   }


    #############################################################################
    #
    # Do not make any changes below this line
    #
    #############################################################################
    import os, sys

    cdir = os.getcwd()
    print '############################################'
    print ''
    print '   Create CESM Case'
    print ''
    print '############################################'
    print ''
    os.chdir(cesm_code_base+'/cime/scripts/')
    os.system(new_case)
    nc_split = new_case.split(' ')
    for i in range(0,len(nc_split)):
	if '-case' in nc_split[i]:
	    case_root = nc_split[i+1]
    os.chdir(case_root)
    os.system('./case.setup')


    print '############################################'
    print ''
    print '   Create CESM Postprocessing'
    print ''
    print '############################################'
    print ''
    os.environ['POSTPROCESS_PATH'] = cesm_code_base+'/postprocessing/' 
    activate_file = os.environ['POSTPROCESS_PATH']+'/cesm-env2/bin/activate_this.py'
    execfile(activate_file, dict(__file__=activate_file))
    os.system('create_postprocess -caseroot '+case_root)
    os.system('source deactivate')

    for k,v in cesm_xml.iteritems():
	os.system('./xmlchange '+k+'='+v)
    os.chdir(case_root+'/postprocess/')
    for k,v in pp_xml.iteritems():
	os.system('./pp_config --set '+k+'='+v)

    os.chdir(cdir)
