#! /usr/bin/env python

import cesmEnvLib
import argparse, os
import toolTemplate
import graph
import cylc_template

def parseArgs(argv = None):

    desc = "This tool parses the CESM Env to create to Cylc Workflow.  It then creates the Cylc Suite."

    parser = argparse.ArgumentParser(prog='CESM_Cylc_setup',
                                     description=desc)
    parser.add_argument('-p','--path',default=None, type=str,
                        help='Path to output Cylc Suite.  It will be create if it does not exist.', required=True)
    parser.add_argument('-s','--suite',default=None, type=str,
                        help='Name of the ioutput Cylc Suite.', required=True)

    parser.add_argument('-g','--graph',default=None, type=str,
                        help='Name of output graph image.', required=False) 
    return parser.parse_args(argv)


def main(argv=None):

    args = parseArgs(argv)

    caseroot = os.getcwd()+'/../' 

    env_file_list = ['env_run.xml', 'env_batch.xml', 'env_case.xml', 'env_mach_pes.xml',
                     'env_build.xml', 'postprocess/env_postprocess.xml', 'postprocess/env_diags_atm.xml', 
                     'postprocess/env_diags_ocn.xml', 'postprocess/env_diags_lnd.xml',
                     'postprocess/env_diags_ice.xml']  

    env = cesmEnvLib.readXML(caseroot, env_file_list)
    print '\n'
    print 'RESUBMIT: ', env['RESUBMIT']
    print 'RUN_STARTDATE: ', env['RUN_STARTDATE']
    print 'STOP_N: ', env['STOP_N']
    print 'STOP_OPTION: ', env['STOP_OPTION']
    print 'DOUT_S: ', env['DOUT_S']
    print 'DOUT_L_MS: ', env['DOUT_L_MS']
    print 'CASEROOT: ', env['CASEROOT']
    print '\n'
    print 'GENERATE_TIMESERIES: ', env['GENERATE_TIMESERIES']
    print '\n'
    print 'GENERATE_AVGS_ATM: ', env['GENERATE_AVGS_ATM']
    print 'GENERATE_DIAGS_ATM: ', env['GENERATE_DIAGS_ATM']
    print 'ATMDIAG_test_first_yr: ', env['ATMDIAG_test_first_yr']
    print 'ATMDIAG_test_nyrs: ', env['ATMDIAG_test_nyrs']
    print 'ATMDIAG_TEST_TIMESERIES: ', env['ATMDIAG_TEST_TIMESERIES']
    print '\n'
    print 'GENERATE_AVGS_OCN: ', env['GENERATE_AVGS_OCN']
    print 'GENERATE_DIAGS_OCN: ', env['GENERATE_DIAGS_OCN']
    print 'OCNDIAG_YEAR0: ', env['OCNDIAG_YEAR0']
    print 'OCNDIAG_YEAR1: ', env['OCNDIAG_YEAR1']
    print 'OCNDIAG_TSERIES_YEAR0: ', env['OCNDIAG_TSERIES_YEAR0']
    print 'OCNDIAG_TSERIES_YEAR1: ', env['OCNDIAG_TSERIES_YEAR1']
    print 'OCNDIAG_MODELCASE_INPUT_TSERIES: ', env['OCNDIAG_MODELCASE_INPUT_TSERIES']
    print '\n'
    print 'GENERATE_AVGS_LND: ', env['GENERATE_AVGS_LND']
    print 'GENERATE_DIAGS_LND: ', env['GENERATE_DIAGS_LND']
    print 'LNDDIAG_clim_first_yr_1: ', env['LNDDIAG_clim_first_yr_1']
    print 'LNDDIAG_clim_num_yrs_1: ', env['LNDDIAG_clim_num_yrs_1']
    print 'LNDDIAG_trends_first_yr_1: ',env['LNDDIAG_trends_first_yr_1']
    print 'LNDDIAG_trends_num_yrs_1: ', env['LNDDIAG_trends_num_yrs_1']
    print 'LNDDIAG_CASE1_TIMESERIES: ', env['LNDDIAG_CASE1_TIMESERIES']
    print '\n'
    print 'GENERATE_AVGS_ICE: ', env['GENERATE_AVGS_ICE']
    print 'GENERATE_DIAGS_ICE: ', env['GENERATE_DIAGS_ICE']
    print 'ICEDIAG_BEGYR_DIFF: ', env['ICEDIAG_BEGYR_DIFF']
    print 'ICEDIAG_ENDYR_DIFF: ', env['ICEDIAG_ENDYR_DIFF']
    print 'ICEDIAG_DIFF_TIMESERIES: ', env['ICEDIAG_DIFF_TIMESERIES']
    print '\n'

    keys = ['cesm', 'sta', 'lta', 'tseries', 'avg_atm', 'diag_atm',
            'avg_ocn', 'diag_ocn', 'avg_lnd', 'diag_lnd', 'avg_ice', 'diag_ice']

    template = {}
    for tool in keys:
        template[tool] = toolTemplate.toolTemplate(tool, env)
        print tool,template[tool].specs

    for tool in keys:
        toolTemplate.align_dates(template['cesm'].specs, template[tool].specs, tool)


    g = graph.create_graph(keys,template)
    cylc_template.create_cylc_input(g, env, 'suite.rc')

    cylc_template.setup_suite(args.path, args.suite, args.graph)

if __name__ == '__main__':
    main()
