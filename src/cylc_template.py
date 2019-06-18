import os
import json

# The run commands to run each of the tasks
commands = {'case_build': 'case.build', 'case_run': 'case.run.cylc', 'case_st_archive': 'case.st_archive.cylc', 'case_lt_archive': 'case.lt_archive',
            'xconform': 'postprocess/xconform',
            'timeseries': 'postprocess/timeseries', 
            'timeseriesL': 'postprocess/timeseriesL',
            'atm_averages': 'postprocess/atm_averages', 'atm_diagnostics': 'postprocess/atm_diagnostics',
            'ocn_averages': 'postprocess/ocn_averages', 'ocn_diagnostics': 'postprocess/ocn_diagnostics', 
            'lnd_averages': 'postprocess/lnd_averages', 'lnd_diagnostics': 'postprocess/lnd_diagnostics',
            'ice_averages': 'postprocess/ice_averages', 'ice_diagnostics': 'postprocess/ice_diagnostics'}


def create_cylc_input(graph, env, path, queue, exp_name):

    if env['start'] != env['end']:
        ensemble=True
        cr = env['ensemble_root']
    else:
        cr = env['CASEROOT']
        ensemble=False 

    # Make suite directory if it does not exist
    if not os.path.exists(path):
        os.makedirs(path)

    fn = path+'/suite.rc'
    f = open(fn, 'w')
    tfn = path+'/tasklist.json'
    members = {}
    crf = os.path.basename(cr)
    print 'WRITING ',fn

    f.write('#!Jinja2 \n')

    # get dates
    dates = {}
    for t in graph:
        task = t.get_id()
        task_split = task.split('_')
        tool = task_split[0]
        if 'atm' in tool or 'ocn' in tool or 'lnd' in tool or 'ice' in tool or 'case' in tool:
            tool = tool + '_' + task_split[1]
            if 'archive' in task_split[2]:
               tool = tool + '_' + task_split[2]
        if tool not in dates.keys():
            dates[tool] = [task_split[-1]]
        else:
            dates[tool].append(task_split[-1])
  
    # write out dates at the top of the suite.rc file
    for tool in sorted(dates.keys()):
        f.write('{% set dates_'+tool+' = '+str(dates[tool])+' %}\n')
    f.write('{% set ATMDIAG_test_first_yr = ['+env['ATMDIAG_test_first_yr']+'] %}\n')
    f.write('{% set ATMDIAG_test_nyrs = ['+env['ATMDIAG_test_nyrs']+'] %}\n')
    f.write('{% set OCNDIAG_YEAR0 = ['+env['OCNDIAG_YEAR0']+'] %}\n')
    f.write('{% set OCNDIAG_YEAR1 = ['+env['OCNDIAG_YEAR1']+'] %}\n')
    f.write('{% set OCNDIAG_TSERIES_YEAR0 = ['+env['OCNDIAG_TSERIES_YEAR0']+'] %}\n')
    f.write('{% set OCNDIAG_TSERIES_YEAR1 = ['+env['OCNDIAG_TSERIES_YEAR1']+'] %}\n')
    f.write('{% set LNDDIAG_clim_first_yr_1 = ['+env['LNDDIAG_clim_first_yr_1']+'] %}\n')
    f.write('{% set LNDDIAG_trends_first_yr_1 = ['+env['LNDDIAG_trends_first_yr_1']+'] %}\n')
    f.write('{% set LNDDIAG_clim_num_yrs_1 = ['+env['LNDDIAG_clim_num_yrs_1']+'] %}\n')
    f.write('{% set LNDDIAG_trends_num_yrs_1 = ['+env['LNDDIAG_trends_num_yrs_1']+'] %}\n')
    f.write('{% set ICEDIAG_BEGYR_DIFF = ['+env['ICEDIAG_BEGYR_DIFF']+'] %}\n')
    f.write('{% set ICEDIAG_ENDYR_DIFF = ['+env['ICEDIAG_ENDYR_DIFF']+'] %}\n')
    f.write('{% set ICEDIAG_BEGYR_CONT = ['+env['ICEDIAG_BEGYR_CONT']+'] %}\n')
    f.write('{% set ICEDIAG_ENDYR_CONT = ['+env['ICEDIAG_ENDYR_CONT']+'] %}\n')
    f.write('{% set ICEDIAG_YRS_TO_AVG = ['+env['ICEDIAG_YRS_TO_AVG']+'] %}\n')

    if ensemble:
        count = int(env['end'])-int(env['start'])+1 
        f.write('{% set MEMBERS = '+ str(count) +' %} \n'+
            'title = '+crf+'_'+env['start']+'-'+env['end']+' workflow \n')
    else:
        f.write('title = '+crf+' workflow \n')

    if ensemble:
        for i in range(0,count):
            members[crf+'.'+str(i+1).zfill(3)] = []
    else:
        members[crf] = []
    
    # add header
    f.write('[cylc]\n'+
            '    [[environment]]\n'+
            '        MAIL_ADDRESS='+env['email']+'\n'+
            '    [[event hooks]]\n'+
            '        shutdown handler = cylc email-suite\n')

    # add dependencies
    f.write('[scheduling]\n'+
            '    [[dependencies]]\n'+
            '        graph = \"\"\"\n')
    if ensemble:
        f.write('        {% for I in range('+env['start']+', '+str(int(env['end'])+1)+') %}\n')
    first = True
    for t in graph:
        if first:
            if ensemble:
                if 'True' in env['build']:
                    f.write('                    case_build__{{I}} => '+t.get_id()+'__{{I}} \n')                
            else:
                if 'True' in env['build']:
                    f.write('                    case_build  => '+t.get_id()+'\n')
            first = False

        if ensemble:
            d = t.get_id()+'__{{I}} => ' 
            for i in range(0,count):
                members[crf+'.'+str(i+1).zfill(3)].append(t.get_id()+'__'+str(i+1))
        else:
            d = t.get_id()+' => '
            members[crf].append(t.get_id())       
 
        if len(t.depends) > 0:
            for i in range(0,len(t.depends)):
                if ensemble:
                    d = d + t.depends[i]+'__{{I}}'
                    if 'diagnostics' in t.depends[i]:
                        d = d + ' => ' + t.depends[i]+'_post__{{I}}'
                else:
                    d = d + t.depends[i]
                    if 'diagnostics' in t.depends[i]:
                        d = d + ' => ' + t.depends[i]+'_post'
                if i < len(t.depends)-1:
                    d = d + ' & '           
            f.write('                    '+d+'\n')
    with open (tfn, "w") as ts:
        json.dump({crf:members}, ts, indent=4) 
    ts.close()
    if ensemble:
        f.write('        {% endfor %}\n')
    f.write('               \"\"\"\n')

    # add run time - REPLACE WITH REAL DIRECTIVES
    f.write('[runtime]\n'+
            '    [[root]]\n')
    if ensemble:
        f.write('        [[[environment]]]\n')
        f.write('            MEMBERS = {{MEMBERS}}\n')
        f.write('    {% for I in range('+env['start']+', '+str(int(env['end'])+1)+') %}\n')
        f.write('    {% set j = I | pad (3,\'0\') %}\n') 
    else:
        #f.write('        pre-script = \"cd '+cr+'\"\n')
        f.write('        [[[environment]]]\n')

    for t in sorted(dates.keys()):
        f.write('        {% for i in range(0,dates_'+t+'|length) %}\n')
        if ensemble:
            f.write('    [['+t+'_{{dates_'+t+'[i]}}__{{I}} ]]\n')
            f.write('    {% set d = \"'+cr+'.\" %}\n')
            pp = ''
            if 'averages' in t:
                if 'atm' in t:
                    pp = pp+'./pp_config --set ATMDIAG_test_first_yr={{ATMDIAG_test_first_yr[i]}}; '
                    pp = pp+'./pp_config --set ATMDIAG_test_nyrs={{ATMDIAG_test_nyrs[i]}}; '
                elif 'ocn' in t:
                    pp = pp+'./pp_config --set OCNDIAG_YEAR0={{OCNDIAG_YEAR0[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_YEAR1={{OCNDIAG_YEAR1[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_TSERIES_YEAR0={{OCNDIAG_TSERIES_YEAR0[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_TSERIES_YEAR1={{OCNDIAG_TSERIES_YEAR1[i]}}; '
                elif 'lnd' in t:
                    pp = pp+'./pp_config --set LNDDIAG_clim_first_yr_1={{LNDDIAG_clim_first_yr_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_trends_first_yr_1={{LNDDIAG_trends_first_yr_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_clim_num_yrs_1={{LNDDIAG_clim_num_yrs_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_trends_num_yrs_1={{LNDDIAG_trends_num_yrs_1[i]}}; '
                elif 'ice' in t:
                    pp = pp+'./pp_config --set ICEDIAG_BEGYR_DIFF={{ICEDIAG_BEGYR_DIFF[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_ENDYR_DIFF={{ICEDIAG_ENDYR_DIFF[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_BEGYR_CONT={{ICEDIAG_BEGYR_CONT[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_ENDYR_CONT={{ICEDIAG_ENDYR_CONT[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_YRS_TO_AVG={{ICEDIAG_YRS_TO_AVG[i]}}; '
                f.write('        script = cd {{d}}{{j}}/postprocess/; '+pp+' {{d}}{{j}}/'+commands[t]+'\n')
            else:
                f.write('        script = cd {{d}}{{j}}; {{d}}{{j}}/'+commands[t]+'\n')
        else:
            f.write('        [['+t+'_{{dates_'+t+'[i]}} ]]\n')
            pp = ''
            if 'averages' in t:
                if 'atm' in t:
                    pp = pp+'./pp_config --set ATMDIAG_test_first_yr={{ATMDIAG_test_first_yr[i]}}; '
                    pp = pp+'./pp_config --set ATMDIAG_test_nyrs={{ATMDIAG_test_nyrs[i]}}; '
                elif 'ocn' in t:
                    pp = pp+'./pp_config --set OCNDIAG_YEAR0={{OCNDIAG_YEAR0[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_YEAR1={{OCNDIAG_YEAR1[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_TSERIES_YEAR0={{OCNDIAG_TSERIES_YEAR0[i]}}; '
                    pp = pp+'./pp_config --set OCNDIAG_TSERIES_YEAR1={{OCNDIAG_TSERIES_YEAR1[i]}}; '
                elif 'lnd' in t:
                    pp = pp+'./pp_config --set LNDDIAG_clim_first_yr_1={{LNDDIAG_clim_first_yr_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_trends_first_yr_1={{LNDDIAG_trends_first_yr_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_clim_num_yrs_1={{LNDDIAG_clim_num_yrs_1[i]}}; '
                    pp = pp+'./pp_config --set LNDDIAG_trends_num_yrs_1={{LNDDIAG_trends_num_yrs_1[i]}}; '
                elif 'ice' in t:
                    pp = pp+'./pp_config --set ICEDIAG_BEGYR_DIFF={{ICEDIAG_BEGYR_DIFF[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_ENDYR_DIFF={{ICEDIAG_ENDYR_DIFF[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_BEGYR_CONT={{ICEDIAG_BEGYR_CONT[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_ENDYR_CONT={{ICEDIAG_ENDYR_CONT[i]}}; '
                    pp = pp+'./pp_config --set ICEDIAG_YRS_TO_AVG={{ICEDIAG_YRS_TO_AVG[i]}}; '

                f.write('        script = cd '+cr+'/postprocess/; '+pp+' '+cr+'/'+commands[t]+'\n')
            else:
                f.write('        script = cd '+cr+'; '+cr+'/'+commands[t]+'\n')

        if 'cheyenne' in env['machine_name']:
            if 'case_run' in task or 'case_st_archive' in task or 'geyser' not in env['pp_machine_name'] or 'caldera' not in env['pp_machine_name']:
                if 'case_st_archive' in t:
                        f.write('        [[[job]]]\n'+
                        '                method = '+env['batch_type']+'\n'+
                        '                execution time limit = PT1H\n'+
                        '        [[[directives]]]\n')
                elif 'case_run' in t:
                        f.write('        [[[job]]]\n'+
                        '                method = '+env['batch_type']+'\n'+
                        '                execution time limit = PT12H\n'+
                        '                execution retry delays = PT30S, PT120S, PT600S\n'+
                        '        [[[directives]]]\n')
                else:
                        f.write('        [[[job]]]\n'+
                        '                method = '+env['batch_type']+'\n'+
                        '                execution time limit = PT12H\n'+
                        '        [[[directives]]]\n')
            else:
                f.write('        [[[job]]]\n'+
                        '                method = slurm\n'+
                        '        [[[directives]]]\n')

        else:
            f.write('        [[[job]]]\n'+
                    '                method = '+env['batch_type']+'\n'+
                    '        [[[directives]]]\n')

        if t == 'timeseriesL':
            for d in env['directives']['timeseries']:
                f.write('                '+d+'\n')
        elif t == 'case_run':
            for d in env['directives']['case_run']:
                if '-q' in d and 'None' not in queue:
                    d = d.replace(d.split()[-1],queue)
                elif '-N' in d:
                    d = d.replace(d.split()[-1],exp_name)    
                f.write('                '+d+'\n')
        else:
            for d in env['directives'][t]:
                f.write('                '+d+'\n')
        f.write('        [[[event hooks]]]\n'+
                '                started handler = cylc email-suite\n'+
                '                succeeded handler = cylc email-suite\n'+
                '                failed handler = cylc email-suite\n')
        f.write('        {% endfor %}\n')

        if 'diagnostics' in t:
            f.write('\n\n')
            f.write('        {% for i in range(0,dates_'+t+'|length) %}\n')
            if ensemble:
                f.write('    [['+t+'_{{dates_'+t+'[i]}}_post__{{I}} ]]\n')
                f.write('    {% set d = \"'+cr+'.\" %}\n')
                f.write('    script = cd {{d}}{{j}}/postprocess/; {{d}}{{j}}/postprocess/copy_html\n') 
            else:
                f.write('        [['+t+'_{{dates_'+t+'[i]}}_post ]]\n')            
                f.write('        script = cd '+cr+'/postprocess/; '+cr+'/postprocess/copy_html\n')
            f.write('        {% endfor %}\n')
        f.write('\n\n')
    if ensemble:
        f.write('    {% endfor %}\n')            
 

def setup_suite(path, suite_name, image=None):

    import subprocess
    import os
    from shutil import copyfile

    # Register the suite
    cmd = 'cylc register '+suite_name+' '+path  
    os.system(cmd)

    # Validate the suite
    cmd = 'cylc validate '+suite_name
    os.system(cmd)

    if (image):
       # Show a graph
       suffix = image.split('.')[-1]
       cmd = 'cylc graph -O '+path+'/'+image+' --output-format='+suffix+' '+suite_name
       os.system(cmd)

