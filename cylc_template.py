
commands = {'cesm': 'case.run', 'sta': 'case.st_archive', 'lta': 'case.lt_archive',
            'tseries': 'postprocess/timeseries', 'avg_atm': 'atm_averages', 'diag_atm': 'atm_diagnostics',
            'avg_ocn': 'ocn_averages', 'diag_ocn': 'ocn_diagnostics', 
            'avg_lnd': 'lnd_averages', 'diag_lnd': 'lnd_diagnostics',
            'avg_ice': 'ice_averages', 'diag_ice': 'ice_diagnostics'}


def create_cylc_input(graph, env, fn):

    cr = env['CASEROOT']
    f = open(fn, 'w')
    
    # add header
    f.write('title = '+env['CASE']+' workflow \n'+
            '[cylc]\n'+
            '    [[environment]]\n'+
            '        MAIL_ADDRESS=mickelso@ucar.edu\n'+
            '    [[event hooks]]\n'+
            '        shutdown handler = cylc email-suite\n')

    # add dependencies
    f.write('[scheduling]\n'+
            '    [[dependencies]]\n'+
            '        graph = \"\"\"\n')
    for t in graph:
            d = t.get_id()+' => '
            if len(t.depends) > 0:
                for i in range(0,len(t.depends)):
                    d = d + t.depends[i]
                    if i < len(t.depends)-1:
                        d = d + ' & '           
                f.write('                    '+d+'\n')
    f.write('               \"\"\"\n')

    # add run time - REPLACE WITH REAL DIRECTIVES
    f.write('[runtime]\n'+
            '    [[root]]\n'+
            '        pre-script = \"cd '+cr+'\"\n')
    for t in graph:
        task = t.get_id()
        task_split = task.split('_')
        tool = task_split[0]
        #print tool
        if 'avg' in tool:
            tool = tool + '_' + task_split[1] 
        if 'diag' in tool:
            tool = tool + '_' + task_split[1]
        #print tool
        f.write('    [['+task+' ]]\n')
        f.write('        script = '+cr+'/'+commands[tool]+'\n')
        f.write('        [[[job submission]]]\n'+
                '                method = lsf\n'+
                '        [[[directives]]]\n'+
                '                -n = 180\n'+
                '                -q = regular\n'+
                '                -W = 01:30\n'+
                '                -R = \"span[ptile=15]\"\n'+ 
                '                -P = STDD0002\n'+
                '        [[[event hooks]]]\n'+
                '                started handler = cylc email-suite\n'+
                '                succeeded handler = cylc email-suite\n'+
                '                failed handler = cylc email-suite\n')
            

def setup_suite(path, suite_name, image=None):

    import subprocess
    import os
    from shutil import copyfile

    # Make suite directory if it does not exist
    if not os.path.exists(path):
        os.makedirs(path)

    # Copy the suite file over
    cwd = os.getcwd() 
    copyfile(cwd+'/suite.rc', path+'/suite.rc') 

    # Register the suite
    cmd = 'cylc register '+suite_name+' '+path  
    print cmd
    os.system(cmd)

    # Validate the suite
    cmd = 'cylc validate '+suite_name
    os.system(cmd)

    if (image):
       # Show a graph
       cmd = 'cylc graph -O '+image+'.png --output-format=png '+suite_name
       os.system(cmd)

