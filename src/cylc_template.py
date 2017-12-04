import os
import json

# The run commands to run each of the tasks
commands = {'case_build': 'case.build', 'case_run': 'case.run.cylc', 'case_st_archive': 'case.st_archive', 'case_lt_archive': 'case.lt_archive',
            'xconform': 'postprocess/xconform',
            'timeseries': 'postprocess/timeseries', 
            'timeseriesL': 'postprocess/timeseriesL',
            'atm_averages': 'postprocess/atm_averages', 'atm_diagnostics': 'postprocess/atm_diagnostics',
            'ocn_averages': 'postprocess/ocn_averages', 'ocn_diagnostics': 'postprocess/ocn_diagnostics', 
            'lnd_averages': 'postprocess/lnd_averages', 'lnd_diagnostics': 'postprocess/lnd_diagnostics',
            'ice_averages': 'postprocess/ice_averages', 'ice_diagnostics': 'postprocess/ice_diagnostics'}


def create_cylc_input(graph, env, path):

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

    if ensemble:
        count = int(env['end'])-int(env['start'])+1 
        f.write('#!Jinja2 \n'+
            '{% set MEMBERS = '+ str(count) +' %} \n'+
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
                else:
                    d = d + t.depends[i]
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
        f.write('        pre-script = \"cd '+cr+'\"\n')
    if 'True' in env['build']:
        if ensemble:
            f.write('    [[case_build__{{I}} ]]\n')
            f.write('    {% set d = \"'+cr+'.\" %}\n')
            f.write('        script = cd {{d}}{{j}}; {{d}}{{j}}/case.build\n')
        else:
            f.write('    [[case_build]]\n')
            f.write('        script = '+cr+'/case.build\n')
        if 'cheyenne' in env['machine_name']:
            if 'case_run' in task or 'case_st_archive' in task:
                f.write('        [[[job]]]\n'+
                    '                method = '+env['batch_type']+'\n'+
                    '                execution time limit = PT12H\n'+
                    '        [[[directives]]]\n')
            else: 
                f.write('        [[[job]]]\n'+
                        '                method = slurm\n'+
                        '                execution time limit = PT12H\n'+
                        '        [[[directives]]]\n')
            for d,i in enumerate(env['directives']['case_st_archive']):
                if 'select' in d:
                    env['directives']['case_st_archive'][i] = env['directives']['case_st_archive'][i]+',inception=login'
        else:
            f.write('        [[[job]]]\n'+
                    '                method = '+env['batch_type']+'\n'+
                    '        [[[directives]]]\n')
        for d in env['directives']['case_st_archive']:
                f.write('                '+d+'\n')
        f.write('        [[[event hooks]]]\n'+
                '                started handler = cylc email-suite\n'+
                '                succeeded handler = cylc email-suite\n'+
                '                failed handler = cylc email-suite\n')

    for t in graph:
        task = t.get_id()
        task_split = task.split('_')
        tool = task_split[0]
        #print tool
        if 'atm' in tool or 'ocn' in tool or 'lnd' in tool or 'ice' in tool or 'case' in tool:
            tool = tool + '_' + task_split[1]
            if 'archive' in task_split[2]:
               tool = tool + '_' + task_split[2] 
        #print tool
        if ensemble:
            f.write('    [['+task+'__{{I}} ]]\n')
            f.write('    {% set d = \"'+cr+'.\" %}\n')
            f.write('        script = cd {{d}}{{j}}; {{d}}{{j}}/'+commands[tool]+'\n')
        else:
            f.write('    [['+task+' ]]\n')
            f.write('        script = '+cr+'/'+commands[tool]+'\n')

        if 'cheyenne' in env['machine_name']:
            if 'case_run' in task or 'case_st_archive' in task:
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

        if tool == 'timeseriesL':
            for d in env['directives']['timeseries']:
                f.write('                '+d+'\n')
        else:
            for d in env['directives'][tool]:
                f.write('                '+d+'\n')
        f.write('        [[[event hooks]]]\n'+
                '                started handler = cylc email-suite\n'+
                '                succeeded handler = cylc email-suite\n'+
                '                failed handler = cylc email-suite\n')
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

