import os, sys, math, subprocess, glob
        
import cesmEnvLib
from standard_script_setup          import *
from CIME.case import Case
from CIME.utils import transform_vars
from CIME.XML.machines import Machines
from CIME.task_maker import TaskMaker
from CIME.XML.batch                 import Batch
from CIME.XML.env_run               import EnvRun
from CIME.XML.env_case              import EnvCase
from CIME.XML.env_build             import EnvBuild        
        
class EnvCylc():
        
    def __init__(self):
        self.env = {}    

    def get_date(self, runDir, doutDir):

        dates = {}
        rpointers = glob.glob(str(runDir)+'/rpointer.*')
        if len(rpointers) < 1:
            print 'Could not find any rpointer files in: ',runDir
            print 'You need to have rpointer files and the corresponding restart files if you have CONTINUE_RUN set to TRUE.'
            sys.exit(1)
        for rp in rpointers:
            f = open(rp,'r')
            for line in f:
                if '.nc' in line:
                    dates[rp] = {}
                    if './' in line:
                        dates[rp]['fn'] = (str(runDir)+'/'+line[2:]).strip()
                    else:
                        dates[rp]['fn'] = (str(runDir)+'/'+line).strip()
                    dates[rp]['date'] = line.split('.')[-2][:-6]
            f.close()

        sd = 'null'
        for d,v in dates.iteritems():
            if not os.path.isfile(v['fn']):
                print 'Restart file does not exist: ',v['fn']
                print 'This was pointed to by: ',d
                print 'Check rpointer files for errors.'
                sys.exit(1) 

            if sd == 'null':
                sd = v['date']
            else:
                if sd != v['date']:
                    print 'Check rpointer files, detected an inconsistency.'
                    print 'No Cylc workflow will be created.'
                    sys.exit(1)


        return sd


    def get_env(self):
        my_case = os.getcwd() + '/../'
        
        case = Case(my_case, read_only=False)
        
        cwd = os.getcwd()
        os.chdir(my_case)

        env_case = EnvCase()
        machine_name = env_case.get_value('MACH')
        print 'Running on ',machine_name

        env_mach = Machines(machine=machine_name)
        batch_system = env_mach.get_value("BATCH_SYSTEM")
        batch = Batch(batch_system=batch_system, machine=machine_name)
        env_run = EnvRun()
        env_build = EnvBuild() 
        env_batch = case.get_env("batch")
        os.system('./xmlchange RESUBMIT=0')
        os.chdir(cwd)
        
        directives = {}
        
        task_maker = TaskMaker(case)
        
        task_maker = TaskMaker(case)
        
        
         
        bjobs = batch.get_batch_jobs()
        for job, jsect in bjobs:
            job_ = str.replace(job,'.','_')
            directives[job_] = []
            task_count = jsect["task_count"]
            if task_count is None or task_count == "default":
                task_count = str(task_maker.totaltasks)
            else:
                task_count = int(task_count)
            queue = env_batch.select_best_queue(task_count,job)
            if queue is None:
                queue = env_batch.select_best_queue(task_maker.totaltasks,job)
            wall_time = env_batch.get_max_walltime(queue)
            if wall_time is None:
                wall_time = env_batch.get_default_walltime()
            env_batch.set_value("JOB_WALLCLOCK_TIME", wall_time)
            env_batch.set_value("JOB_QUEUE", queue)
            self.ptile = str(task_maker.ptile)
            self.totaltasks = task_count
        
            direct = '' 
            ds = env_batch.get_batch_directives(case, job, raw=True)
            dss = ds.split('\n') 
            for d in dss:  
                direct = direct + transform_vars(d, case=case, subgroup=job, check_members=self)       

            s = env_batch.get_submit_args(case, job)
            bd = env_batch.get_node("batch_directive").text
            direct = direct.replace(bd,'')
            direct = direct + s 
            direct = direct.replace('-', '\n-')
            direct = direct.split('\n')
            for d in direct:
                d.lstrip()
                d.strip()
                d = d.split(' ')
                d=' '.join(d).split()
                if len(d) == 2:
                    if ' ' not in d[0] and ' ' not in d[1]:
                        directives[job_].append(d[0]+' = '+d[1])

        self.env['directives'] = directives
        self.env['STOP_N'] = env_run.get_value("STOP_N")
        self.env['RESUBMIT'] = env_run.get_value("RESUBMIT")
        self.env['STOP_OPTION'] = env_run.get_value('STOP_OPTION')
        self.env['DOUT_S'] = env_run.get_value('DOUT_S')
        self.env['DOUT_L_MS'] = env_run.get_value('DOUT_L_MS')
        self.env['CASEROOT'] = env_case.get_value('CASEROOT')
        self.env['CASE'] = env_case.get_value('CASE')
        self.env['RUNDIR'] = env_run.get_value('RUNDIR')        
        self.env['CESMSCRATCHROOT'] = env_build.get_value('CESMSCRATCHROOT')
        self.env['USER'] = env_case.get_value('USER')
        # Resolve RUNDIR
        while '$' in str(self.env['RUNDIR']):
            split = str(self.env['RUNDIR']).split('/')
            for v in split:
                if '$' in v:
                    self.env['RUNDIR'] = str.replace(self.env['RUNDIR'],v,self.env[v[1:]])
        cont_run = env_run.get_value('CONTINUE_RUN')
        if not cont_run:
            start = env_run.get_value('RUN_STARTDATE')
        else:
            start = self.get_date(self.env['RUNDIR'],self.env['DOUT_S'])
        valid = False
        while not valid:
            choice = str(raw_input("Use start date "+start+"? y/n \n"))
            if choice == 'Y' or choice == 'y':
                valid = True
                self.env['RUN_STARTDATE'] = start
            elif choice == 'N' or choice == 'n':
                valid = True
                user_date = str(raw_input("Enter new date (format yyyy-mm-dd):\n"))
        env_run.set_value("RUN_WITH_SUBMIT", True)
       
        pp_dir = my_case+'/postprocess/'
   
        self.env['GENERATE_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_TIMESERIES', shell=True)
 
        self.env['GENERATE_AVGS_ATM'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_AVGS_ATM', shell=True)
        self.env['GENERATE_DIAGS_ATM'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_DIAGS_ATM', shell=True)
        self.env['ATMDIAG_test_first_yr'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ATMDIAG_test_first_yr', shell=True)
        self.env['ATMDIAG_test_nyrs'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ATMDIAG_test_nyrs', shell=True)
        self.env['ATMDIAG_TEST_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ATMDIAG_TEST_TIMESERIES', shell=True)

        self.env['GENERATE_AVGS_OCN'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_AVGS_OCN', shell=True)
        self.env['GENERATE_DIAGS_OCN'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_DIAGS_OCN', shell=True)
        self.env['OCNDIAG_YEAR0'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get OCNDIAG_YEAR0', shell=True)
        self.env['OCNDIAG_YEAR1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get OCNDIAG_YEAR1', shell=True)
        self.env['OCNDIAG_TSERIES_YEAR0'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get OCNDIAG_TSERIES_YEAR0', shell=True)
        self.env['OCNDIAG_TSERIES_YEAR1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get OCNDIAG_TSERIES_YEAR1', shell=True)
        self.env['OCNDIAG_MODELCASE_INPUT_TSERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get OCNDIAG_MODELCASE_INPUT_TSERIES', shell=True)
   
        self.env['GENERATE_AVGS_LND'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_AVGS_LND', shell=True)
        self.env['GENERATE_DIAGS_LND'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_DIAGS_LND', shell=True)
        self.env['LNDDIAG_clim_first_yr_1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get LNDDIAG_clim_first_yr_1', shell=True)
        self.env['LNDDIAG_clim_num_yrs_1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get LNDDIAG_clim_num_yrs_1', shell=True)
        self.env['LNDDIAG_trends_first_yr_1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get LNDDIAG_trends_first_yr_1', shell=True)
        self.env['LNDDIAG_trends_num_yrs_1'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get LNDDIAG_trends_num_yrs_1', shell=True)
        self.env['LNDDIAG_CASE1_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get LNDDIAG_CASE1_TIMESERIES', shell=True)
   
        self.env['GENERATE_AVGS_ICE'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_AVGS_ICE', shell=True)
        self.env['GENERATE_DIAGS_ICE'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_DIAGS_ICE', shell=True)
        self.env['ICEDIAG_BEGYR_DIFF'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ICEDIAG_BEGYR_DIFF', shell=True)
        self.env['ICEDIAG_ENDYR_DIFF'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ICEDIAG_ENDYR_DIFF', shell=True)
        self.env['ICEDIAG_DIFF_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get ICEDIAG_DIFF_TIMESERIES', shell=True)

        # Capitalize all true false values
        for k,v in self.env.iteritems():
            if 'directive' not in k:
                self.env[k] = str(self.env[k]).strip()
                v = str(v).strip()
                v = str(v).lstrip()
                if 'True' in v or 'False' in v or 'true' in v or 'false' in v or v == True or v == False:
                    self.env[k] = str(v).upper()

        return self.env        

def get_env():
    cylc = EnvCylc()
    env = cylc.get_env()
    return env
#for k in env.keys():
#    print k,': ',env[k]
