import os, sys, subprocess, glob
import math
        
from standard_script_setup          import *
from CIME.case import Case
from CIME.utils import transform_vars
from CIME.XML.batch                 import Batch
 
class EnvCylc():
        
    def __init__(self):
        self.env = {}    
        self.ptile = None
        self.total_tasks = None
        self.tasks_per_node = None

    def get_date(self, runDir):

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

    def get_tseries_info(self,pp_dir,stop_n, stop_option):

        import xml.etree.ElementTree as ET

        xml_tree = ET.ElementTree()
        tpers = ['hour', 'day', 'month', 'year']
        sizes = [1, 24, 720, 8760]  
        i_tper = len(tpers)-1
        i_size = 999
        s_size = i_size * 365 * 24
        xml_tree.parse(pp_dir+'/env_timeseries.xml')
        for comp_archive_spec in xml_tree.findall("components/comp_archive_spec"):        
 
            for file_spec in comp_archive_spec.findall("files/file_extension"):
                if file_spec.find("tseries_create") is not None:
                    tseries_create = file_spec.find("tseries_create").text
                if tseries_create.upper() in ["T","TRUE"]: 
                   if file_spec.find("tseries_filecat_tper") is not None:
                        tper = file_spec.find("tseries_filecat_tper").text
                        if file_spec.find("tseries_filecat_n") is not None:
                            size = file_spec.find("tseries_filecat_n").text
                            s = size.split(',')
                            t = tper.split(',')
                            for it in range(0,len(t)):
                                for i in range(0,len(tpers)):
                                    if tpers[i] in t[it]:
                                        c_size = int(sizes[i])*int(s[it])
                                        if c_size < s_size:
                                            i_tper = i
                                            i_size = s[it]
                                            s_size = c_size
        # We don't want this interval shorter than the cesm run length, if it is, set to cesm stop_n and stop_option
        for i in range(0,len(tpers)):
           if tpers[i] in stop_option:
               c_size = int(sizes[i])*int(stop_n)
               if c_size > s_size:
                   i_size = stop_n
                   i_tper = i

        return tpers[i_tper],i_size

    def get_tseries_resubmit(self, ts_tper, ts_n, stop_n, stop_option):

        tpers = ['hour', 'day', 'month', 'year']
        sizes = [1, 24, 720, 8760]
        ts = 0
        cesm = 0
        if ts_tper not in stop_option and stop_option not in ts_tper:
            for i in range(0,len(tpers)):
                if tpers[i] in ts_tper:
                    ts = int(ts_n) * sizes[i]
                if tpers[i] in stop_option:
                    cesm = int(stop_n) * sizes[i]
        else:
            ts = ts_n
            cesm = stop_n
        if int(ts)%int(cesm) > 0:
            freq = (int(ts)/int(cesm))+1
        else:
            freq = (int(ts)/int(cesm))
        return freq  
               
         

    def get_env(self, my_case, debug):
        
        case = Case(my_case, read_only=False)
        
        cwd = os.getcwd()
        os.chdir(my_case)

        machine_name = case.get_value('MACH')
        print 'Running on ',machine_name

        batch_system = case.get_value("BATCH_SYSTEM")
        batch = Batch(batch_system=batch_system, machine=machine_name)
        env_batch = case.get_env("batch")
        os.chdir(cwd)
        
        directives = {}
        
        num_nodes = case.num_nodes
        bjobs = batch.get_batch_jobs()
        for job, jsect in bjobs:
            job_ = str.replace(job,'.','_')
            directives[job_] = []

            #task_count = jsect["task_count"]
            #task_count = env_batch.get_value("task_count", subgroup=job)

#            models = case.get_values("COMP_CLASSES")
#            env_mach_pes = case.get_env("mach_pes")
#            #task_count = env_mach_pes.get_total_tasks(models)
#            ptile = case.get_value("PES_PER_NODE")
#            self.num_nodes = case.num_nodes
#            self.thread_count = case.thread_count

            #task_count = jsect["task_count"] if "task_count" in jsect else env_mach_pes.get_total_tasks(models)     
#            task_count = case.get_value("TOTALPES")*int(case.thread_count)

#            if task_count == "default":
#                models = case.get_values("COMP_CLASSES")
#                env_mach_pes = case.get_env("mach_pes") 
#                task_count = env_mach_pes.get_total_tasks(models)
#                ptile = case.get_value("PES_PER_NODE")
#                self.num_nodes = case.num_nodes
#                self.thread_count = case.thread_count
#            else:
#                ptile = 4                
#                self.num_nodes = 1
#                self.thread_count = 1

#            self.ptile = ptile
#            self.total_tasks = task_count
#            self.tasks_per_node = ptile

#            queue = env_batch.select_best_queue(int(task_count),job=job)
#            if queue is None:
#                queue = env_batch.select_best_queue(task_count,job)
#            all_queue = []
#            all_queue.append(env_batch.get_default_queue())
#            all_queue = all_queue + env_batch.get_all_queues()               
#            queue = None
# Add back in when cime is frozen
#            for q in all_queue:
#                if q is not None:
#                    if queue is None:
#                        queue = q.xml_element.text

#            wall_time=None
            #wall_time = env_batch.get_max_walltime(queue) if wall_time is None else wall_time
#            wall_time = env_batch.get_queue_specs(queue)[3] if wall_time is None else wall_time
#            env_batch.set_value("JOB_WALLCLOCK_TIME", wall_time, subgroup=job)
#            env_batch.set_value("JOB_QUEUE", queue, subgroup=job)

            #direct = ''
            #ds = env_batch.get_batch_directives(case, job, raw=True)
#            overrides = {"total_tasks": int(task_count),"num_nodes":int(math.ceil(float(task_count)/float(case.tasks_per_node)))}
#            overrides["job_id"] = case.get_value("CASE") + os.path.splitext(job)[1]
#            overrides["batchdirectives"] = env_batch.get_batch_directives(case, job, overrides=overrides)
                
#            ds = env_batch.get_batch_directives(case, job, overrides=overrides)
#            dss = ds.split('\n') 
#            for d in dss:
#                direct = direct + transform_vars(d, case=case, subgroup=job)   
                #direct = direct + transform_vars(d, case=case, subgroup=job, check_members=self)       

#            s = env_batch.get_submit_args(case, job)
#            bd = env_batch.get_batch_directives(case, job, overrides=overrides) 

# Add this back in when cime is more stable
#            if "run" not in job_:
#                direct = direct.replace(bd,'')
#                direct = direct + s 
#                direct = direct.replace('-', '\n-')
#                direct = direct.split('\n')
#                for d in direct:
#                    d.lstrip()
#                    d.strip()
#                    if '#PBS' in d:
#                        d=d.replace("#PBS",'')
#                    d = d.split(' ')
#                    d=' '.join(d).split()
#                    if len(d) == 2:
#                        if ' ' not in d[0] and ' ' not in d[1] and 'walltime' not in d[1]:
#                            directives[job_].append(d[0]+' = '+d[1])

#### Start temp code to get pbs directives from case.run
            if 'st_archive' in job_:
                directives[job_].append("-A = "+os.getenv('PROJECT'))
                directives[job_].append("-q = regular")
                with open(my_case+"/case.st_archive") as f:
                    for l in f:
                        if '#PBS' in l:
                            pbs_split = l.split()
                            if len(pbs_split) == 3:
                                directives[job_].append(pbs_split[1]+" = "+pbs_split[2])
            else:
                with open(my_case+"/.case.run") as f:
                    directives[job_].append("-A = "+os.getenv('PROJECT'))
                    directives[job_].append("-q = regular")
                    for l in f:
                        if '#PBS' in l:
                            pbs_split = l.split()
                            if len(pbs_split) == 3:
                                directives[job_].append(pbs_split[1]+" = "+pbs_split[2])


#### End temp code to get pbs directives from case.run

        self.env['machine_name'] = machine_name
        self.env['batch_type'] = env_batch.get_batch_system_type()
        self.env['directives'] = directives
        self.env['STOP_N'] = case.get_value("STOP_N")
        self.env['RESUBMIT'] = case.get_value("RESUBMIT")
        self.env['STOP_OPTION'] = case.get_value('STOP_OPTION')
        self.env['DOUT_S'] = case.get_value('DOUT_S')
        self.env['DOUT_L_MS'] = case.get_value('DOUT_L_MS')
        self.env['CASEROOT'] = case.get_value('CASEROOT')
        self.env['CASE'] = case.get_value('CASE')
        self.env['RUNDIR'] = case.get_value('RUNDIR')        
        self.env['CESMSCRATCHROOT'] = case.get_value('CIME_OUTPUT_ROOT')
        self.env['USER'] = case.get_value('USER')
        cont_run = case.get_value('CONTINUE_RUN')
        if not cont_run:
            start = case.get_value('RUN_STARTDATE')
        else:
            start = self.get_date(self.env['RUNDIR'])
        if debug is True:
            valid = True
            self.env['RUN_STARTDATE'] = start
        else:
            valid = False
        while not valid:
            choice = str(raw_input("Use start date "+start+"? y/n \n"))
            if choice == 'Y' or choice == 'y':
                valid = True
                self.env['RUN_STARTDATE'] = start
            elif choice == 'N' or choice == 'n':
                valid = True
                user_date = str(raw_input("Enter new date (format yyyy-mm-dd):\n"))
        #case.set_value("RUN_WITH_SUBMIT", True)
       
        if os.path.isdir(my_case+'/postprocess/'):
            pp_dir = my_case+'/postprocess/'

            os.chdir(pp_dir)
  
            # get pp directives
            comps = ['atm', 'ocn', 'lnd', 'ice']
            diag_t = ['diagnostics', 'averages']
            for c in comps:
                for d in diag_t:
                    job = c+"_"+d
                    directives[job] = []
                    output = subprocess.check_output('./pp_config --getbatch '+d+' --machine '+machine_name+' -comp '+c, shell=True)
                    output_s = output.split('\n')
                    for o in output_s:
                        o_s = o.split()
                        if len(o_s) > 1:
                            if 'walltime' not in o_s[1]:
                                directives[job].append(o_s[0]+' = '+o_s[1])
            # get pp for timeseries and xconform
            tools = ['xconform', 'timeseries']
            for t in tools:
                directives[t]=[]
                output = subprocess.check_output('./pp_config --getbatch '+t+' --machine '+machine_name, shell=True)
                output_s = output.split('\n')
                for o in output_s:
                    o_s = o.split()
                    if len(o_s) > 1:
                        if 'walltime' not in o_s[1]:
                            directives[t].append(o_s[0]+' = '+o_s[1])
 
            self.env['GENERATE_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get GENERATE_TIMESERIES', shell=True)
            self.env['TIMESERIES_TPER'],self.env['TIMESERIES_N'] = self.get_tseries_info(pp_dir,self.env['STOP_N'],self.env['STOP_OPTION'])
            self.env['TIMESERIES_RESUBMIT'] = self.get_tseries_resubmit(self.env['TIMESERIES_TPER'],self.env['TIMESERIES_N'],
                                                                    self.env['STOP_N'],self.env['STOP_OPTION'])

            self.env['STANDARDIZE_TIMESERIES'] = subprocess.check_output('./pp_config -value -caseroot '+pp_dir+' --get STANDARDIZE_TIMESERIES', shell=True)
   
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

        os.chdir(cwd)

        # Capitalize all true false values
        for k,v in self.env.iteritems():
            if 'directive' not in k:
                self.env[k] = str(self.env[k]).strip()
                v = str(v).strip()
                v = str(v).lstrip()
                if 'True' in v or 'False' in v or 'true' in v or 'false' in v or v == True or v == False:
                    self.env[k] = str(v).upper()

        return self.env        

def get_env(case_dir, debug=False):
    cylc = EnvCylc()
    env = cylc.get_env(case_dir, debug)
    return env
#for k in env.keys():
#    print k,': ',env[k]
