import collections

class Task:

    def __init__(self, node, label):

        self.id = node
        self.actual = label
        self.depends = []

    def get_id(self):

        return self.actual


    def add_depend(self, task):

        self.depends.append(task)


    def get_depends(self):

        return self.depends


class Graph:

    def __init__(self):

        self.tasks = collections.OrderedDict()
        self.task_count = 0        


    def __iter__(self):

        return iter(self.tasks.values())

    def add_task(self, task, label):

        new_task = Task(task, label)
        self.tasks[task] = Task(task, label)
        self.task_count = self.task_count + 1
        return self.tasks[task]


    def get_task(self, task_name):

        if task_name in self.tasks:
            return self.tasks[task_name]
        else:
            return None


    def add_depend(self, frm, depend, flabel, dlabel):

        #print frm,'->',flabel
        #print depend,'->',dlabel
        if frm not in self.tasks:
            self.add_task(frm, flabel)
        if depend not in self.tasks:
            self.add_task(depend, dlabel)
        #self.tasks[frm].add_depend(depend)
        self.tasks[frm].add_depend(dlabel)

    def get_depends():
        
        return self.tasks.keys()



def create_graph(keys, tasks):
    
    g = Graph()
    all_dates = tasks['cesm'].specs['date_queue']

    i = keys.index('cesm')
    keys.pop(i)
    first = True
    for i in range(0,len(all_dates)):
        date = all_dates[i]
        # Add cesm job
        if first:
            g.add_task('cesm_'+date, 'cesm_'+date)
            first = False
        else:
            g.add_depend(tasks['cesm'].specs['dependancy']+'_'+all_dates[i-1], 'cesm_'+date, 
                         tasks['cesm'].specs['dependancy']+'_'+all_dates[i-1], 'cesm_'+date)
        # Find out who has this date
        run_now = []
        for tool in keys:
            if date in tasks[tool].specs['date_queue']:
                # Add task to graph
                if date in tasks[tasks[tool].specs['dependancy']].specs['date_queue']:
                    g.add_depend(tasks[tool].specs['dependancy']+'_'+date, tool+'_'+date,
                                 tasks[tool].specs['dependancy']+'_'+tasks[tasks[tool].specs['dependancy']].specs['actual_dates'][date],
                                 tool+'_'+tasks[tool].specs['actual_dates'][date])
                else:
                    # There was not a dependancy date that matches for the current tool date
                    # If possible, match it up.
                    if len(tasks[tasks[tool].specs['dependancy']].specs['date_queue']) > 0:
                        date_found = False
                        tool_split = date.split('-')
                        t_year = tool_split[0]
                        if not date_found:
                            for d in tasks[tasks[tool].specs['dependancy']].specs['date_queue']:
                                # Parse date
                                date_split = d.split('-')
                                d_year = date_split[0]
                                if d_year > t_year:
                                    g.add_depend(tasks[tool].specs['dependancy']+'_'+d, tool+'_'+date,
                                                 tasks[tool].specs['dependancy']+'_'+tasks[tasks[tool].specs['dependancy']].specs['actual_dates'][d],
                                                 tool+'_'+tasks[tool].specs['actual_dates'][date])
                            
                    else:
                        print 'Broken dependancy for: ',tool+'_'+date

    for t in g:
            d = t.get_id()+' => '
            if len(t.depends) > 0:
                for i in range(0,len(t.depends)):
                    d = d + t.depends[i]
                    if i < len(t.depends)-1:
                        d = d + ' & '
                #print d     

    return g           
            #print t.get_id(),'->',t.depends
