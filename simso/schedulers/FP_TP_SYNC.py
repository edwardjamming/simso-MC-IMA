#!/usr/bin/python
# coding=utf-8

from simso.core import Scheduler
from simso.schedulers import scheduler

@scheduler("simso.schedulers.FP_TP_SYNC", 
    required_task_fields = [
        {'name': 'priority', 'type' : 'int', 'default' : '0' }   
    ]
)
class FP_TP_SYNC(Scheduler):
    """ Time Partition Fixed Priority (use 'priority' field) """

    def init(self):
        self.ready_list = []
        self.donation_policy = None
        
    def on_activate_inst(self, inst):
        self.ready_list.append(inst)
        inst._part.cpus[0].resched_inst()

    def on_terminated_inst(self, inst):
        inst._part.cpus[0].resched_inst()

    def select_donation_policy(self, policy):
        self.donation_policy = policy

    def on_donate(self, inst, amount):
        # print "INSTANCE " + inst.name + " DONATED " + str(amount)
        done = False

        while amount > 0 and not done:
            res = None

            if self.donation_policy == 'priority':
                res = self.donate_by_prio(inst, amount)
            elif self.donation_policy == 'workload':
                res = self.donate_by_workload(inst, amount)
            elif self.donation_policy == 'fairness':
                res = self.donate_by_fairness(inst, amount)
            else:
                print "INVALID DONATION POLICY "
                exit()

            if res:
                inst = res[0]
                used_amount = res[1]
                # print "AMT: " + str(amount) + "; USED: " + str(used_amount)
                used_amount = min(used_amount, amount)
                amount -= used_amount
                inst.receive_budget(used_amount)
            else:
                done = True

    def donate_by_prio(self, donating, amount):
        # Order the list of ready instances by priority
        eligibles = [inst for inst in self.ready_list if (inst.is_eligible and 
                     (inst.data['priority'] < donating.data['priority'] or 
                      inst.data['priority'] == donating.data['priority']+1))]
        if eligibles:
            selected = max(eligibles, key=lambda x: x.data['priority'])
            return (selected, selected.max_usable_budget())
        else:
            return None

    def donate_by_workload(self, donating, amount):
        # Order the list of ready instances by priority
        eligibles = [inst for inst in self.ready_list if (inst.is_eligible and 
                     (inst.data['priority'] < donating.data['priority'] or 
                      inst.data['priority'] == donating.data['priority']+1))]
        if eligibles:
            selected = eligibles[0]
            usable = selected.usable_budget_all_cores(amount)
            for inst in eligibles:
                if inst.usable_budget_all_cores(amount) > usable:
                    selected = inst
                    usable = inst.usable_budget_all_cores(amount)
            return (selected, selected.max_usable_budget())
        else:
            return None

    def donate_by_fairness_old(self, donating, amount):
        # Order the list of ready instances by priority
        eligibles = [inst for inst in self.ready_list if (inst.is_eligible and 
                     (inst.data['priority'] < donating.data['priority'] or 
                      inst.data['priority'] == donating.data['priority']+1))]
        if eligibles:
            selected = None
            min_rate = 100
            for inst in eligibles:
                p_info = inst.part._part_info
                cur_rate = 0
                if self.sim._released_opt_by_part[p_info]:
                    cur_rate = float(self.sim._completed_opt_by_part[p_info])/self.sim._released_opt_by_part[p_info]
                if cur_rate < min_rate:
                    min_rate = cur_rate
                    selected = inst
            return (selected, selected.max_usable_budget())
        else:
            return None

    def donate_by_fairness(self, donating, amount):
        # Order the list of ready instances by priority
        eligibles = [inst for inst in self.ready_list if (inst.is_eligible and 
                     (inst.data['priority'] < donating.data['priority'] or 
                      inst.data['priority'] == donating.data['priority']+1))]
        if eligibles:
            total_ratio = float(self.sim._completed_opt_jobs)/self.sim.n_noncrit_part
            selected = eligibles[0]
            metric =  total_ratio - self.sim._completed_opt_by_part[selected.part._part_info]
            for inst in eligibles:
                p_info = inst.part._part_info
                cur_metric = total_ratio - self.sim._completed_opt_by_part[p_info]
                if cur_metric > metric:
                    metric = cur_metric
                    selected = inst
            return (selected, selected.max_usable_budget())
        else:
            return None

    def schedule(self, cpu):
        if self.ready_list:

            # Get the job with the highest priority.
            inst = max(self.ready_list, key=lambda x: x.data['priority'])

            if (cpu._running_inst is None or
                    cpu._running_inst.data['priority'] < inst.data['priority']):
                self.ready_list.remove(inst)
                if cpu._running_inst:
                    self.ready_list.append(cpu._running_inst)
                # print "===> DECISION TPART " + inst.name 
                return (inst, self.processors)
            
        return None
