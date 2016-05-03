"""
Rate Monotic algorithm for uniprocessor architectures.
"""
from simso.core import Scheduler
from simso.schedulers import scheduler

@scheduler("simso.schedulers.RM_mono")
class RM_mono(Scheduler):
    def init(self):
        self.ready_list = []
        self._part = None
        
    def set_time_partition(self, part):
        self._part = part

    def on_activate(self, job):
        self.ready_list.append(job)
        job.cpu.resched(self)

    def on_terminated(self, job):
        if job in self.ready_list:
            self.ready_list.remove(job)
        job.cpu.resched(self)

    def schedule(self, cpu):
        if self.ready_list:
            # job with the highest priority
            job = min(self.ready_list, key=lambda x: x.period)
        else:
            job = None

        return (job, cpu)
