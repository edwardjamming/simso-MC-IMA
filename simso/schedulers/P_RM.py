"""
Partitionned EDF using PartitionedScheduler.
"""
from simso.core.Scheduler import SchedulerInfo
from simso.utils import PartitionedScheduler
from simso.schedulers import scheduler

@scheduler("simso.schedulers.P_RM")
class P_RM(PartitionedScheduler):
    def init(self):
        PartitionedScheduler.init(
            self, SchedulerInfo("simso.schedulers.RM_mono"))

    def set_time_partition(self, part):
        self._part = part
        for idc in self.map_cpu_sched:
            sched = self.map_cpu_sched[idc]
            sched.set_time_partition(part)

    def packer(self):
        # First Fit
        cpus = [[cpu, 0] for cpu in self.processors]
        # print "CPUs for this sched: " + str(len(cpus)) + " - TASKs: " + str(len(self.task_list))
        for task in self.task_list:
            m = cpus[0][1]
            j = 0
            
            # Is there a specific mapping given by the user?
            if 'CPU' in task.data:
                j = int(task.data['CPU'])
            else:
                # Find the processor with the lowest load.
                for i, c in enumerate(cpus):
                    if c[1] < m:
                        m = c[1]
                        j = i

            # Affect it to the task.
            self.affect_task_to_processor(task, cpus[j][0])

            # Update utilization.
            cpus[j][1] += float(task.wcet) / task.period
        return True
