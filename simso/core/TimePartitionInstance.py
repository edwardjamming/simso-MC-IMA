# coding=utf-8

from SimPy.Simulation import Process, hold, passivate
from simso.core.JobEvent import JobEvent
from simso.core.Timer import Timer
from math import ceil

def debug(*args, **kwargs):
    """Debug function to automatically suppress verbose prints."""
    # Adding new arguments to the print function signature 
    # is probably a bad idea.
    # Instead consider testing if custom argument keywords
    # are present in kwargs
    return
#    return __builtins__.print(*args, **kwargs)

class TimePartitionInstance(Process):
    """The Job class simulate the behavior of a time partition. This *should* only be
    instantiated by a TimePartition."""

    def __init__(self, part, name, pred, monitor, etm, sim, can_donate = False):
        """
        Args:
            - `part`: The parent :class:`TimePartition <simso.core.TimePartition.TimePartition>`.
            - `name`: The name for this instance.
            - `pred`: If the partition is not periodic, pred is the partition instance that \
            released this one.
            - `monitor`: A monitor is an object that log in time.
            - `etm`: The execution time model.
            - `sim`: :class:`Model <simso.core.Model>` instance.

        :type part: PartitionInstance
        :type name: str
        :type pred: bool
        :type monitor: Monitor
        :type etm: AbstractExecutionTimeModel
        :type sim: Model
        """
        Process.__init__(self, name=name, sim=sim)
        self._part = part
        self._pred = pred
        self.instr_count = 0  # Updated by the cache model.
        self._computation_time = 0
        self._last_exec = None
        self._start_date = None
        self._end_date = None
        self._is_preempted = False
        self._donated = False
        self._activation_date = self.sim.now_ms()
        self._absolute_deadline = self.sim.now_ms() + part.deadline
        self._aborted = False
        self._sim = sim
        self._monitor = monitor
        self._etm = etm
        self._was_running_on = part.cpus
        self.can_donate = can_donate
        self._extra_budget = 0
        self._avail_budget = {}

        # Initialize WCB  for this partition
        self._WCB = {}
        self._WCB_tmr = {}
        for cpu in part.cpus:
            self._WCB[cpu] = part.length
            self._WCB_tmr[cpu] = None
            self._avail_budget[cpu] = 0

        # This is only required to get the execution speed. So pick
        # the first CPU in the list
        self.cpu = part.cpus[0]

        self._on_activate()

        self.context_ok = True  # The context is ready to be loaded.

    def is_active(self):
        """
        Return True if the job is still active.
        """
        return self._end_date is None

    def _on_activate(self):
        self._monitor.observe(JobEvent(self, JobEvent.ACTIVATE))
        self._sim.logger.log("Partition " +self.name + " Activated.", kernel=True)
        self._etm.on_activate(self)

    def _on_execute(self):
        self._last_exec = self.sim.now()

        self._etm.on_execute(self)
        if self._is_preempted:
            self._is_preempted = False

        # TODO loops on cpus - possible bug? Shoud be: _part.cpu.was_running = self
        # self.cpu.was_running = self

        # TODO list all CPUs on log
        for cpu in self.part.cpus:
            self._monitor.observe(JobEvent(self, JobEvent.EXECUTE, cpu))
            self._sim.logger.log("Time partition {} Executing on {}".format(
                self.name, cpu.name), kernel=True)

    def _on_stop_exec(self):
        if self._last_exec is not None:
            self._computation_time += self.sim.now() - self._last_exec
        self._last_exec = None

    def _on_preempted(self):
        self._on_stop_exec()
        self._etm.on_preempted(self)
        self._is_preempted = True
        self._was_running_on = self._part.cpus

        self._monitor.observe(JobEvent(self, JobEvent.PREEMPTED))
        self._sim.logger.log("Time partition " + self.name + " Preempted! ret: " +
                             str(self.interruptLeft), kernel=True)

    def _on_donate_terminate(self):
        if self._donated:
            return
        self._donated = True
        amount = self._etm.get_ret(self)/self._sim.cycles_per_ms
        self._sim.logger.log("Time partition {} Donating {}".format(
            self.name, amount), kernel=True)
        self.sim.part_scheduler.on_donate(self, amount)
        self._on_terminated()

    def _on_terminated(self):
        self._on_stop_exec()
        self._etm.on_terminated(self)

        self._end_date = self.sim.now()
        self._monitor.observe(JobEvent(self, JobEvent.TERMINATED))
        self._part.end_inst(self)
        debug("ON_TERMINATE invoked for " + self.name)
        for cpu in self._part.cpus:
            cpu.terminate_inst(self)
        self._sim.logger.log("Time partition " + self.name + " Terminated.", kernel=True)
        for task in self.part.tasks:
            if task.opt_pending():
                self._sim.logger.log("Opt pending for " + self.name + " (" + task.name  + ")", kernel=True)
                
    def _on_abort(self):
        self._on_stop_exec()
        self._etm.on_abort(self)
        self._end_date = self.sim.now()
        self._aborted = True
        self._monitor.observe(JobEvent(self, JobEvent.ABORTED))
        self._part.end_inst(self)
        for cpu in self._part.cpus:
            cpu.terminate_inst(self)
        self._sim.logger.log("Time partition " + str(self.name) + " aborted! ret:" + str(self.ret))
        self._sim.report_inst_miss(self, self.ret)

    def receive_budget(self, amount):
        self._extra_budget += amount
        for cpu in self.part.cpus:
            self._avail_budget[cpu] += amount

        for task in self.part.tasks:
            task_amount = task.skipping_amount()
            give_amount = min(task_amount, self._avail_budget[task.cpu]) 
            if give_amount > 0:
                task.get_reclaimed_budget(give_amount)
                self._avail_budget[task.cpu] -= give_amount

        self._sim.logger.log("Time partition " + str(self.name) + " got budget: " + str(amount) + " len = " + str(self.wcet))

    def notify_job_termination(self, cpu, job):
        if self.can_donate == False:
            return

        saved = 0
        if job.overrun:
            saved = job.overrun - job.wcet

        WCB = self.WCB[cpu]
        if self.WCB_tmr[cpu]:
            self.WCB_tmr[cpu].stop()
            self.WCB_tmr[cpu] = None

        WCB = WCB - job.wcet - saved
        self.WCB[cpu] = min(self._etm.get_ret(self)/self._sim.cycles_per_ms, WCB)

        self.check_donate()

    def notify_job_execution(self, cpu, job):
        if self.can_donate == False:
            return

        # Start a timer that will fire at the expiration of the
        # current WCB
        if self.WCB_tmr[cpu]:
            self.WCB_tmr[cpu].stop()
            self.WCB_tmr[cpu] = None
            
        if self.WCB[cpu] > 0:
            self.WCB_tmr[cpu] = Timer(self.sim, TimePartitionInstance.WCB_expired, 
                                      (self, cpu), self.WCB[cpu])
            self.WCB_tmr[cpu].start()

    def WCB_expired(self, cpu):
        # The WCB is expired. Time to donate the budget.
        debug("WCB expired! on " + cpu.name)
        self.WCB[cpu] = 0
        self.check_donate()

    def check_donate(self):
        remaining = self._etm.get_ret(self)/self._sim.cycles_per_ms
        
        # is there something left to donate?
        if(remaining > 0):
            
            # Now check the WCB of all the CPUs
            for cpu in self.WCB:
                debug( "WCB on " + cpu.name + " = " + str(self.WCB[cpu]))
                debug( "More work on " + cpu.name + " : " + str(self.more_work(cpu)))
                if self.WCB[cpu] > 0 and self.more_work(cpu):
                    return

            self._on_donate_terminate()

    def more_work(self, cpu):
        end = self.absolute_deadline
        debug( "  end at " + str(end))
        
        for task in self.part.tasks:
            if task.cpu == cpu:
                debug( "  Task " + task.name + " - ended: " + str(task.job_ended()) + "; next: " + str(task.next_activation()))
                if not task.job_ended():
                    return True
                if task.next_activation() < end:
                    return True

        return False

    def is_running(self):
        """
        Return True if the job is currently running on a processor.
        Equivalent to ``self.cpu.running == self``.

        :rtype: bool
        """
        return self.cpu.running_inst == self #TODO add field to CPU

    def abort(self):
        """
        Abort this job. Warning, this is currently only used by the Part when
        the job exceeds its deadline. It has not be tested from outside, such
        as from the scheduler.
        """
        self._on_abort()

    @property
    def aborted(self):
        """
        True if the job has been aborted.

        :rtype: bool
        """
        return self._aborted

    @property
    def exceeded_deadline(self):
        """
        True if the end_date is greater than the deadline or if the job was
        aborted.
        """
        return (self._absolute_deadline * self._sim.cycles_per_ms <
                self._end_date or self._aborted)

    @property
    def start_date(self):
        """
        Date (in ms) when this job started executing
        (different than the activation).
        """
        return self._start_date

    @property
    def end_date(self):
        """
        Date (in ms) when this job finished its execution.
        """
        return self._end_date

    @property
    def response_time(self):
        if self._end_date:
            return (float(self._end_date) / self._sim.cycles_per_ms -
                    self._activation_date)
        else:
            return None

    @property
    def ret(self):
        """
        Remaining execution time in ms.
        """
        return self.length - self.actual_computation_time

    @property
    def computation_time(self):
        """
        Time spent executing the job in ms.
        """
        return float(self.computation_time_cycles) / self._sim.cycles_per_ms

    @property
    def computation_time_cycles(self):
        """
        Time spent executing the job.
        """
        if self._last_exec is None:
            return int(self._computation_time)
        else:
            return (int(self._computation_time) +
                    self.sim.now() - self._last_exec)

    @property
    def actual_computation_time(self):
        """
        Computation time in ms as if the processor speed was 1.0 during the
        whole execution.
        """
        return float(
            self.actual_computation_time_cycles) / self._sim.cycles_per_ms

    @property
    def actual_computation_time_cycles(self):
        """
        Computation time as if the processor speed was 1.0 during the whole
        execution.
        """
        return self._etm.get_executed(self)

    @property
    def cpus(self):
        """
        The :class:`processors <simso.core.Processor.Processor>` on which the
        time partition is attached. Equivalent to ``self._part.cpus``.
        """
        return self._part.cpu

    @property
    def part(self):
        """The :class:`TimePartition <simso.core.TimePartition>` for this instance."""
        return self._part

    @property
    def data(self):
        """
        The extra data specified for the part. Equivalent to
        ``self.part.data``.
        """
        return self._part.data

    @property
    def length(self):
        """
        Worst-Case Execution Time in milliseconds.
        Equivalent to ``self.part.wcet``.
        """
        return self._part.length + self._extra_budget

    @property
    def wcet(self):
        """
        Worst-Case Execution Time in milliseconds.
        Equivalent to ``self.part.wcet``.
        """
        return self._part.length + self._extra_budget

    @property
    def WCB(self):
        """
        Worst-Case Execution Budget in milliseconds.
        Equivalent to ``self._WCB``.
        """
        return self._WCB

    @property
    def WCB_tmr(self):
        """
        Worst-Case Execution Budget timer.
        Equivalent to ``self._WCB_tmr``.
        """
        return self._WCB_tmr

    @property
    def is_eligible(self):
        """
        Returns if this partition instance is currently eligible for
        bugdet donation

        """
        for task in self.part.tasks:
            if task.skipping_amount() > 0:
                return True

        return False

    def max_usable_budget(self):
        """
        Maximum budget that tasks in this partition can use on a single core
        """
        budget_all_cores = {}
        for cpu in self.part.cpus:
            budget_all_cores[cpu] = 0

        for task in self.part.tasks:
            budget_all_cores[task.cpu] += task.skipping_amount()

        return budget_all_cores[max(budget_all_cores, key=lambda i: budget_all_cores[i])]

    def usable_budget_all_cores(self, per_core_cap):
        """
        Maximum budget that tasks in this partition can use, regardless of
        which core

        """
        budget_all_cores = {}
        usable = 0
        for cpu in self.part.cpus:
            budget_all_cores[cpu] = 0

        for task in self.part.tasks:
            budget_all_cores[task.cpu] += task.skipping_amount()

        for cpu in self.part.cpus:
            budget_all_cores[cpu] = min(budget_all_cores[cpu], per_core_cap)
            usable += budget_all_cores[cpu]

        return usable

    @property
    def activation_date(self):
        """
        Activation date in milliseconds for this job.
        """
        return self._activation_date

    @property
    def absolute_deadline(self):
        """
        Absolute deadline in milliseconds for this job. This is the activation
        date + the relative deadline.
        """
        return self._absolute_deadline

    @property
    def absolute_deadline_cycles(self):
        return self._absolute_deadline * self._sim.cycles_per_ms

    @property
    def period(self):
        """Period in milliseconds. Equivalent to ``self.part.period``."""
        return self._part.period

    @property
    def deadline(self):
        """
        Relative deadline in milliseconds.
        Equivalent to ``self.part.deadline``.
        """
        return self._part.deadline

    @property
    def pred(self):
        return self._pred

    def activate_inst(self):
        self._start_date = self.sim.now()

        # Notify the OS - only sent to one CPU, the partition
        # scheduler will propagate the decision to the others
        #for cpu in self._part.cpus[0]:
        self._part.cpus[0].activate_inst(self)

        # While the instance's execution is not finished.
        while self._end_date is None:
            # Wait an execute order.
            yield passivate, self

            # Execute the instance.
            if not self.interrupted():
                self._on_execute()
                # ret is a duration lower than the remaining execution time.
                ret = self._etm.get_ret(self)

                while ret > 0:
                    yield hold, self, int(ceil(ret))

                    if not self.interrupted():
                        # If executed without interruption for ret cycles.
                        ret = self._etm.get_ret(self)
                    elif self._donated:
                        break
                    else:
                        self._on_preempted()
                        self.interruptReset()
                        break

                if ret <= 0:
                    # End of instance.
                    self._on_terminated()

            else:
                self.interruptReset()
