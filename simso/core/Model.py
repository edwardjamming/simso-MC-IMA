# coding=utf-8

from SimPy.Simulation import Simulation
from simso.core.Processor import Processor
from simso.core.Task import Task
from simso.core.Timer import Timer
from simso.core.etm import execution_time_models
from simso.core.Logger import Logger
from simso.core.results import Results
from simso.core.TimePartition import TimePartitionInfo
from simso.core.TimePartition import TimePartition


class Model(Simulation):
    """
    Main class for the simulation. It instantiate the various components
    required by the simulation and run it.
    """

    def __init__(self, configuration, callback=None):
        """
        Args:
            - `callback`: A callback can be specified. This function will be \
                called to report the advance of the simulation (useful for a \
                progression bar).
            - `configuration`: The :class:`configuration \
                <simso.configuration.Configuration>` of the simulation.

        Methods:
        """
        Simulation.__init__(self)
        self._logger = Logger(self)
        task_info_list = configuration.task_info_list
        part_info_list = configuration.part_info_list
        proc_info_list = configuration.proc_info_list
        self._cycles_per_ms = configuration.cycles_per_ms
        self._time_partitioning = False
        self._donation_policy = configuration.donation_policy
        self._task_list = []

        self._aborted_jobs = []
        self._aborted_insts = []
        self._aborted_jobs_times = {}
        self._aborted_insts_times = {}
        self._released_opt_jobs = 0
        self._completed_opt_jobs = 0
        self._released_opt_by_part = {}
        self._completed_opt_by_part = {}

        if(configuration.part_scheduler_info):
            self._time_partitioning = True
            self.part_scheduler = configuration.part_scheduler_info.instantiate(self)
            self.scheduler = None
        else:
            self.scheduler = configuration.scheduler_info.instantiate(self)
            self.scheduler.task_list = self._task_list

        try:
            self._etm = execution_time_models[configuration.etm](
                self, len(proc_info_list)
            )
        except KeyError:
            print("Unknowned Execution Time Model.", configuration.etm)

        for task_info in task_info_list:
            self._task_list.append(Task(self, task_info))

        self._part_list = []
        for part_info in part_info_list:
            self._part_list.append(TimePartition(self, part_info))
            self._released_opt_by_part[part_info] = 0
            self._completed_opt_by_part[part_info] = 0

        # Init the processor class. This will in particular reinit the
        # identifiers to 0.
        Processor.init()

        # Initialization of the caches
        for cache in configuration.caches_list:
            cache.init()

        self._processors = []
        for proc_info in proc_info_list:
            proc = Processor(self, proc_info)
            if(self._time_partitioning):
                proc.activate_time_partitioning(self.part_scheduler)
                for i in range(len(self._part_list)):
                    if proc_info in part_info_list[i].cpus:
                        self._part_list[i].cpus.append(proc)
                        
            proc.caches = proc_info.caches
            self._processors.append(proc)

        # Now that processors have been added, init list of
        # interrupted jobs per partition
        for part in self._part_list:
            part.init_part()

        # XXX: too specific.
        self.penalty_preemption = configuration.penalty_preemption
        self.penalty_migration = configuration.penalty_migration

        self._etm.init()

        self._duration = configuration.duration
        self.progress = Timer(self, Model._on_tick, (self,),
                              self.duration // 20 + 1, one_shot=False,
                              in_ms=False)
        self._callback = callback

        
        # Add processors to either task scheduler or partition
        # scheduler
        if(self._time_partitioning):
            self.part_scheduler.processors = self._processors
        else:
            self.scheduler.processors = self._processors

        self.results = None

    def now_ms(self):
        return float(self.now()) / self._cycles_per_ms

    @property
    def logs(self):
        """
        All the logs from the :class:`Logger <simso.core.Logger.Logger>`.
        """
        return self._logger.logs

    @property
    def logger(self):
        return self._logger

    @property
    def cycles_per_ms(self):
        """
        Number of cycles per milliseconds. A cycle is the internal unit used
        by SimSo. However, the tasks are defined using milliseconds.
        """
        return self._cycles_per_ms

    @property
    def etm(self):
        """
        Execution Time Model
        """
        return self._etm

    @property
    def processors(self):
        """
        List of all the processors.
        """
        return self._processors

    @property
    def task_list(self):
        """
        LisXbt of all the tasks.
        """
        return self._task_list

    @property
    def part_list(self):
        """
        List of all the tasks.
        """
        return self._part_list

    @property
    def duration(self):
        """
        Duration of the simulation.
        """
        return self._duration

    @property
    def n_crit_part(self):
        return len([p for p in self._part_list if p.can_donate])

    @property
    def n_noncrit_part(self):
        return len(self._part_list) - self.n_crit_part

    @property
    def time_partitioning(self):
        """
        Is time partitioning active?
        """
        return self._time_partitioning

    def has_misses(self):
        if self._aborted_jobs or self._aborted_insts:
            return True
        return False

    def report_job_miss(self, job, ret):
        if job.task not in self._aborted_jobs:
            self._aborted_jobs.append(job.task)
            self._aborted_jobs_times[job.task] = ret
        elif ret > self._aborted_jobs_times[job.task]:
            self._aborted_jobs_times[job.task] = ret

    def report_inst_miss(self, inst, ret):
        if inst.part not in self._aborted_insts:
            self._aborted_insts.append(inst.part)
            self._aborted_insts_times[inst.part] = ret
        elif ret > self._aborted_insts_times[inst.part]:
            self._aborted_insts_times[inst.part] = ret

    def record_opt_available(self, opt_wcet, task):
        self._released_opt_jobs += opt_wcet
        self._released_opt_by_part[task.part._part_info] += opt_wcet

    def record_opt_completed(self, opt_wcet, task):
        self._completed_opt_jobs += opt_wcet
        self._completed_opt_by_part[task.part._part_info] += opt_wcet

    def _on_tick(self):
        if self._callback:
            self._callback(self.now())

    def run_model(self):
        """ Execute the simulation."""
        self.initialize()
        
        if(self.scheduler):
            self.scheduler.init()

        if(self._time_partitioning and self.part_scheduler):
            self.part_scheduler.init()
            self.part_scheduler.select_donation_policy(self._donation_policy)

        self.progress.start()
        
        for cpu in self._processors:
            self.activate(cpu, cpu.run())

        for part in self._part_list:
            self.activate(part, part.execute())

        for task in self._task_list:
            self.activate(task, task.execute())
            
        try:
            self.simulate(until=self._duration)
        finally:
            self._etm.update()

            if self.now() > 0:
                self.results = Results(self)
                self.results.end()
