# coding=utf-8

from collections import deque
from SimPy.Simulation import Process, Monitor, hold, passivate
from simso.core.TimePartitionInstance import TimePartitionInstance
from simso.core.Task import TaskInfo
from simso.core.Task import Task
from simso.core.Timer import Timer
from simso.core.Scheduler import SchedulerInfo
from simso.core import Scheduler

from .CSDP import CSDP

import os
import os.path

class TimePartitionInfo(object):

    """
    TimePartitionInfo is mainly a container class grouping the data
    that characterize a TimePartition. A list of TimePartitionInfo
    objects are passed to the Model so that :class:`TimePartition`
    instances can be created.

    """

    def __init__(self, name, identifier, part_type, abort_on_miss,
                 period, activation_date, length, overrun, deadline,
                 preemption_cost, data, task_scheduler, cpus):
        """
        :type name: str
        :type identifier: int
        :type task_type: str
        :type abort_on_miss: bool
        :type period: float
        :type activation_date: float
        :type length: float
        :type overrun: float
        :type deadline: float
        :type preemption_cost: int
        :type data: dict
        """
        self.name = name
        self.identifier = identifier
        self.part_type = part_type
        self.period = period
        self.activation_date = activation_date
        self.length = length
        self.overrun = overrun
        self.deadline = deadline
        self.abort_on_miss = abort_on_miss
        self.data = data
        self.preemption_cost = preemption_cost
        self.task_scheduler_info = SchedulerInfo()
        self.task_scheduler_info.clas = task_scheduler
        self.cpus = cpus
        self.task_infos = []

class GenericTimePartition(Process):
    """
    Abstract class for TimePartition. :class:`PTimePartition` inherits from
    this class.

    These classes simulate the behavior of the simulated time
    partition. It controls the release of the instances and is able to
    abort the instances that exceed their deadline.

    The majority of the part_info attributes are available through
    this class too. A set of metrics such as the number of preemptions
    are available for analysis.

    """
    fields = []

    def __init__(self, sim, part_info):
        """
        Args:

        - `sim`: :class:`Model <simso.core.Model>` instance.
        - `part_info`: A :class:`TimePartitionInfo` representing the TimePartition.

        :type sim: Model
        :type task_info: TimePartitionInfo
        """
        Process.__init__(self, name=part_info.name, sim=sim)
        self.name = part_info.name
        self._part_info = part_info
        self._monitor = Monitor(name="Monitor" + self.name + "_states",
                                sim=sim)
        self._activations_fifo = deque([])
        self._sim = sim
        self.cpus = []
        self._etm = sim.etm
        self._last_cpus = None
        self._inst_count = 0
        self._insts = []
        self.inst = None
        self._task_evts = {}
        self._sched_info = part_info.task_scheduler_info
                        
        """
        This structure keeps track of which job is running on each cpu of
        a partition

        """
        self.running_jobs = {}

        # Create tasks for this partition
        self._tasks = []
        for task_info in part_info.task_infos:
            task = Task(sim, task_info)
            task.set_time_partition(self)
            self._tasks.append(task)
            sim.task_list.append(task)

        
        # Is budget donation active for this partition?
        self.can_donate = False
        if 'can_donate' in part_info.data:
            self.can_donate = part_info.data['can_donate']


    def __lt__(self, other):
        return self.identifier < other.identifier

    def is_active(self):
        return self.inst is not None and self.inst.is_active()

    @property
    def data(self):
        """
        Extra data to characterize the task. Only used by the scheduler.
        """
        return self._part_info.data

    @property
    def deadline(self):
        """
        Deadline in milliseconds.
        """
        return self._part_info.deadline

    @property
    def preemption_cost(self):
        return self._part_info.preemption_cost

    @property
    def length(self):
        """Worst-Case Execution Time in milliseconds."""
        return self._part_info.length

    @property
    def overrun(self):
        return self._part_info.overrun

    @property
    def period(self):
        """
        Period of the task.
        """
        return self._part_info.period

    @property
    def identifier(self):
        """
        Identifier of the task.
        """
        return self._part_info.identifier

    @property
    def monitor(self):
        """
        The monitor for this Task. Similar to a log mechanism (see Monitor in
        SimPy doc).
        """
        return self._monitor

    @property
    def insts(self):
        """
        List of the partition instances.
        """
        return self._insts

    @property
    def tasks(self):
        """
        List of the partition instances.
        """
        return self._tasks
        
    @property
    def task_evts(self):
        """
        List of the partition instances.
        """
        return self._task_evts


    def add_task(self, task):
        """
        Add a task to this partition.
        """
        task.set_time_partition(self)
        self._tasks.append(task)


    def init_part(self):
        # Initialized local container for interrupted tasks
        for cpu in self.cpus:
            self.running_jobs[cpu] = None
            self._task_evts[cpu] = deque([])

        # Create partition scheduler
        """
        Each partition has a task scheduler. This is different from the
        partition scheduler defined and invoked inside each processor

        """

        # Instantiate scheduler for this partition
        self.scheduler = self._sched_info.instantiate(self._sim)
        self.scheduler.processors = self.cpus
        self.scheduler.task_list = self.tasks
        self.scheduler.init()
        self.scheduler.set_time_partition(self)


    def end_inst(self, inst):
        self._last_cpus = self.cpus

        if len(self._activations_fifo) > 0:
            self._activations_fifo.popleft()
        if len(self._activations_fifo) > 0:
            self.inst = self._activations_fifo[0]
            self.sim.activate(self.inst, self.inst.activate_inst())

    def _inst_killer(self, inst):
        if inst.end_date is None and inst.computation_time < inst.length:
            if self._part_info.abort_on_miss:
                self.cancel(inst)
                inst.abort()

    def create_inst(self, pred=None):
        """
        Create a new instance from this partition. This should probably
        not be used directly by a scheduler.

        """
        self._inst_count += 1

        inst = TimePartitionInstance(self, "{}_{}".format(self.name, self._inst_count), pred,
                                     monitor=self._monitor, etm=self._etm, sim=self.sim, 
                                     can_donate=self.can_donate)

        if len(self._activations_fifo) == 0:
            self.inst = inst
            self.sim.activate(inst, inst.activate_inst())
        self._activations_fifo.append(inst)
        self._insts.append(inst)

        timer_deadline = Timer(self.sim, GenericTimePartition._inst_killer,
                               (self, inst), self.deadline)
        timer_deadline.start()

    def _init(self):
        # by default run on all the CPUs
        if self.cpus is None:
            self.cpus = self._sim.processors

class PTimePartition(GenericTimePartition):
    """
    Periodic TimePartition process. Inherits from
    :class:`GenericTimePartition`. The instances are created
    periodically.

    """
    fields = ['activation_date', 'period', 'deadline', 'length']

    def execute(self):
        self._init()
        # wait the activation date.
        yield hold, self, int(self._part_info.activation_date *
                              self._sim.cycles_per_ms)

        while True:
            #print self.sim.now(), "activate", self.name
            self.create_inst()
            yield hold, self, int(self.period * self._sim.cycles_per_ms)

part_types = {
    "Periodic": PTimePartition,
}

part_types_names = ["Periodic"]


def TimePartition(sim, part_info):
    """
    Task factory. Return and instantiate the correct class according to the
    part_info.
    """

    return part_types[part_info.part_type](sim, part_info)
