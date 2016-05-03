# coding=utf-8
from __future__ import print_function
from collections import deque
from SimPy.Simulation import Process, Monitor, hold, waituntil
from simso.core.ProcEvent import ProcRunEvent, ProcIdleEvent, \
    ProcOverheadEvent, ProcCxtSaveEvent, ProcCxtLoadEvent, ProcStartPart, ProcNoPart


RESCHED = 1
ACTIVATE = 2
TERMINATE = 3
TIMER = 4
PREEMPT = 5
SPEED = 6

# EDIT added events for time partitioning
ACTIVATE_TPART = 7
TERMINATE_TPART = 8
RESCHED_TPART = 9
PREEMPT_TPART = 10

def debug(*args, **kwargs):
    """Debug function to automatically suppress verbose prints."""
    # Adding new arguments to the print function signature 
    # is probably a bad idea.
    # Instead consider testing if custom argument keywords
    # are present in kwargs
    return
#    return __builtins__.print(*args, **kwargs)


class ProcInfo(object):
    def __init__(self, identifier, name, cs_overhead=0, cl_overhead=0,
                 migration_overhead=0, speed=1.0, data=None):
        self.identifier = identifier
        self.name = name
        self.penalty = 0
        self.caches = []
        self.cs_overhead = cs_overhead
        self.cl_overhead = cl_overhead
        self.migration_overhead = migration_overhead
        if data is None:
            data = {}
        self.data = data
        self.speed = speed

    def add_cache(self, cache):
        self.caches.append(cache)


class Processor(Process):
    """
    A processor is responsible of deciding whether the simulated processor
    should execute a job or execute the scheduler. There is one instance of
    Processor per simulated processor. Those are responsible to call the
    scheduler methods.

    When a scheduler needs to take a scheduling decision, it must invoke the
    :meth:`resched` method. This is typically done in the :meth:`on_activate
    <simso.core.Scheduler.Scheduler.on_activate>`, :meth:`on_terminated
    <simso.core.Scheduler.Scheduler.on_terminated>` or in a :class:`timer
    <simso.core.Timer.Timer>` handler.
    """
    _identifier = 0

    @classmethod
    def init(cls):
        cls._identifier = 0

    def __init__(self, model, proc_info):
        Process.__init__(self, name=proc_info.name, sim=model)
        self._model = model
        self._internal_id = Processor._identifier
        Processor._identifier += 1
        self.identifier = proc_info.identifier
        # EDIT currently running time partition
        self._time_part = False
        self._running_inst = None
        self._evts_inst = deque([])
        self._sched_part = None

        self._running = None
        self.was_running = None
        self._evts = deque([])
        self.sched = model.scheduler
        self.monitor = Monitor(name="Monitor" + proc_info.name, sim=model)
        self._caches = []
        self._penalty = proc_info.penalty
        self._cs_overhead = proc_info.cs_overhead
        self._cl_overhead = proc_info.cl_overhead
        self._migration_overhead = proc_info.migration_overhead
        self.set_caches(proc_info.caches)
        self.timer_monitor = Monitor(name="Monitor Timer" + proc_info.name,
                                     sim=model)
        self._speed = proc_info.speed

    def activate_time_partitioning(self, part_scheduler):
        # Enable time partitioning on this CPU and reset structures
        self._time_part = True
        self._running_inst = None
        self.sched = None
        self._running = None
        self._evts = deque([])
        self._sched_part = part_scheduler

    def resched(self, sched=None):
        """
        Add a resched event to the list of events to handle.
        """
        if(self._time_part and sched != None):
            sched._part._task_evts[self].append((RESCHED,))
        else:
            self._evts.append((RESCHED,))


    def activate(self, job):
        debug("---> ACTIVATE " + job.name + " on " + self.name + " at " + str(self._model.now()))

        if(self._time_part):
            job._task._part._task_evts[self].append((ACTIVATE, job,))
            
        else:
            self._evts.append((ACTIVATE, job))

    def terminate(self, job):
        debug("---> TERMINATE " + job.name + " on " + self.name + " at " + str(self._model.now()))
        if(self._time_part):
            job._task._part._task_evts[self].append((TERMINATE, job,))

        else:
            self._evts.append((TERMINATE, job))

    def preempt(self, job=None):
        to_remove = []
        for e in self._evts:
            if e[0] != PREEMPT:
                to_remove.append(e)

        for e in to_remove:
            self._evts.remove(e)

        #self._evts = deque([e for e in self._evts if e[0] != PREEMPT])
        self._evts.append((PREEMPT,))
        self._running = job

    def timer(self, timer):
        # No easy way to derive current partition from a timer
        # instance. But args[1] in a timer should be a Task instance
        # or a scheduler
        if(self._time_part):
            timer.args[0]._part._task_evts[self].append((TIMER, timer))

        else:
            self._evts.append((TIMER, timer))

    def set_speed(self, speed):
        assert speed >= 0, "Speed must be positive."
        self._evts.append((SPEED, speed))

    # EDIT added actions on time partitions
    def activate_inst(self, inst):
        self._evts_inst.append((ACTIVATE_TPART, inst))

    def terminate_inst(self, inst):
        self._evts_inst.append((TERMINATE_TPART, inst))

    def preempt_inst(self, inst=None):
        self._evts_inst.append((PREEMPT_TPART,))

        # First stop the job in the old partition
        if(inst != self._running_inst and self._running):
            #self.interrupt(self._running)
            self._running_inst.part.running_jobs[self] = self._running

        self._running_inst = inst
        self._running = None
        self.sched = None

        # Resume suspended jobs
        if(inst):
            self._running = inst.part.running_jobs[self]
            inst.part.running_jobs[self] = None
            self._evts = inst.part.task_evts[self]
            self.sched = inst.part.scheduler
            self._evts.append((RESCHED,))

            # if(self._running):
            #     self._running.interruptReset()
            #     self.sim.reactivate(self._running)

        else:
            self._evts = deque([])

        if(inst):
            debug("Switched to part " + inst.name)
        else:
            debug("Switched to NONE")

    def resched_inst(self, sched=None):
        """
        Add a resched event to the list of events to handle for time
        partitions.

        """
        self._evts_inst.append((RESCHED_TPART,))

            
    @property
    def speed(self):
        return self._speed

    def is_running(self):
        """
        Return True if a job is currently running on that processor.
        """
        return self._running is not None

    def is_time_partitioned(self):
        return self._time_part

    def set_caches(self, caches):
        self._caches = caches
        for cache in caches:
            cache.shared_with.append(self)

    def get_caches(self):
        return self._caches

    caches = property(get_caches, set_caches)

    @property
    def penalty_memaccess(self):
        return self._penalty

    @property
    def cs_overhead(self):
        return self._cs_overhead

    @property
    def cl_overhead(self):
        return self._cl_overhead

    @property
    def internal_id(self):
        """A unique, internal, id."""
        return self._internal_id

    @property
    def running(self):
        """
        The job currently running on that processor. None if no job is
        currently running on the processor.
        """
        return self._running

    @property
    def running_inst(self):
        """
        The time partition currently running on that processor. None if no
        time partition is currently running on the processor.

        """
        return self._running_inst

    @property
    def sched_part(self):
        """A unique, internal, id."""
        return self._sched_part

    def act_to_name(self, act):
        if(act == 1):
            return "RESCHED"
        elif(act == 2):
            return "ACTIVATE"
        elif(act == 3):
            return "TERMINATE"
        elif(act == 4):
            return "TIMER"
        elif(act == 5):
            return "PREEMPT"
        elif(act == 6):
            return "SPEED"
        elif(act == 7):
            return "ACTIVATE_TPART"
        elif(act == 8):
            return "TERMINATE_TPART"
        elif(act == 9):
            return "RESCHED_TPART"
        elif(act == 10):
            return "PREEMPT_TPART"
        
    # EDIT added procedures to run function to handle time partitioned
    # case
    def run(self):
        while True:
            debug("--- LOOP ---")

            if not self._evts and not self._evts_inst:
                job = self._running
                if job:
                    yield waituntil, self, lambda: job.context_ok
                    self.monitor.observe(ProcCxtLoadEvent())
                    yield hold, self, self.cl_overhead  # overhead load context
                    self.monitor.observe(ProcCxtLoadEvent(terminated=True))
                    job.interruptReset()
                    self.sim.reactivate(job)
                    self.monitor.observe(ProcRunEvent(job))
                    job.context_ok = False
                    if (self._time_part):
                        self._running_inst.notify_job_execution(self, job)
                    debug(self.name + " RUNNING " + job.name + " at " + str(self._model.now()))

                else:
                    debug(self.name + " IDLE AT " + str(self._model.now()))
                    self.monitor.observe(ProcIdleEvent())

                inst = self._running_inst
                if inst:
                    inst.interruptReset()
                    self.sim.reactivate(inst)
                    self.monitor.observe(ProcStartPart(inst))
                else:
                    self.monitor.observe(ProcNoPart(inst))

                # Wait for a job or a time partition event.
                yield waituntil, self, lambda: self._evts or self._evts_inst
                if job:
                    self.interrupt(job)
                    self.monitor.observe(ProcCxtSaveEvent())
                    yield hold, self, self.cs_overhead  # overhead save context
                    self.monitor.observe(ProcCxtSaveEvent(terminated=True))
                    job.context_ok = True

                if inst and self._evts_inst:
                    self.interrupt(inst)

            if self._evts:
                evt = self._evts.popleft()
            
                # If there is a job event being processed, we must be
                # executing one of the partitions (or no time partition
                # system is active on this CPU)
                #assert self.sched != None or evt[0]==ACTIVATE, "Job event processed outside time partition."
                debug("TASK: " + self.act_to_name(evt[0]) + " at " + str(self._model.now()) + " on " + self.name)

                if evt[0] == RESCHED:
                    if any(x[0] != RESCHED for x in self._evts):
                        self._evts.append(evt)
                        continue

                if evt[0] == ACTIVATE:
                    if self._time_part:
                        sched = evt[1].task.part.scheduler
                        sched.on_activate(evt[1])
                    else:
                        sched = self.sched
                        sched.on_activate(evt[1])
                        self.monitor.observe(ProcOverheadEvent("JobActivation"))
                        sched.monitor_begin_activate(self)
                        yield hold, self, sched.overhead_activate
                        sched.monitor_end_activate(self)
                elif evt[0] == TERMINATE:
                    if self._time_part:
                        sched = evt[1].task.part.scheduler
                        sched.on_terminated(evt[1])
                        debug("NOTIF...")
                        self._running = None
                        self._running_inst.notify_job_termination(self, evt[1])
                    else:
                        self.sched.on_terminated(evt[1])
                        self._running = None
                        self.monitor.observe(ProcOverheadEvent("JobTermination"))
                        self.sched.monitor_begin_terminate(self)
                        yield hold, self, self.sched.overhead_terminate
                        self.sched.monitor_end_terminate(self)
                        
                elif evt[0] == TIMER:
                    self.timer_monitor.observe(None)
                    if evt[1].overhead > 0:
                        debug(self.sim.now(), "hold", evt[1].overhead)
                        yield hold, self, evt[1].overhead
                    evt[1].call_handler()
                elif evt[0] == SPEED:
                    self._speed = evt[1]
                elif evt[0] == RESCHED:
                    self.monitor.observe(ProcOverheadEvent("Scheduling"))
                    self.sched.monitor_begin_schedule(self)
                    yield waituntil, self, self.sched.get_lock
                    decisions = self.sched.schedule(self)
                    yield hold, self, self.sched.overhead  # overhead scheduling
                    if type(decisions) is not list:
                        decisions = [decisions]
                    decisions = [d for d in decisions if d is not None]

                    # if(decisions):
                    #      debug "TASK RESCED at " + str(self._model.now()) + " - " + str(decisions) + " on " + self.name

                    for job, cpu in decisions:
                        # If there is nothing to change, simply ignore:
                        if cpu.running == job:
                            continue

                        # If trying to execute a terminated job, warn and ignore:
                        if job is not None and not job.is_active():
                            debug("Can't schedule a terminated job! ({})"
                                  .format(job.name))
                            continue

                        # if the job was running somewhere else, stop it.
                        if job and job.cpu.running == job:
                            job.cpu.preempt()

                        # Send that job to processor cpu.
                        cpu.preempt(job)

                        if job:
                            job.task.cpu = cpu

                    # Forbid to run a job simultaneously on 2 or more processors.
                    running_tasks = [
                        cpu.running.name
                        for cpu in self._model.processors if cpu.running]
                    assert len(set(running_tasks)) == len(running_tasks), \
                        "Try to run a job on 2 processors simultaneously!"

                    self.sched.release_lock()
                    self.sched.monitor_end_schedule(self)

            # If there are more task-related events, skip to next loop
            # to process all task events before partition events
            if (self._time_part and self._evts):
                continue
                
            # LOGIC TO RUN PARTITIONS JUST LIKE JOBS
            # In case time-partitioning is used, process also partition
            # events
            if (self._time_part and self._evts_inst):
                evt = self._evts_inst.popleft()
                debug("PART: " + self.act_to_name(evt[0]) + " at " + str(self._model.now()) + " on " + self.name)

                # TODO EDIT HERE
                if evt[0] == ACTIVATE_TPART:
                    self.sched_part.on_activate_inst(evt[1])
                    self.monitor.observe(ProcOverheadEvent("PartitionActivation"))
                    self.sched_part.monitor_begin_activate(self)
                    yield hold, self, 0 # TODO add overhead for partition activation
                    self.sched_part.monitor_end_activate(self)
                elif evt[0] == TERMINATE_TPART:
                    self.sched_part.on_terminated_inst(evt[1])
                    self.preempt_inst(None)
                    self.monitor.observe(ProcOverheadEvent("PartitionTermination"))
                    self.sched_part.monitor_begin_terminate(self)
                    yield hold, self, 0 # TODO add overhead for partition termination
                    self.sched_part.monitor_end_terminate(self)
                elif evt[0] == RESCHED_TPART:
                    self.monitor.observe(ProcOverheadEvent("PartitionScheduling"))
                    self.sched_part.monitor_begin_schedule(self)
                    decisions = self.sched_part.schedule(self)
                    yield hold, self, 0  # TODO add overhead for partition scheduling
                    if type(decisions) is not list:
                        decisions = [decisions]
                    decisions = [d for d in decisions if d is not None]

                    for inst, cpus in decisions:

                        for cpu in cpus:
                            
                            # If we are already running the same partition, do nothing
                            if cpu._running_inst == inst:
                                continue
                                
                            # If a partition was completed, raise a warning
                            if inst is not None and not inst.is_active():
                                debug("Can't schedule a terminated partition instance! ({}) at {} - {}"
                                      .format(inst.name, self._model.now(), self.name))
                                continue
                                
                            cpu.preempt_inst(inst)
                                
                    self.sched_part.monitor_end_schedule(self)
                    
