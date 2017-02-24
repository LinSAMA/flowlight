#-*- coding:utf-8 -*-
import paramiko
import threading
from contextlib import contextmanager
from time import time

def _need_connection(func):
    def wrapper(self, *args, **kwargs):
        if not self._connection:
            raise Exception('Connection is not set')
        return func(self, *args, **kwargs)
    return wrapper


class Node:
    """Abstract representation of `Machine` and `Group`.

    :param name: the unique name of a `Node`.

    Usage::

        >>> machine = Machine('host1')
        >>> isinstance(machine, Node)
        True
        >>> group = Group([Machine('host1'), Machine('host2')])
        >>> isinstance(group, Node)
        True
    """
    def __init__(self, name):
        self.name = name

    def run(self):
        raise NotImplemented

    def set_connection(self):
        raise NotImplemented


class Machine(Node):
    def __init__(self, host, name=None):
        Node.__init__(self, name)
        self.host = host
        self._connection = None

    @_need_connection
    def run(self, cmd, **kwargs):
        command = Command(cmd)
        response = self._connection.exec_command(command, **kwargs)
        return response

    def run_task(self, task, *args, **kwargs):
        if not isinstance(task, _Task):
            raise Exception('Need a task')
        return task(self, *args, **kwargs)

    def set_connection(self, port=22, username='root', password=None, timeout=5, auto_add_host_policy=True, lazy=True):
        if self._connection:
            self._connection = Connection.__init__(self._connection, self.host, port, username, password, timeout, auto_add_host_policy)
        else:
            self._connection = Connection(
                machine=self,
                host=self.host, 
                port=port, 
                username=username,
                password=password, 
                timeout=timeout, 
                auto_add_host_policy=auto_add_host_policy,
                lazy=lazy
            )

    def get_connection(self):
        return self._connection


class Group(Node):
    def __init__(self, nodes, name=None):
        Node.__init__(self, name)
        self._nodes = []
        self._nodes_map = {}
        for node in nodes:
            self.add(node)

    def set_connection(self, port=22, username='root', password=None, timeout=5, auto_add_host_policy=True, lazy=True):
        for node in self.nodes():
            node.set_connection(port, username, password, timeout, auto_add_host_policy, lazy)

    def add(self, node):
        if not isinstance(node, Node):
            host = name = node
            node = Machine(host, name)
            self._nodes_map[name] = node
        else:
            self._nodes_map[node.name] = node
        self._nodes.append(node)

    def nodes(self):
        return self._nodes

    def __iter__(self):
        return iter(self.nodes())

    def get(self, name):
        return self._nodes_map.get(name, None)

    def run(self, cmd):
        responses = []
        for node in self:
            res = node.run(cmd)
            res = res if isinstance(res, list) else [res]
            responses.extend(res)
        return responses


    def run_task(self, task, *args, **kwargs):
        if not isinstance(task, _Task):
            raise Exception('Need a task')
        return task(self, *args, **kwargs)


class Cluster(Group):
    def __init__(self, nodes, name=None):
        Group.__init__(self, nodes, name)


class Command:
    def __init__(self, cmd):
        self.cmd = cmd


class Signal:
    def __init__(self, func=None, name='DEFAULT'):
        self.name = name
        self._receivers = []
        if func is not None:
            self.connect(func)

    def connect(self, func):
        self._receivers.append(func)

    def send(self, *args, **kwargs):
        for receiver in self._receivers:
            receiver(*args, **kwargs)

    def __call__(self, func):
        self.connect(func)

class Trigger:
    def __init__(self):
        self.signals = []

    def add(self, signal):
        self.signals.append(signal)

    def __iter__(self):
        return iter(self.signals)

class _Task:
    def __init__(self, func, run_after=None):
        self.func = func
        self.meta = _TaskMeta(self)
        self.trigger = Trigger()

        self.on_start = Signal(self.start)
        self.on_complete = Signal(self.complete)
        self.on_error = Signal(self.error)

        self.event = threading.Event()
        self.run_after = run_after
        
        if run_after is not None and isinstance(run_after, _Task):
            run_after.trigger.add(Signal(lambda: self.event.set()))

    def __call__(self, *args, **kwargs):
        run_after = self.run_after
        if run_after is not None and isinstance(run_after, _Task):
            self.event.wait()
            self.event.clear()
        try:
            self.on_start.send(self.meta)
            result = self.func(self.meta, *args, **kwargs)
            self.on_complete.send(self.meta)
            return (result, True)
        except Exception as e:
            self.on_error.send(e)
            return (e, False)
        finally:
            for signal in self.trigger:
                signal.send()

    def on(self, signal, *args, **kwargs):
        assert signal in ('start', 'complete', 'error')
        def _wrapper(func):
            assert callable(func)
            self.trigger.add(Signal(func=func, name=signal))
        return _wrapper

    def start(self, *args):
        self.meta.started_at = time()

    def complete(self, *args):
        self.meta.finished_at = time()

    def error(self, exception):
        import traceback
        traceback.print_exc()


def task(func=None, *args, **kwargs):
    if func is None:
        class _:
            def __new__(cls, func):
                return _Task(func, *args, **kwargs)
        return _
    return _Task(func, *args, **kwargs)


class _TaskMeta(dict):
    def __init__(self, task, **kwargs):
        self.task = task

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value


class Stage:
    def __init__(self, name, tasks):
        self.name = name
        self.tasks = tasks

    def add_task(self, task):
        self.tasks.append(task)


class Response:
    def __init__(self, target, stdin, stdout, stderr):
        self.target = target
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.result = self.stdout.read().decode()

    def __str__(self):
        return self.result


class Connection:
    def __init__(self, machine, host, port=22, username='root', password=None, timeout=5, auto_add_host_policy=True, lazy=True):
        self._machine = machine
        self.client = paramiko.SSHClient()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.is_connected = False
        if auto_add_host_policy:
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if not lazy:
            self.build_connect(host, port, username, password, timeout)

    def build_connect(self, host, port, username, password, timeout):
        self.client.connect(host, port, username, password, timeout=timeout)
        self.is_connected = True
    
    def exec_command(self, command):
        if not self.is_connected:
            self.build_connect(self.host, self.port, self.username, self.password, self.timeout)
        response = Response(self._machine, *self.client.exec_command(command.cmd))
        return response


class Util:
    pass


class API:
    def serve(self):
        pass


