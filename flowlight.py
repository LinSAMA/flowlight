#-*- coding:utf-8 -*-
import os
import paramiko
import subprocess
import threading
from contextlib import contextmanager
from time import time

__all__ = [
    'task',
    'Machine',
    'Group',
    'Cluster',
    'API'
]


class Util:
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
    """remote Machine.

    Usage::

        >>> m = Machine('127.0.0.1')
        >>> m.name = 'remote'
        >>> m.set_connection(password='root')
        >>> isinstance(m.run('echo 1;'), Response)
        True
    """
    def __init__(self, host, name=None):
        Node.__init__(self, name)
        self.host = host
        self._connection = None

    @Util._need_connection
    def run(self, cmd, **kwargs):
        command = Command(cmd)
        response = self._connection.exec_command(command, **kwargs)
        return response

    def run_task(self, task, *args, **kwargs):
        if not isinstance(task, _Task):
            raise Exception('Need a task')
        return task(self, *args, **kwargs)

    def set_connection(self, port=22, username='root', password=None, timeout=5, **kwargs):
        if self._connection:
            self._connection = Connection.__init__(
                machine=self._connection, 
                host=self.host,
                port=port,
                username=username,
                password=password,
                timeout=timeout,
                **kwargs
            )
        else:
            self._connection = Connection(
                machine=self,
                host=self.host, 
                port=port, 
                username=username,
                password=password, 
                timeout=timeout, 
                **kwargs
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

    def set_connection(self, port=22, username='root', password=None, timeout=5, **kwargs):
        for node in self.nodes():
            node.set_connection(port, username, password, timeout, **kwargs)

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
    def __init__(self, cmd, bufsize=-1, timeout=5):
        self.cmd = cmd
        self.bufsize = bufsize
        self.timeout = timeout


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
    def __init__(self, func, run_after=None, run_only=None):
        self.func = func
        self.meta = _TaskMeta(self)
        self.trigger = Trigger()

        self.on_start = Signal(self.start)
        self.on_complete = Signal(self.complete)
        self.on_error = Signal(self.error)

        self.event = threading.Event()
        self.run_after = run_after
        self.run_only = run_only
        
        if run_after is not None and isinstance(run_after, _Task):
            run_after.trigger.add(Signal(lambda: self.event.set()))


    def __call__(self, *args, **kwargs):
        if self.run_only is not None and self.run_only() is False:
            return (Exception('Run condition check is failed.'), False)
        if self.run_after is not None and isinstance(self.run_after, _Task):
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

    def start(self, *args):
        self.meta.started_at = time()

    def complete(self, *args):
        self.meta.finished_at = time()

    def error(self, exception):
        import traceback
        traceback.print_exc()


def task(func=None, *args, **kwargs):
    """Decorator function on task procedure which will be executed by machine cluster.

    :param func: the function to be decorated, act like a task.
            if no function specified, this will return a temporary class,
            which will instantiate a `_Task` object when it was called.
            otherwise, this will return a standard `_Task` object with
            parameters passed in.

    Usage::

        >>> deferred = task()
        >>> isinstance(deferred, _Task)
        False
        >>> t = deferred(lambda: None)
        >>> isinstance(t, _Task)
        True
        >>> t2 = task(lambda: None)
        >>> isinstance(t2, _Task)
        True

    """
    if func is None:
        class _Deffered:
            def __new__(cls, func):
                return _Task(func, *args, **kwargs)
        return _Deffered
    return _Task(func, *args, **kwargs)


class _TaskMeta(dict):
    """A dict-like storage data structure to collect task's information.

    :param task: the task that owns this meta object.

    Usage::

        >>> t = _Task(lambda: None)
        >>> isinstance(t.meta, _TaskMeta)
        True
        >>> isinstance(t.meta, dict)
        True
        >>> 'task' in t.meta
        True
        >>> t.meta['a'] = 1
        >>> print(t.meta['a'])
        1
        >>> t.meta.b = 2
        >>> print(t.meta['b'])
        2

    """
    def __init__(self, task, **kwargs):
        self.task = task

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value


class Response:
    """A wrapper of executed commands' result.

    :param target: the machine that runs the commands.
    :param stdin: standard input.
    :param stdout: standard output.
    :param stderr: standard error.

    Usage::

        >>> from io import BytesIO as bio
        >>> r = Response(Machine('127.0.0.1'), bio(b'stdin'), bio(b'stdout'), bio(b'stderr'))
        >>> print(r)
        stdout

    """
    def __init__(self, target, stdin, stdout, stderr):
        self.target = target
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.result = self.stdout.read().decode()

    def __str__(self):
        return self.result


class Connection:
    """A SSH Channel connection to remote machines.

    :param machine: the `Machine` owns this connection.
    :param host: the remote host string.
    :param port: the remote port.
    :param username: loggin user's name.
    :param password: loggin user's password.
    :param pkey: private key to use for authentication.
    :param timeout: timeout seconds for the connection.
    :param auto_add_host_policy: auto add host key when no keys were found.
    :param lazy: whether to build the connection when initializing.

    """
    def __init__(self, machine, host, port=22, username='root', 
            password=None, pkey='~/.ssh/id_rsa', timeout=5, auto_add_host_policy=True, lazy=True, **kwargs):
        self._machine = machine
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.pkey = paramiko.RSAKey.from_private_key_file(
            os.path.abspath(os.path.expanduser(pkey))
        )
        self._connect_args = kwargs
        self.is_connected = False
        if auto_add_host_policy:
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if not lazy:
            self.build_connect()

    def build_connect(self):
        if self.host == '127.0.0.1':
            setattr(self, '_exec_command', self.exec_local_command)
        else:
            self.client.connect(self.host, self.port, self.username, self.password, 
                pkey=self.pkey, timeout=self.timeout, **self._connect_args)
        self.is_connected = True

    def close(self):
        self.client.close()
        self.is_connected = False

    def exec_local_command(self, command):
        p = subprocess.Popen(command.cmd, shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            bufsize=command.bufsize
        )
        response = Response(self._machine, p.stdin, p.stdout, p.stderr)
        return response

    def exec_remote_command(self, command):
        response = Response(self._machine, *self.client.exec_command(
            command.cmd,
            bufsize=command.bufsize,
            timeout=command.timeout
        ))
        return response
    
    def exec_command(self, command):
        if not self.is_connected:
            self.build_connect()
        return self._exec_command(command)

    _exec_command = exec_remote_command

    __del__ = close


class API:
    def serve(self):
        pass


