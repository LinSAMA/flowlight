#-*- coding:utf-8 -*-
import asyncio
import os
import paramiko
import socket
import subprocess
import threading
from time import time
from io import BytesIO

__all__ = [
    'task',
    'Machine',
    'Group',
    'Cluster',
    'API'
]


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
        >>> m.name = 'local'
        >>> m.set_connection(password='root')
        >>> isinstance(m.run('echo 1;'), Response)
        True
    """
    def __init__(self, host, name=None, connect=False, **kwargs):
        Node.__init__(self, name)
        self.host = host
        self._connection = None
        if connect is True:
            self.set_connection(connect=connect, **kwargs)

    def _need_connection(func):
        def wrapper(self, *args, **kwargs):
            if not self._connection:
                raise Exception('Connection is not set')
            return func(self, *args, **kwargs)
        return wrapper

    @_need_connection
    def run(self, cmd, **kwargs):
        command = Command(cmd, **kwargs)
        response = self._connection.exec_command(command)
        return response

    @_need_connection
    def run_async(self, cmd, **kwargs):
        command = Command(cmd, **kwargs)
        response = yield from self._connection.exec_async_command(command)
        return response

    def run_task(self, task, *args, **kwargs):
        if not isinstance(task, _Task):
            raise Exception('Need a task')
        return task(self, *args, **kwargs)

    def set_connection(self, **kwargs):
        if self._connection:
            self._connection = Connection.__init__(machine=self, host=self.host, **kwargs)
        else:
            self._connection = Connection(machine=self, host=self.host, **kwargs)

    def get_connection(self):
        return self._connection

    __str__ = lambda self: self.host


class Group(Node):
    """ A group of multiple `Machine` and `Group`.
    """
    def __init__(self, nodes, name=None):
        Node.__init__(self, name)
        self._nodes = []
        self._nodes_map = {}
        for node in nodes:
            self.add(node)

    def set_connection(self, **kwargs):
        for node in self.nodes():
            node.set_connection(**kwargs)

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
    def __init__(self, cmd, bufsize=-1, timeout=5, env=None):
        self.cmd = cmd
        self.bufsize = bufsize
        self.timeout = timeout
        self.env = None


class Signal:
    def __init__(self, doc=None):
        self.doc = doc
        self._receivers = []

    def connect(self, func):
        self._receivers.append(func)

    def send(self, *args, **kwargs):
        for receiver in self._receivers:
            receiver(*args, **kwargs)

    def __call__(self, func):
        self.connect(func)

    @classmethod
    def func_signal(cls, func):
        signal = cls(None)
        signal.connect(func)
        return signal


class Trigger:
    def __init__(self):
        self.signals = []

    def add(self, signal):
        self.signals.append(signal)

    def __iter__(self):
        return iter(self.signals)


class _Task:
    """ A task wrapper on funciton.

    :param func: task function called by `Node` lately.
    :param run_after: run the task when `run_after` is finish stage.
    :param run_only: only run the task when `run_only` condition is True.
    """
    def __init__(self, func, run_after=None, run_only=None):
        self.func = func
        self.meta = _TaskMeta(self)
        self.trigger = Trigger()

        self.on_start = Signal.func_signal(self.start)
        self.on_complete = Signal.func_signal(self.complete)
        self.on_error = Signal.func_signal(self.error)

        self.event = threading.Event()
        self.meta.run_after = run_after
        self.run_only = run_only
        
        if run_after is not None and isinstance(run_after, _Task):
            run_after.trigger.add(Signal.func_signal(lambda: self.event.set()))

    def __call__(self, *args, **kwargs):
        if self.run_only is not None and self.run_only() is False:
            return (Exception('Run condition check is failed.'), False)
        if self.meta.run_after is not None and isinstance(self.meta.run_after, _Task):
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
    """Decorator function on task procedure which will be executed on machine cluster.

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
    cls = _Task
    if func is None:
        class _Deffered:
            def __new__(_cls, func):
                return cls(func, *args, **kwargs)
        return _Deffered
    return cls(func, *args, **kwargs)


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
        self.result = self.stdout.read()

    def __str__(self):
        return self.result.decode()


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
    def __init__(self, machine, host='127.0.0.1', port=22, username='root', 
            password=None, pkey='~/.ssh/id_rsa', timeout=5, auto_add_host_policy=True, connect=False, **kwargs):
        self._machine = machine
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.host = socket.gethostbyname(host)
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        pkey = os.path.abspath(os.path.expanduser(pkey))
        if os.path.exists(pkey):
            self.pkey = paramiko.RSAKey.from_private_key_file(pkey)
        self._connect_args = kwargs
        self.is_connected = False
        if auto_add_host_policy:
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if connect:
            self.build_connect()

    def build_connect(self):
        if socket.gethostbyname(self.host) == '127.0.0.1':
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
            bufsize=command.bufsize,
            env=command.env
        )
        response = Response(self._machine, p.stdin, p.stdout, p.stderr)
        return response

    def exec_remote_command(self, command):
        response = Response(self._machine, *self.client.exec_command(
            command.cmd,
            bufsize=command.bufsize,
            timeout=command.timeout,
            environment=command.env
        ))
        return response
    
    def ensure_connect(func):
        def wrapper(self, *args, **kwargs):
            if not self.is_connected:
                self.build_connect()
            return func(self, *args, **kwargs)
        return wrapper
    
    @ensure_connect
    def exec_command(self, command):
        return self._exec_command(command)

    @asyncio.coroutine
    @ensure_connect
    def exec_async_command(self, command):
        def _read(chan):
            CHUNK_SIZE = 1024
            future = asyncio.Future()
            loop = asyncio.get_event_loop()

            def on_read():
                chunk = chan.recv(CHUNK_SIZE)
                future.set_result(chunk)
            
            loop.add_reader(chan.fileno(), on_read)
            
            chunk = yield from future
            loop.remove_reader(chan.fileno())
            return chunk

        def _read_all(chan):
            chunks = []
            chunk = yield from _read(chan)
            while chunk:
                chunks.append(chunk)
                chunk = yield from _read(chan)
            return b''.join(chunks)
        
        chan = self.client.get_transport().open_session()
        chan.exec_command(command.cmd)
        
        data = yield from _read_all(chan)

        response = Response(self._machine, BytesIO(b''), BytesIO(data), BytesIO(b''))
        return response

    def sftp(self):# TODO
        pass

    @ensure_connect
    def __enter__(self):
        return self.client

    def __exit__(self, type, value, traceback):
        pass

    _exec_command = exec_remote_command

    __del__ = close


class API:
    """ Simple HTTP API Server to run command on machines by url.
    """
    PORT = 3600
    __doc__ = 'Usage:: http://127.0.0.1:3600/<machines>/<command>'

    @classmethod
    def serve(cls):
        from http.server import SimpleHTTPRequestHandler, HTTPServer
        from socketserver import ThreadingMixIn
        class Server(ThreadingMixIn, HTTPServer): pass
        class Handler(SimpleHTTPRequestHandler): pass
        def do_GET(self):
            try:
                paths = self.path.split('/')
                machines = paths[1]
                cmd = paths[2] if len(paths) == 3 else None
                if cmd is None:
                    raise Exception
                hosts = machines.split(',')
                cluster = Cluster(hosts)
                cluster.set_connection()
                from urllib.request import unquote
                response = cluster.run(unquote(cmd))
                html = '\n'.join(map(lambda r: '<h1>{host}</h1><pre>{result}</pre>'.format(
                    host=str(r.target), result=r.result.decode()), response))
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.copyfile(BytesIO(bytes(html, 'utf-8')), self.wfile)
            except:
                self.send_response(400)
                self.end_headers()
                self.copyfile(BytesIO(bytes(cls.__doc__, 'utf-8')), self.wfile)
        Handler.do_GET = do_GET
        server = Server(('0.0.0.0', cls.PORT), Handler)
        print('FlowLight API serve on {port}... \n {usage}'.format(port=cls.PORT, usage=cls.__doc__))
        server.serve_forever()

api_serve = API.serve

