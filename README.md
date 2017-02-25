# flowlight

a tool make remote operations easier

## Usage

Run task via ssh on remote machines.

```python
from flowlight import Cluster, Group, task

cluster = Cluster(['host1', Group(['host2', 'host3'])])
cluster.set_connection(password='password')

@task
def create_file(task, cluster):
    responses = cluster.run('''
    echo {value} > /tmp/test;
        '''.format(value=task.value)
    )
    task.value += 1

@create_file.on_start
def before_create_file(task):
    task.value = 1

@create_file.on_complete
def after_create_file(task):
    print(task.value)

@create_file.on_error
def error_when_create_file(exception):
    print(exception)
    import traceback
    traceback.print_exc()

cluster.run_task(create_file)
```

output:

```
2
```

Scheduling tasks with order.

```python
@task(run_after=create_file)
def show_file(meta, cluster):
    responses = cluster.run('''
        cat /tmp/test;            
    ''')
    for res in responses:
        print(res)

cluster.run_task(show_file)
```

Use `run_only` for task running pre-check.

```python
@task(run_only=lambda: 1 > 2)
def fail_task(self):
    print('condition is passed')

err, status = cluster.run_task(fail_task)
```

Use trigger in multi-threading.

```python
import threading
from time import sleep
after = threading.Thread(target=lambda: cluster.run_task(show_file))
before = threading.Thread(target=lambda: cluster.run_task(create_file))
after.start()
print('sleep a while...')
sleep(2)
before.start()
before.join()
after.join()
```

output:

```
sleep a while...
2
1

1

1
```

Async tasks supported.

```python
async def async_task(machine):
    await machine.run_async("ls")

m = Machine('host1')
m.set_connection()
m2 = Machine('host2')
m2.set_connection()

ev_loop = asyncio.get_event_loop()
ev_loop.run_until_complete(asyncio.gather(
    async_task(m), async_task(m2)
))
ev_loop.close()
```

Simple HTTP API command runner.

```python
API.serve()
```

```
http -f GET "http://127.0.0.1:3600/127.0.0.1/whoami"
```

output:

```html
<h1>127.0.0.1</h1><pre>tonnie
</pre>
```

```

```
http -f GET "http://127.0.0.1:3600/127.0.0.1/ps aux|wc -l"
```

output:

```html
<h1>127.0.0.1</h1><pre>     280
</pre>
```