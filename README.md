# flowlight

a tool make remote operations easier

# Example

Run task via ssh on remote machines.

```python
from flowlight import Cluster, Group, task

cluster = Cluster(['host1', Group(['host2', 'host3'])])
cluster.set_connection(password='password')

@task
def create_file(meta, cluster):
    responses = cluster.run('''
    echo {value} > /tmp/test;
        '''.format(value=meta.value)
    )
    meta.value += 1

@create_file.on('start')
def before_create_file(meta):
    meta.value = 1

@create_file.on('complete')
def after_create_file(meta):
    print(meta.value)

@create_file.on('error')
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