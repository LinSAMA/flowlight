# flowlight

a tool make remote operations easier

# Example

```python
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

    @task(run_after=create_file)
    def show_file(meta, cluster):
        responses = cluster.run('''
            cat /tmp/test;            
        ''')
        for res in responses:
            print(res)

    cluster.run_task(show_file)
```