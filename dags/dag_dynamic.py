#defining a dag dynamically

last_task = None
for i in range(1,3):
	task = BashOperator(
			task_id = 'task' + str(i),
			bash_command = "echo 'Task " + str(i) + "'",
			dag = dag)
	if last_task is None:
		last_task = task
	else:
		last_task.set_downstream(task)
		last_task = task