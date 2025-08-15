from networkx import nodes


class JobsManager:
    def __init__(self):
        self.waiting_queue = []
        self.scheduled_queue = []
        self.terminated_jobs = []
        self.finished_jobs = []
        self.active_jobs = []
        self.num_terminated_jobs = 0
        self.num_finished_jobs = 0
        
    def add_job_to_waiting_queue(self, job):
        self.waiting_queue.append(job)
    
    def remove_job_from_waiting_queue(self, job_id, type):
        for i, job in enumerate(self.waiting_queue):
            if job['job_id'] == job_id:
                if type=='terminated':
                    self.terminated_jobs.append(job)
                    self.num_terminated_jobs += 1
                    print(f'Job {job_id} is terminated')
                elif type=='execution_start':
                    self.active_jobs.append(job)
                else:
                    raise ValueError(f"Invalid removal type: '{type}' (expected 'terminated' or 'execution_start')")
                del self.waiting_queue[i]
                break

    def add_job_to_scheduled_queue(self, job_id, nodes, predicted_time):
        for job in self.waiting_queue:
            if job['job_id'] == job_id:
                job['nodes'] = nodes
                job['predicted_time'] = predicted_time
                self.waiting_queue.remove(job)
                self.scheduled_queue.append(job)
                break
        