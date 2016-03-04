import subprocess
import re
import os
import io
import glob
import time

class PBSQueue(object):

    def __init__(self, user, reservoir, num_jobs, job_regex=r'*.sh'):
        self.split_regex = re.compile(r'\s+')
        self.status = None
        self.num_jobs = num_jobs
        self.running_jobs = 0
        
        # Get the job file reservior
        job_files = glob.glob(os.path.join(reservoir, job_regex))
        self.queue = job_files

        self.last_difference = None

    def update_status(self):        
        # Request status through shell
        response = subprocess.check_output(['qstat', '-u', self.user])
        # Get the parsed job list
        jobs = _parse_output(response)
        self.running_jobs = sum(j['status'] in ['R', 'Q'] for j in jobs)

    def _submit_job(job_file): 
        subprocess.check_output(['qsub', job_file])

    def submit_jobs(self):
        self.last_difference = self.num_jobs - self.running_jobs
        c = 1
        while c <= self.last_difference:
            c += 1
            _submit_job(queue[0])
            queue.pop(0)

    def _parse_output(output):
        lines = response.split('\n')
        del lines[:5]
        jobs = [] 
        for line in lines:
            els = self.split_regex.split(line)
            j = {"id_": els[0], "user": els[1], "queue": els[2], "name": els[3],
                 "status": els[9], "elapsed_time": els[10]}
            jobs.append(j)

        return jobs



if __name__ == "__main__":
    
    # Initialize Queue monitor
    queue = PBSQueue('fjl128', 'pbs_scripts/', 20)

    while True:
        queue.update(status)
        if queue.running_jobs >= queue.numb_jobs:
            pass
        else:
            queue.submit_jobs()
        print "Submitted {} jobs".format(queue.last_difference)
        time.sleep(600)

