import subprocess
import re
import os
import io
import glob
import time
import argparse


class PBSQueue(object):

    def __init__(self, user_id, reservoir, num_jobs, job_regex=r'*.sh'):
        self.split_regex = re.compile(r'\s+')
        self.status = None
        self.num_jobs = num_jobs
        self.running_jobs = 0
	self.user_id = user_id
        
        # Get the job file reservior
        job_files = glob.glob(os.path.join(reservoir, job_regex))
        self.queue = job_files

        self.last_difference = None

    def update(self):        
        # Request status through shell
        response = subprocess.check_output(['qstat', '-u', self.user_id])
        # Get the parsed job list
        jobs = self._parse_output(response)
        self.running_jobs = sum(j['status'] in ['R', 'Q'] for j in jobs)

    def _submit_job(self, job_file): 
        subprocess.check_output(['qsub', job_file])

    def submit_jobs(self):
        self.last_difference = self.num_jobs - self.running_jobs
        c = 1
        while c <= self.last_difference:
            c += 1
            self._submit_job(self.queue[0])
            self.queue.pop(0)

    def _parse_output(self, output):
        lines = output.split('\n')
        del lines[:5]
        jobs = [] 
        for line in lines:
            els = self.split_regex.split(line)
            j = {"id_": els[0], "user": els[1], "queue": els[2], "name": els[3],
                 "status": els[9], "elapsed_time": els[10]}
            jobs.append(j)

        return jobs



if __name__ == "__main__":
	
    parser = argparse.ArgumentParser(description='Process some integers.')    
    parser.add_argument('--job_dir', dest='job_dir', action='store', type=str,
                        help='Directory containing the pbs job files. Format: [jobname].sh')
    parser.add_argument('--user_id', dest='user_id', action='store',
                        type=str,
                        help='User id')
    args = parser.parse_args()

    # Initialize Queue monitor
    queue = PBSQueue(args.user_id, args.job_dir, 5)

    while True:
        queue.update()
        if queue.running_jobs >= queue.num_jobs:
            pass
        else:
            queue.submit_jobs()
            print "Submitted {} jobs".format(queue.last_difference)
        time.sleep(600)

