import subprocess
import re
import os
import io
import glob
import time
import argparse
import datetime


class PBSQueue(object):
    '''
    PBS personal queue class

    Monitors a users pbs job submissions. Given a reservoir of jobs and the 
    desired number of parallel jobs, the queue submits new jobs as soon as the
    number of jobs drops below the desired number.

    Arguments:
    ----------
    user_id: str, pbs user id.
    reservior: str, path to folder containing the pbs job files.
    num_jobs: int, how many jobs sould be running in parallel.
    job_regex: str, which files in reservior should be used (can use glob
        compatible wildcards.
    remove_submitted: bool, should the job files be deleted after submission.
    '''

    def __init__(self, user_id, reservoir, num_jobs, job_regex=r'*', remove_submitted=True):
        self.split_regex = re.compile(r'\s+')
        self.status = None
        self.num_jobs = num_jobs
        self.running_jobs = 0
	self.user_id = user_id
        
        # TODO get this runnign with variable input path
        job_files = glob.glob('../bruce_shared/text_reuse/generate_alignments/pbs_scripts/*')
        self.queue = sorted(job_files)
	print len(self.queue)
        self.last_difference = 0
	self.submitted_jobs = []
	self.remove_submitted=remove_submitted

    def update(self):        
        '''
        Update status of the queue.

        Checks the status of the users jobs in the pbs system.

        Arguments:
        ----------
        
        Returns:
        ---------
        Updates the running_jobs attribute of the queue
        '''
        # Request status through shell
        response = subprocess.check_output(['qstat', '-u', self.user_id])
        # Get the parsed job list
        jobs = self._parse_output(response)
        self.running_jobs = sum(j['status'] in ['R', 'Q'] for j in jobs)

    def _submit_job(self, job_file): 
        '''
        Submit a job to the pbs queue.

        Arguments:
        ---------
        job_file: str, path to the file to be submitted

        Returns:
        ---------
        None
        '''
        subprocess.check_output(['qsub', job_file])
	print "submitting {}".format(job_file)
	self.submitted_jobs.append(job_file)
	if self.remove_submitted:
	    os.remove(job_file)

    def submit_jobs(self):
        '''
        Submit jobs until the desired amount of jobs is runnig

        Arguments:
        ---------
        None

        Returns:
        ----------
        None
        '''
        self.last_difference = self.num_jobs - self.running_jobs
        c = 1
        while c <= self.last_difference:
	    time.sleep(2)
            c += 1
	    len(self.queue)
            self._submit_job(self.queue[0])
            self.queue.pop(0)

    def _parse_output(self, output):
        '''
        Parse output from qstat pbs commandline program
        
        Arguments:
        ----------
        output: str, output obtained from qstat command

        Returns:
        ---------
        list, of all jobs. Each job is represented as a dictionary conainting
            all relevant information.
        '''
        lines = output.split('\n')
        del lines[:5]
        jobs = [] 
        for line in lines:
            els = self.split_regex.split(line)
            try:	
            	j = {"id_": els[0], "user": els[1], "queue": els[2], "name": els[3],
                 	"status": els[9], "elapsed_time": els[10]}    
            	jobs.append(j)

	    except IndexError:
 		pass

        return jobs



if __name__ == "__main__":
	
    parser = argparse.ArgumentParser(description='Process some integers.')    
    parser.add_argument('--job_dir', dest='job_dir', action='store', type=str,
                        help='Directory containing the pbs job files. Format: [jobname].sh')
    parser.add_argument('--user_id', dest='user_id', action='store',
                        type=str,
                        help='User id')
    parser.add_argument('--num_jobs', dest='num_jobs', action='store', type=int,
                        help='How many jobs should be run in parallel')
    args = parser.parse_args()

    # Initialize Queue monitor
    queue = PBSQueue(args.user_id, args.job_dir, args.num_jobs)

    while True:
        queue.update()
	ts = time.time()
	st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        if queue.running_jobs >= queue.num_jobs:
            print "[{}]: {} jobs running. No new jobs.".format(st, queue.running_jobs)
        else:
            queue.submit_jobs()
	    print "[{}]: {} jobs running. Submitted {} jobs".format(st, queue.running_jobs,
                                                                queue.last_difference)
        time.sleep(600)
