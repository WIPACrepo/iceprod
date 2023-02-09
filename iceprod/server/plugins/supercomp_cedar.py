"""
A supercomputer plugin, specificially for
`Cedar <https://docs.computecanada.ca/wiki/Cedar>`_.
"""
import asyncio
from datetime import datetime,timedelta
from functools import partial
import getpass
import logging
import os
import stat

from .condor_direct import check_call, check_output, condor_direct

logger = logging.getLogger('plugin-cedar')


async def subprocess_ssh(host, args):
    """A subprocess call over ssh"""
    cmd = ['ssh', '-i', '~/.ssh/iceprod', host]
    if 'ICEPRODBASE' in os.environ:
        cmd.append(f'{os.environ["ICEPRODBASE"]}/env-shell.sh')
    cmd += args
    logger.info('subprocess_ssh: %r', cmd)
    p = await asyncio.create_subprocess_exec(*cmd)
    await asyncio.wait_for(p.wait(), timeout=20*60)
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return p


class supercomp_cedar(condor_direct):
    """Plugin Overrides for Cedar"""

    batch_site = 'Cedar'
    batch_outfile = 'slurm.out'

    # Cedar queue requirements
    batch_resources = {
        'cpu': 1,
        'gpu': 0,
        'memory': 3.75,
        'disk': 48.,
        'time': 20.,
        'os': 'RHEL_7_x86_64',
    }

    async def get_hold_reason(self, submit_dir, resources=None):
        """Search for a hold reason in the slurm stderr"""
        reason = None
        filename = os.path.join(submit_dir, 'slurm.err')
        if os.path.exists(filename):
            with open(filename) as f:
                for line in f:
                    line = line.strip().lower()
                    if line.startswith('slurmstepd: error:'):
                        resource_type = None
                        val = 0
                        if 'oom-kill' in line:
                            resource_type = 'memory'
                            with open(os.path.join(submit_dir, 'submit.sh')) as f2:
                                for line2 in f2:
                                    if line2.startswith('export NUM_MEMORY='):
                                        try:
                                            val = float(line2.split('=')[-1].strip())
                                        except Exception:
                                            pass
                                        break
                        elif 'due to time limit' in line:
                            resource_type = 'time'
                            with open(os.path.join(submit_dir, 'submit.sh')) as f2:
                                for line2 in f2:
                                    if line2.startswith('export NUM_TIME='):
                                        try:
                                            val = float(line2.split('=')[-1].strip())
                                        except Exception:
                                            pass
                                        break
                        if resource_type:
                            reason = f'Resource overusage for {resource_type}: {val}'
                            if val:
                                resources[resource_type] = val
                            break
        return reason

    async def download_input(self, task):
        """
        Download input files for task.

        Args:
            task (dict): task info
        """
        self.x509proxy.get_proxy()
        try:
            await check_call(
                'python', '-m', 'iceprod.core.data_transfer', '-f',
                os.path.join(task['submit_dir'],'task.cfg'),
                '-d', task['submit_dir'],
                'input'
            )
        except Exception:
            logger.info('failed to download input', exc_info=True)
            raise Exception(f'download input failed for {task["dataset_id"]}.{task["task_id"]}')

    async def upload_output(self, task):
        """
        Upload output files for task.

        Args:
            task (dict): task info
        """
        self.x509proxy.get_proxy()
        try:
            await check_call(
                'python', '-m', 'iceprod.core.data_transfer', '-f',
                os.path.join(task['submit_dir'],'task.cfg'),
                '-d', task['submit_dir'],
                'output'
            )
        except Exception:
            logger.info('failed to upload output', exc_info=True)
            raise Exception(f'upload output failed for {task["dataset_id"]}.{task["task_id"]}')

    async def generate_submit_file(self, task, cfg=None, passkey=None, filelist=None):
        """Generate queueing system submit file for task in dir."""
        args = self.get_submit_args(task,cfg=cfg)
        args.extend(['--offline',])

        # write the submit file
        submit_file = os.path.join(task['submit_dir'],'submit.sh')
        with open(submit_file,'w') as f:
            p = partial(print,sep='',file=f)
            p('#!/bin/bash')
            p('#SBATCH --account=rpp-kenclark')
            p('#SBATCH --output={}'.format(os.path.join(task['submit_dir'],'slurm.out')))
            p('#SBATCH --error={}'.format(os.path.join(task['submit_dir'],'slurm.err')))
            p(f'#SBATCH --chdir={task["submit_dir"]}')
            p('#SBATCH --ntasks=1')
            p('#SBATCH --export=NONE')
            p('#SBATCH --mail-type=NONE')
            p('#SBATCH --job-name=iceprod_{}'.format(os.path.basename(task['submit_dir'])))

            # handle resource requests
            if 'requirements' in task:
                if 'cpu' in task['requirements'] and task['requirements']['cpu']:
                    p(f'#SBATCH --cpus-per-task={task["requirements"]["cpu"]}')
                if 'gpu' in task['requirements'] and task['requirements']['gpu']:
                    p(f'#SBATCH --gres=gpu:{task["requirements"]["gpu"]}')
                if 'memory' in task['requirements'] and task['requirements']['memory']:
                    p('#SBATCH --mem={}M'.format(int(task['requirements']['memory']*1000)))
                # we don't currently use the local disk, just the global scratch
                # if 'disk' in task['requirements'] and task['requirements']['disk']:
                #     p('#SBATCH --tmp={}M'.format(int(task['requirements']['disk']*1000)))
                if 'time' in task['requirements'] and task['requirements']['time']:
                    p('#SBATCH --time={}'.format(int(task['requirements']['time']*60)))

            # get batchopts
            for b in self.queue_cfg['batchopts']:
                p(b+'='+self.queue_cfg['batchopts'][b])

            # make resources explicit in env
            if 'requirements' in task:
                if 'cpu' in task['requirements'] and task['requirements']['cpu']:
                    p(f'export NUM_CPUS={task["requirements"]["cpu"]}')
                if 'gpu' in task['requirements'] and task['requirements']['gpu']:
                    p(f'export NUM_GPUS={task["requirements"]["gpu"]}')
                if 'memory' in task['requirements'] and task['requirements']['memory']:
                    p(f'export NUM_MEMORY={task["requirements"]["memory"]}')
                if 'disk' in task['requirements'] and task['requirements']['disk']:
                    p(f'export NUM_DISK={task["requirements"]["disk"]}')
                if 'time' in task['requirements'] and task['requirements']['time']:
                    p(f'export NUM_TIME={task["requirements"]["time"]}')

            p('module load singularity/3.2')
            p('/opt/software/singularity-3.2/bin/singularity exec --nv --cleanenv -C', end=' ')
            p(f'-B /tmp -B /cvmfs -B /scratch -B /home --pwd {task["submit_dir"]}', end=' ')
            p('/cvmfs/singularity.opensciencegrid.org/opensciencegrid/osgvo-el7-cuda10:latest', end=' ')
            p('{} {}'.format(os.path.join(task['submit_dir'],'loader.sh'), ' '.join(args)))

        # make it executable
        st = os.stat(submit_file)
        os.chmod(submit_file, st.st_mode | stat.S_IEXEC)

    async def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['sbatch','submit.sh']
        ret = await check_output(*cmd, cwd=task['submit_dir'])
        grid_queue_id = ''
        for line in ret.split('\n'):
            if 'Submitted batch job' in line:
                grid_queue_id = line.strip().rsplit(' ',1)[-1]
                break
        else:
            raise Exception('did not get a grid_queue_id')
        task['grid_queue_id'] = grid_queue_id

    async def get_grid_status(self):
        """
        Get all tasks running on the queue system.

        Returns:
            dict: {grid_queue_id: {status, submit_dir} }
        """
        cmd = ['squeue', '-u', getpass.getuser(), '-h', '-o', '%A %t %j %o']
        out = await check_output(*cmd)
        ret = {}
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,name,cmd = line.split()
            if not name.startswith('iceprod'):
                continue
            if status == 'PD':
                status = 'queued'
            elif status == 'R':
                status = 'processing'
            elif status == 'CD':
                status = 'completed'
            else:
                status = 'error'
            ret[gid] = {'status': status, 'submit_dir': os.path.dirname(cmd),
                        'site': self.site}
        return ret

    async def get_grid_completions(self):
        """
        Get completions in the last 4 days.

        Returns:
            dict: {grid_queue_id: {status, submit_dir} }
        """
        date = (datetime.now()-timedelta(days=4)).isoformat().split('.',1)[0]
        cmd = ['sacct', '-u', getpass.getuser(), '-n', '-P', '-S', date, '-o', 'JobIDRaw,State,JobName,ExitCode,Workdir']
        out = await check_output(*cmd)
        ret = {}
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,name,exit_code,workdir = line.strip().split('|')
            if status in ('PENDING', 'RESIZING', 'REQUEUED', 'RUNNING') or not name.startswith('iceprod'):
                continue
            if status == 'COMPLETED' and exit_code == '0:0':
                status = 'ok'
            else:
                status = 'error'
            ret[gid] = {'status': status, 'submit_dir': workdir, 'site': self.site}
        return ret

    async def remove(self,tasks):
        """Remove tasks from queueing system."""
        if tasks:
            cmd = ['scancel']+list(tasks)
            await check_call(*cmd)
