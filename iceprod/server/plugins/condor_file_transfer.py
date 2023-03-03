"""
Task submission to condor, using condor file transfer.
"""
import os
import logging

from iceprod.server.plugins.condor_direct import condor_direct

logger = logging.getLogger('plugin-condor_file_transfer')


class condor_file_transfer(condor_direct):
    """Plugin Overrides for HTCondor with file transfer for osdf://"""

    batch_site = 'CondorFileTransfer'
    batch_resources = {}

    async def customize_task_config(self, task_cfg, **kwargs):
        """Do OSDF file transfers via condor, so move files to batchsys"""
        await super().customize_task_config(task_cfg, **kwargs)

        in_files = []
        out_files = []

        def escape_remap(x):
            return x.replace('=','\\=').replace(';','\\;')

        def process_data(cfg):
            new_data = []
            for d in cfg.get('data', []):
                if d['remote'].startswith('osdf://'):
                    if d['movement'] in ('input', 'both'):
                        in_files.append(d['remote'])
                    if d['movement'] in ('output', 'both'):
                        local = d['local'] if d['local'] else os.path.basename(d['remote'])
                        remote = d['remote']
                        out_files.append(f'{escape_remap(local)} = {escape_remap(remote)}')
                else:
                    new_data.append(d)
            cfg['data'] = new_data

        process_data(task_cfg)
        for tray in task_cfg['trays']:
            process_data(tray)
            for module in tray['modules']:
                process_data(module)

        if in_files or out_files:
            # batchsys config can be None in config!
            batchsys = task_cfg.get('batchsys', None)
            if not batchsys:
                batchsys = {}
            batchsys_condor = batchsys.get('condor', {})
            reqs = batchsys_condor.get('requirements','')
            if reqs:
                reqs += ' && regexp("osdf",HasFileTransferPluginMethods)'
            else:
                reqs = 'regexp("osdf",HasFileTransferPluginMethods)'
            batchsys_condor['requirements'] = reqs
            batchsys_condor['transfer_input_files'] = ','.join(in_files)
            batchsys_condor['transfer_output_files'] = ','.join(v.split('=',1)[0].strip() for v in out_files)
            batchsys_condor['transfer_output_remaps'] = ','.join(out_files)
            batchsys['condor'] = batchsys_condor
            task_cfg['batchsys'] = batchsys
