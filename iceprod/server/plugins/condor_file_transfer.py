"""
Task submission to condor, using condor file transfer.
"""
import os
import logging

from iceprod.server.plugins.condor_direct import condor_direct

logger = logging.getLogger('plugin-condor_file_transfer')


class condor_file_transfer(condor_direct):

    ### Plugin Overrides ###

    batch_site = 'CondorFileTransfer'
    batch_resources = {}

    async def customize_task_config(self, task_cfg):
        """Do all file transfers via condor, so move files to batchsys"""
        in_files = []
        out_files = []

        def process_data(cfg):
            new_data = []
            for d in cfg.get('data', []):
                if d['remote'].startswith('osdf://'):
                    if d['movement'] in ('input', 'both'):
                        in_files.append(data['remote'])
                    if d['movement'] in ('output', 'both'):
                        local = data['local'] if data['local'] else os.path.basename(data['remote'])
                        remote = data['remote']
                        out_files.append(f'{local} = {remote}')
                else:
                    new_data.append(d)
            cfg['data'] = new_data

        process_data(task_cfg)
        for tray in task_cfg['trays']:
            process_data(tray)
            for module in tray['modules']:
                process_data(module)

        # batchsys config can be None in config!
        batchsys = task_cfg.get('batchsys', None)
        if not batchsys:
            batchsys = {}
        batchsys_condor = batchsys.get('condor', {})
        batchsys_condor['transfer_input_files'] = in_files
        batchsys_condor['transfer_output_remaps'] = out_files
        batchsys['condor'] = batchsys_condor
        task_cfg['batchsys'] = batchsys
