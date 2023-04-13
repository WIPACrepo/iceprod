
DEFAULT_OPTIONS = {
    'dataset': 0,
    'job': 0,
    'jobs_submitted': 1,
    'resource_url': 'http://prod-exe.icecube.wisc.edu/',
    'data_url': 'gsiftp://gridftp.icecube.wisc.edu/',
    'svn_repository': 'http://code.icecube.wisc.edu/svn/',
    'site_temp': 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/',
    'dataset_temp': '$(site_temp)/$(dataset)',
    'job_temp': '$(site_temp)/$(dataset)/$(job)',
}


def add_default_options(opts):
    """Add default config options"""
    for k in DEFAULT_OPTIONS:
        if k not in opts:
            opts[k] = DEFAULT_OPTIONS[k]
