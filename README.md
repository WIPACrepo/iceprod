<!--- Top of README Badges (automated) --->
[![PyPI](https://img.shields.io/pypi/v/iceprod)](https://pypi.org/project/iceprod/) [![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/iceprod?include_prereleases)](https://github.com/WIPACrepo/iceprod) [![Versions](https://img.shields.io/pypi/pyversions/iceprod.svg)](https://pypi.org/project/iceprod/) [![PyPI - License](https://img.shields.io/pypi/l/iceprod)](https://github.com/WIPACrepo/iceprod/blob/master/LICENSE) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/iceprod)](https://github.com/WIPACrepo/iceprod/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/iceprod)](https://github.com/WIPACrepo/iceprod/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen)
<!--- End of README Badges (automated) --->
# IceProd

[![image](https://zenodo.org/badge/58235078.svg)](https://zenodo.org/badge/latestdoi/58235078)
<!--- Top of README Metadata Section (automated) --->

<!--- note: this information is pulled from the pyproject.toml --->

<dl>
    <dt><sub>Description</sub></dt>
    <dd><sub>IceCube dataset management system</sub></dd>
    <dt><sub>Authors</sub></dt>
    <dd><sub>WIPAC Developers / <a href='mailto:developers@icecube.wisc.edu'>developers@icecube.wisc.edu</a></sub></dd>
    <dt><sub>Keywords</sub></dt>
    <dd><sub>WIPAC&nbsp;&nbsp;·&nbsp;&nbsp;batch&nbsp;&nbsp;·&nbsp;&nbsp;workload</sub></dd>
    <dt><sub>URLs</sub></dt>
    <dd><sub><a href='https://pypi.org/project/iceprod/'>Homepage</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href='https://github.com/WIPACrepo/iceprod/issues'>Tracker</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href='https://github.com/WIPACrepo/iceprod'>Source</a></sub></dd>
</dl>

<br>
<!--- End of README Metadata Section (automated) --->

IceProd is a Python framework for distributed management of batch jobs.
It runs as a layer on top of other batch middleware, such as HTCondor,
and can pool together resources from different batch systems. The
primary purpose is to coordinate and administer many large sets of jobs
at once, keeping a history of the entire job lifecycle.

See also: Aartsen, Mark G., et al. \"The IceProd framework: Distributed
data processing for the IceCube neutrino observatory.\" Journal of
parallel and distributed computing 75 (2015): 198-211.

**Note:**

For IceCube users with CVMFS access, IceProd is already installed. To
load the environment execute:

    /cvmfs/icecube.wisc.edu/iceprod/latest/env-shell.sh

or:

    eval `/cvmfs/icecube.wisc.edu/iceprod/latest/setup.sh`

depending on whether you want to get a new shell or load the variables
into the current shell.

## Installation

**Platforms**:

IceProd should run on any Unix-like platform, although only Linux has
been extensively tested and can be recommented for production
deployment.

**Prerequisites**:

Listed here are any packages outside pip:

-   Python 3.7+
-   MongoDB 3.6+ (for the REST API)
-   nginx (for ssl offloading and better security)
-   globus (for data transfer)

**Installation**:

From the latest release:

Get the tarball link from
<https://github.com/WIPACrepo/iceprod/releases/latest>

Then install like:

    pip install https://github.com/WIPACrepo/iceprod/archive/v2.0.0.tar.gz

**Installing from master**:

If you must install the dev version from master, do:

    pip install --upgrade git+git://github.com/WIPACrepo/iceprod.git#egg=iceprod
