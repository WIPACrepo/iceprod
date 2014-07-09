IceProd
=======

IceProd is a Python framework for distributed management of batch jobs. 
It runs as a layer on top of other batch middleware such as CREAM, Condor, 
and PBS and can pool together resources from different batch systems. 
The primary purpose is to coordinate and administer many large sets of 
jobs at once, keeping a history of the entire job lifecycle.

.. note:

    For IceCube users with CVMFS access, IceProd is already installed. 
    To load the environment execute::
    
        /cvmfs/icecube.wisc.edu/iceprod/stable/env-shell.sh
    
    or::
    
        source `/cvmfs/icecube.wisc.edu/iceprod/stable/setup.sh`
    
    depending on whether you want to get a new shell or load the variables
    into the current shell.

Installation
------------

**Easy Installation**: Download the latest source and build.

.. parsed-literal::

    tar xvzf icecube-$VERSION.tar.gz
    cd icecube-$VERSION
    python setup.py build
    sudo python setup.py install

**Platforms**: IceProd should run on any Unix-like platform, although only
Linux has been extensively tested and can be recommented for production
deployment (even though Mac OS X is derived from BSD and supports kqueue, its
networking performance is generally poor so it is recommended only for
development use).

**Prerequisites**: IceProd runs on python 2.7, 3.2, 3.3, and 3.4. 

The following is required:

Programs:

* curl

Python libraries:

* pycurl

* tornado

There are two types of database interface available:
* sqlite:  depends on apsw: http://code.google.com/p/apsw/
* mysql:   depends on mysqldb: https://pypi.python.org/pypi/MySQL-python

Other non-essential dependencies:
* p7zip       (for compression)
* nginx       (for ssl offloading and better security)
* squid       (for http proxy)
* libtool     (a globus dependency)
* perl 5.10 + modules: Archive::Tar Compress::Zlib Digest::MD5 File::Spec IO::Zlib Pod::Parser XML::Parser
              (a globus dependency)
* globus      (for gridftp)

Python non-essentials:
* pyasn1      (for generating ssl certs)
* pyopenssl   (for generating ssl certs)
* setproctitle(for setting a process name)
* pygridftp   (for gridftp-python integration)
* sphinx      (for generating documentation)
* coverage    (for tests)
* flexmock    (for tests)


**Manual Install of Prerequisites**:

`http://brew.sh Homebrew`_ and the 
`https://github.com/Homebrew/linuxbrew Linuxbrew fork`_
greatly simplify installation.

Requirements:

    a compiler
    curl, git, ruby
    openssl-dev, libz-dev, libbz2-dev, libncurses-dev, libxml2-dev, libxslt-dev, libexpat1-dev

Install Homebrew to the directory of your choice::

    git clone https://github.com/Homebrew/linuxbrew.git ~/.linuxbrew
    echo "export PATH=~/.linuxbrew/bin:~/.linuxbrew/sbin:$PATH" >> ~/.bash_profile
    echo "export LD_LIBRARY_PATH=~/.linuxbrew/lib:$LD_LIBRARY_PATH" >> ~/.bash_profile
    export PATH=~/.linuxbrew/bin:~/.linuxbrew/sbin:$PATH
    export LD_LIBRARY_PATH=~/.linuxbrew/lib:$LD_LIBRARY_PATH

Tap the iceprod repository::

    brew tap dsschult/iceprod

Install the main dependencies::

    brew install curl
    brew link curl --force
    brew install readline
    brew link readline --force
    brew install dsschult/iceprod/python
    brew install dsschult/iceprod/p7zip
    brew install nginx
    brew install apsw
    pip install pycurl tornado

Install optional dependencies::

    curl -kL http://install.perlbrew.pl | bash
    echo "source ~/perl5/perlbrew/etc/bashrc" >> ~/.bash_profile
    source ~/perl5/perlbrew/etc/bashrc
    perlbrew install perl-5.18.1
    perlbrew switch perl-5.18.1
    cpan App::cpanminus
    cpanm Archive::Tar Compress::Zlib Digest::MD5 File::Spec IO::Zlib Pod::Parser Test::Simple XML::Parser
    brew install dsschult/iceprod/libtool
    brew install dsschult/iceprod/globus-toolkit --gridftp-only
    echo "export GLOBUS_LOCATION=~/.linuxbrew" >> ~/.bash_profile
    export GLOBUS_LOCATION=~/.linuxbrew
    brew install globus-ca-certs
    brew install squid
    pip install pyasn1 pyopenssl sphinx coverage flexmock
    brew install python-gridftp

