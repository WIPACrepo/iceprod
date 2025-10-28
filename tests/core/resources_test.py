"""
Test script for resources
"""

import logging
import os

import iceprod.core.resources


logger = logging.getLogger('resources')

 
def test_000_Resources():
    r = iceprod.core.resources.Resources
    for t in ('cpu','gpu','memory','disk','time'):
        assert t in r.defaults


def test_230_du(tmp_path):
    du_dir = str(tmp_path / 'test')
    os.mkdir(du_dir)
    for f in ('a','b','c'):
        path = os.path.join(du_dir,f)
        open(path,'w').write('a'*100)
    assert iceprod.core.resources.du(du_dir) == 300


def test_231_du_symlink(tmp_path):
    du_dir = str(tmp_path / 'test')
    os.mkdir(du_dir)
    for f in ('a','b','c'):
        path = os.path.join(du_dir,f)
        open(path,'w').write('a'*100)
    os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
    assert iceprod.core.resources.du(du_dir) == 300


def test_232_du_dir_symlink(tmp_path):
    du_dir = str(tmp_path / 'test')
    os.mkdir(du_dir)
    for f in ('a','b','c'):
        path = os.path.join(du_dir,f)
        open(path,'w').write('a'*100)
    os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
    os.mkdir(os.path.join(du_dir,'subdir'))
    for f in ('a','b','c'):
        path = os.path.join(du_dir,'subdir',f)
        open(path,'w').write('a'*100)
    os.symlink(os.path.join(du_dir,'subdir'), os.path.join(du_dir,'s2'))
    os.symlink(os.path.join(du_dir,'subdir','a'), os.path.join(du_dir,'subdir','s3'))
    assert iceprod.core.resources.du(du_dir) == 600


def test_300_group_hasher():
    r = iceprod.core.resources.Resources.defaults
    h = iceprod.core.resources.group_hasher(r)


def test_301_group_hasher():
    r = iceprod.core.resources.Resources.defaults.copy()

    hashes = set()
    for i in range(1,100):
        r['memory'] = i
        hashes.add(iceprod.core.resources.group_hasher(r))
    logger.info('hashes: %r', hashes)
    assert len(hashes) < 15


def test_400_sanitized_requirements():
    r = {'cpu':2,'gpu':1}
    ret = iceprod.core.resources.sanitized_requirements(r)
    assert r['cpu'] == ret['cpu']
    assert r['gpu'] == ret['gpu']
    assert 'memory' not in ret

    r = {'os':['RHEL_7_x86_64'], 'site':'foo'}
    ret = iceprod.core.resources.sanitized_requirements(r)
    assert r['os'] == ret['os']
    assert r['site'] == ret['site']
    assert 'gpu' not in ret
    assert 'memory' not in ret


def test_401_sanitized_requirements():
    r = {'cpu':2,'gpu':1}
    ret = iceprod.core.resources.sanitized_requirements(r, use_defaults=True)
    assert r['cpu'] == ret['cpu']
    assert r['gpu'] == ret['gpu']
    assert iceprod.core.resources.Resources.defaults['memory'] == ret['memory']

    r = {'os':['RHEL_7_x86_64'], 'site':'foo'}
    ret = iceprod.core.resources.sanitized_requirements(r, use_defaults=True)
    assert r['os'] == ret['os']
    assert r['site'] == ret['site']
    assert 'gpu' not in ret
    assert iceprod.core.resources.Resources.defaults['memory'] == ret['memory']
