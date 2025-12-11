"""
Test script for core exe
"""

import logging
import pytest

import iceprod.core.config
import iceprod.core.exe
from iceprod.core.exe import Data, Transfer
from iceprod.core.defaults import add_default_options


logger = logging.getLogger('exe_test')


def get_task(config):
    d = iceprod.core.config.Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, config)
    d.fill_defaults()
    add_default_options(d.config['options'])
    j = iceprod.core.config.Job(d, 'j123', 1, 'processing')
    t = iceprod.core.config.Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})
    return t


def test_config_parser():
    t = get_task({
        'steering': {
            'parameters': {'foo': 1, 'bar': [2, 3, 4]}
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }]
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    assert c.parseValue('$steering(foo)') == 1
    assert c.parseObject({'foo': '$steering(bar)'}, {}) == {'foo': [2, 3, 4]}
    assert c.parseObject('$(bar)', {'parameters': {'bar': {'a': 'b'}}}) == {'a': 'b'}


def test_scope_env():
    t = get_task({
        'steering': {
            'parameters': {'foo': 1, 'bar': [2, 3, 4]}
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [{
                'movement': 'input',
                'remote': 'https://foo.bar/baz'
            }],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    with iceprod.core.exe.scope_env(c, t.dataset.config['steering']) as env:
        # test parameters
        assert env.parameters == t.dataset.config['steering']['parameters']
        assert c.parseObject('$(foo)', env.to_parser_dict()) == 1

        # test parsing data
        with iceprod.core.exe.scope_env(c, t.dataset.config['tasks'][0], env) as tenv:
            assert tenv.input_files == [Data('https://foo.bar/baz', 'baz', Transfer.TRUE)]
        assert env.input_files == []


def test_download_data():
    data = {
        'movement': 'input',
        'remote': 'https://foo.bar/baz'
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.downloadData(data, c, logger=logger)
    assert ret == Data('https://foo.bar/baz', 'baz', Transfer.TRUE)


def test_download_data_false():
    data = {
        'movement': 'input',
        'remote': 'https://foo.bar/baz',
        'transfer': 0,
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.downloadData(data, c, logger=logger)
    assert not ret


def test_download_data_maybe():
    data = {
        'movement': 'input',
        'remote': 'https://foo.bar/baz',
        'transfer': 'maybe',
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.downloadData(data, c, logger=logger)
    assert ret == Data('https://foo.bar/baz', 'baz', Transfer.MAYBE)


def test_download_data_invalid():
    data = {
        'movement': 'input',
        'remote': ''
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    with pytest.raises(Exception):
        iceprod.core.exe.downloadData(data, c, logger=logger)


def test_download_data_no_transfer():
    data = {
        'movement': 'input',
        'remote': 'https://foo.bar/baz',
        'transfer': 'no'
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.downloadData(data, c, logger=logger)
    assert not ret


def test_download_data_job_temp():
    data = {
        'movement': 'input',
        'local': 'baz',
        'remote': '',
        'type': 'job_temp'
    }
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.downloadData(data, c, logger=logger)
    assert ret == Data('https://foo.bar/baz', 'baz', Transfer.TRUE)


def test_upload_data():
    data = {
        'movement': 'output',
        'remote': 'https://foo.bar/baz'
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.uploadData(data, c, logger=logger)
    assert ret == Data('https://foo.bar/baz', 'baz', Transfer.TRUE)


def test_upload_data_invalid():
    data = {
        'movement': 'output',
        'remote': ''
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    with pytest.raises(Exception):
        iceprod.core.exe.uploadData(data, c, logger=logger)


def test_upload_data_no_transfer():
    data = {
        'movement': 'output',
        'remote': 'https://foo.bar/baz',
        'transfer': 'no'
    }
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.uploadData(data, c, logger=logger)
    assert not ret


def test_upload_data_job_temp():
    data = {
        'movement': 'output',
        'local': 'baz',
        'type': 'job_temp'
    }
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
            'data': [data],
        }]
    })

    c = iceprod.core.exe.ConfigParser(t.dataset, logger=logger)
    ret = iceprod.core.exe.uploadData(data, c, logger=logger)
    assert ret == Data('https://foo.bar/baz', 'baz', Transfer.TRUE)


async def test_write_to_script_no_module(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{}]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    with pytest.raises(iceprod.core.exe.ConfigError):
        await ws.convert()
    assert not ws.infiles
    assert not ws.outfiles


async def test_write_to_script_module_src(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == 'python foo.py'


async def test_write_to_script_module_shell(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.sh'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == '/bin/sh foo.sh'


async def test_write_to_script_module_binary(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == './foo'

async def test_write_to_script_module_binary_fullpath(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': '/cvmfs/foo'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == '/cvmfs/foo'


async def test_write_to_script_os_arch(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': '/cvmfs/foo/$environ(OS_ARCH)'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == '/cvmfs/foo/$OS_ARCH'


async def test_write_to_script_tray_iter(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'iterations': 3,
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py',
                    'args': '$(iter)'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    logging.debug('script: \n%s', script)
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-3:] == [
        'python foo.py 0',
        'python foo.py 1',
        'python foo.py 2',
    ]


async def test_write_to_script_module_env_clear(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': True,
                    'src': 'foo.py'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1].startswith('env -i ')
    assert lines[-1].endswith(' python foo.py')


async def test_write_to_script_module_env_shell(tmp_path):
    t = get_task({
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'env_shell': '/foo/bar/baz.sh',
                    'src': 'foo.py'
                }]
            }],
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert not ws.infiles
    assert not ws.outfiles
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == '/foo/bar/baz.sh python foo.py'


async def test_write_to_script_data(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py'
                }]
            }],
            'data': [{
                'movement': 'input',
                'type': 'permanent',
                'remote': 'https://foo.bar/baz',
            }, {
                'movement': 'output',
                'type': 'job_temp',
                'local': '1234',
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert ws.infiles == {Data('https://foo.bar/baz', 'baz', Transfer.TRUE)}
    assert ws.outfiles == {Data('https://foo.bar/1234', '1234', Transfer.TRUE)}
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == 'python foo.py'


async def test_write_to_script_data_task_files(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py',
                    'args': '--foo -b $(input) $(output)',
                }]
            }],
            'data': [{
                'movement': 'input',
                'type': 'permanent',
                'remote': 'https://foo.bar/baz',
            }, {
                'movement': 'output',
                'type': 'job_temp',
                'local': '1234',
            }]
        }]
    })
    t.task_files = [{
        'movement': 'input',
        'type': 'permanent',
        'remote': 'https://foo.bar/blah',
        'local': '',
        'transfer': True,
    }, {
        'movement': 'output',
        'type': 'job_temp',
        'remote': '',
        'local': 'abcde',
        'transfer': True,
    }]

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert ws.infiles == {Data('https://foo.bar/baz', 'baz', Transfer.TRUE), Data('https://foo.bar/blah', 'blah', Transfer.TRUE)}
    assert ws.outfiles == {Data('https://foo.bar/1234', '1234', Transfer.TRUE), Data('https://foo.bar/abcde', 'abcde', Transfer.TRUE)}
    script = open(scriptpath).read()
    lines = [line for line in script.split('\n') if not (not line.strip() or line.startswith('#') or line.startswith('set '))]
    assert lines[-1] == 'python foo.py --foo -b baz blah 1234 abcde'


async def test_write_to_script_data_dups(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py',
                    'data': [{
                        'movement': 'input',
                        'type': 'permanent',
                        'remote': 'https://foo.bar/baz',
                    }, {
                        'movement': 'output',
                        'type': 'job_temp',
                        'local': '1234',
                    }]
                }],
                'data': [{
                    'movement': 'input',
                    'type': 'permanent',
                    'remote': 'https://foo.bar/baz',
                }]
            }],
            'data': [{
                'movement': 'input',
                'type': 'permanent',
                'remote': 'https://foo.bar/baz',
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert ws.infiles == {Data('https://foo.bar/baz', 'baz', Transfer.TRUE)}
    assert ws.outfiles == {Data('https://foo.bar/1234', '1234', Transfer.TRUE)}


async def test_write_to_script_data_iterations(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'steering': {
            'parameters': {
                'foo0': 'baz',
                'test': 'foo_bar_$steering(foo$(iter))',
                'remote': 'https://foo.bar',
            }
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py'
                }],
                'data': [{
                    'movement': 'input',
                    'type': 'permanent',
                    'remote': '$steering(remote)/$steering(test)',
                }]
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert()

    assert ws.infiles == {Data('https://foo.bar/foo_bar_baz', 'foo_bar_baz', Transfer.TRUE)}
    assert ws.outfiles == set()


async def test_write_to_script_data_transfer(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'steering': {
            'parameters': {
                'foo0': 'baz',
                'test': 'foo_bar_$steering(foo$(iter))',
                'remote': 'https://foo.bar',
            }
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py',
                    'data': [{
                        'movement': 'input',
                        'type': 'permanent',
                        'remote': 'gsiftp://foo.bar/baz',
                        'local': 'foo_local',
                    }, {
                        'movement': 'output',
                        'type': 'permanent',
                        'transfer': 'maybe',
                        'remote': 'gsiftp://foo.bar/stats',
                        'local': 'stats',
                    }, {
                        'movement': 'output',
                        'type': 'permanent',
                        'transfer': True,
                        'remote': 'gsiftp://foo.bar/stats2',
                        'local': 'stats2',
                    }, {
                        'movement': 'both',
                        'type': 'permanent',
                        'transfer': 'maybe',
                        'remote': 'osdf://icecube/wipac/data/file'
                    }]
                }],
                'data': [{
                    'movement': 'input',
                    'type': 'permanent',
                    'remote': '$steering(remote)/$steering(test)',
                }]
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert(transfer=True)

    script = open(scriptpath).read()
    assert 'gsiftp://foo.bar/baz' in script
    assert 'foo_local' in script
    assert 'gsiftp://foo.bar/stats' in script
    assert 'if [ -f stats ' in script
    assert 'gsiftp://foo.bar/stats2' in script
    assert 'if [ -f stats2 ' not in script
    assert 'object get' in script
    assert 'object put' in script
    assert ' osdf://icecube/wipac/data/file ' in script
    assert 'if [ -f file ' in script

    assert ws.infiles == {Data('https://foo.bar/foo_bar_baz', 'foo_bar_baz', Transfer.TRUE)}
    assert ws.outfiles == set()


async def test_write_to_script_executable(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'steering': {
            'parameters': {
                'foo0': 'baz',
                'test': 'foo_bar_$steering(foo$(iter))',
                'remote': 'https://foo.bar',
            }
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': '$steering(remote)/foo.py',
                }]
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    await ws.convert(transfer=True)

    assert ws.infiles == {Data('https://foo.bar/foo.py', 'foo.py', Transfer.TRUE)}
    assert ws.outfiles == set()


async def test_write_to_script_configs(tmp_path):
    t = get_task({
        'options': {
            'job_temp': 'https://foo.bar',
        },
        'steering': {
            'parameters': {
                'foo0': 'baz',
                'test': 'foo_bar_$steering(foo$(iter))',
                'remote': 'https://foo.bar',
            }
        },
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py',
                    'configs': {
                        'snowstorm.json': {
                            'foo': 1,
                            'bar': 2,
                            'baz': {'remote': '$steering(remote)'},
                        },
                    },
                }]
            }]
        }]
    })

    ws = iceprod.core.exe.WriteToScript(t, workdir=tmp_path, logger=logger)
    scriptpath = await ws.convert(transfer=True)

    config_path = tmp_path / 'snowstorm.json'
    assert config_path.exists()
    assert ws.infiles == {Data(str(config_path), 'snowstorm.json', Transfer.TRUE)}
    assert ws.outfiles == set()
