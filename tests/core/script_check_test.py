import shutil

import pytest

from iceprod.core.config import Config
import iceprod.core.script_check
from iceprod.core.script_check import (
    ScriptProblem,
    check_config_scripts,
    check_syntax,
    find_unexpanded,
    format_problems,
    parse_shell_error,
)


def make_config(args='', name='gen', **module):
    """Make a validated config with one task/tray/module"""
    mod = {'name': 'mod1', 'src': '/usr/bin/python3', 'args': args}
    mod.update(module)
    c = Config({
        'version': 3.2,
        'steering': {'parameters': {'gcd': '/data/gcd.i3'}},
        'tasks': [{
            'name': name,
            'trays': [{'name': 'main', 'modules': [mod]}],
        }],
    })
    c.fill_defaults()
    c.validate()
    return c.config


SCRIPT = """#!/bin/sh
set -e
OS_ARCH=$(/cvmfs/os_arch.sh)
HOME=${HOME:-$PWD}

# running tray main, iter 0
# running module mod1
python foo.py --gcd=$steering(gcdx)
"""


@pytest.mark.parametrize('stderr,line_no,message', [
    ("task_runner.sh: line 43: syntax error near unexpected token `('\ntask_runner.sh: line 43: `foo $a(b)'\n",
     43, "syntax error near unexpected token `('"),
    ('/tmp/dir_755629/task_runner.sh: 12: Syntax error: "(" unexpected\n',
     12, 'Syntax error: "(" unexpected'),
    ('something odd\n', None, 'something odd'),
    ('', None, ''),
])
def test_parse_shell_error(stderr, line_no, message):
    assert parse_shell_error(stderr) == (line_no, message)


async def test_check_syntax_ok(tmp_path):
    script = tmp_path / 'task_runner.sh'
    script.write_text('#!/bin/sh\necho "hello $(pwd)"\n')
    assert await check_syntax(script) is None


@pytest.mark.parametrize('shell', ['bash', 'sh'])
async def test_check_syntax_error(shell, tmp_path, monkeypatch):
    if not shutil.which(shell):
        pytest.skip(f'{shell} not available')
    monkeypatch.setattr(iceprod.core.script_check, 'get_shell', lambda: shutil.which(shell))
    script = tmp_path / 'task_runner.sh'
    script.write_text(SCRIPT)

    p = await check_syntax(script)
    assert p is not None
    assert p.kind == 'syntax'
    assert p.line_no == 8
    assert p.line == 'python foo.py --gcd=$steering(gcdx)'
    assert p.context == 'tray "main", module "mod1"'
    assert '$steering(' in p.hint
    assert p.message


async def test_check_syntax_fail_open(tmp_path, monkeypatch):
    monkeypatch.setattr(iceprod.core.script_check, 'get_shell', lambda: str(tmp_path / 'no-such-shell'))
    script = tmp_path / 'task_runner.sh'
    script.write_text(SCRIPT)
    assert await check_syntax(script) is None


async def test_check_syntax_timeout(tmp_path, monkeypatch):
    # a fake "shell" that hangs
    shell = tmp_path / 'slow.sh'
    shell.write_text('#!/bin/sh\nexec sleep 10\n')
    shell.chmod(0o755)
    monkeypatch.setattr(iceprod.core.script_check, 'get_shell', lambda: str(shell))
    script = tmp_path / 'task_runner.sh'
    script.write_text(SCRIPT)
    assert await check_syntax(script, timeout=.1) is None


def test_find_unexpanded(tmp_path):
    script = tmp_path / 'task_runner.sh'
    script.write_text("""#!/bin/sh
OS_ARCH=$(/cvmfs/os_arch.sh)
X=$(preamble)

# running tray 0, iter 0
# running module
# comment $steering(ignored)
python foo.py --a=$(missing) --b=$args(x)
python - <<____HERE
print('$steering(in_heredoc)')
____HERE
echo $( pwd ) `hostname` $(/bin/date) ${HOME} $PWD $(echo a b)
""")
    problems = find_unexpanded(script, start_line=4)
    assert [p.line_no for p in problems] == [8, 10]
    assert all(p.kind == 'unexpanded' for p in problems)
    assert problems[0].message == '$(missing), $args('
    assert problems[0].context == 'tray "0"'
    assert '$(missing)' in problems[0].hint
    assert problems[1].message == '$steering('

    # the preamble is checked if start_line is not set
    assert [p.line_no for p in find_unexpanded(script)] == [3, 8, 10]


def test_excerpt_long_line(tmp_path):
    script = tmp_path / 'task_runner.sh'
    script.write_text('env ' + 'A=b '*200 + 'python foo.py --x=$(missing) ' + 'c '*200 + '\n')
    p = find_unexpanded(script)[0]
    assert len(p.line) <= iceprod.core.script_check.EXCERPT_WIDTH + 6
    assert p.line.startswith('...')
    assert p.line.endswith('...')
    assert '$(missing)' in p.line


def test_format():
    p = ScriptProblem(kind='syntax', line_no=3, line='foo $a(b)', message='bad', context='task "x"', hint='Hint: h')
    assert p.format() == 'Generated task script has a syntax error (task "x") at line 3: bad in: foo $a(b) Hint: h'
    assert '\n' not in str(p)
    assert p.format(multiline=True) == 'Generated task script has a syntax error (task "x") at line 3: bad\n  > foo $a(b)\n  Hint: h'

    p = ScriptProblem(kind='unexpanded', line_no=None, line='', message='$(x)')
    assert p.format() == 'Generated task script has an unexpanded IceProd expression: $(x)'

    assert format_problems([p]) == 'Config has 1 task script problem:\n' + p.format(multiline=True)
    assert format_problems([p]*7, limit=2) == '\n'.join([
        'Config has 7 task script problems:',
        p.format(multiline=True),
        p.format(multiline=True),
        '... and 5 more',
    ])


@pytest.mark.parametrize('args', [
    '--gcd=$steering(gcd)',
    '--x=$( pwd ) `hostname` ${HOME}',
    '--dataset=$(dataset) --job=$(job) --task=$(task)',
    '',
])
async def test_check_config_scripts_ok(args):
    assert await check_config_scripts(make_config(args)) == []


async def test_check_config_scripts_syntax():
    problems = await check_config_scripts(make_config('--gcd=$steering(gcdx)'))
    assert len(problems) == 1  # not also reported as unexpanded
    p = problems[0]
    assert p.kind == 'syntax'
    assert p.context == 'task "gen", tray "main", module "mod1"'
    assert '$steering(gcdx)' in p.line


async def test_check_config_scripts_unexpanded():
    problems = await check_config_scripts(make_config('--x=$(missing)'))
    assert len(problems) == 1
    p = problems[0]
    assert p.kind == 'unexpanded'
    assert p.message == '$(missing)'
    assert p.context.startswith('task "gen"')


async def test_check_config_scripts_class_module():
    """Class modules use a python heredoc, so this is not a syntax error"""
    cfg = make_config({'x': '$steering(gcdx)'}, src='', running_class='foo.Bar')
    problems = await check_config_scripts(cfg)
    assert len(problems) == 1
    assert problems[0].kind == 'unexpanded'
    assert problems[0].message == '$steering('


async def test_check_config_scripts_multiple_tasks():
    cfg = make_config('--x=$(missing)')
    task2 = dict(cfg['tasks'][0], name='second')
    cfg['tasks'].append(task2)
    problems = await check_config_scripts(cfg)
    assert [p.context.split(',')[0] for p in problems] == ['task "gen"', 'task "second"']


async def test_check_config_scripts_convert_error():
    """Conversion errors are skipped, not reported"""
    cfg = make_config()
    cfg['tasks'][0]['trays'][0]['modules'][0]['src'] = ''
    assert await check_config_scripts(cfg) == []


async def test_check_config_scripts_no_side_effects():
    cfg = make_config('--x=$(job)')
    before = repr(cfg)
    await check_config_scripts(cfg, jobs_submitted=10)
    assert repr(cfg) == before
