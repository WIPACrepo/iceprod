"""
Sanity checks for generated task scripts (``task_runner.sh``).

IceProd converts a dataset config into a shell script with
:class:`iceprod.core.exe.WriteToScript`. Module args are not quoted, so that
shell expansion keeps working. The downside is that config mistakes, most
commonly an IceProd expression that could not be expanded (for example
``$steering(typo)``), only show up as a cryptic shell syntax error on the
worker node.

This module finds those problems before the script runs:

* :func:`check_syntax` - run ``bash -n`` (or ``sh -n``) on a script.
* :func:`find_unexpanded` - find leftover IceProd expressions.
* :func:`check_config_scripts` - convert every task of a dataset config
  with dummy job values, then run both checks.
"""

import asyncio
import copy
import logging
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from iceprod.core import config as config_mod
from iceprod.core.exe import WriteToScript

logger = logging.getLogger('script_check')

#: IceProd function syntax, like ``$steering(x)``. This is never valid shell
#: syntax outside of quotes.
RE_IP_FUNCTION = re.compile(r'\$[A-Za-z_]\w*\(')
#: A bare IceProd lookup, like ``$(dataset)``.
RE_IP_LOOKUP = re.compile(r'\$\([A-Za-z_][\w.\-]*\)')

#: Line number in shell -n output; bash uses "line 43:", dash uses ": 43:"
RE_SHELL_LINE = re.compile(r':\s*(?:line\s+)?(\d+):\s*')

#: Comments written by WriteToScript.convert(), used for error context
RE_TRAY_COMMENT = re.compile(r'^# running tray (.*?), iter \d+\s*$')
RE_MODULE_COMMENT = re.compile(r'^# running module ?(.*)$')

EXCERPT_WIDTH = 300
#: characters to show before the problem position in an excerpt
EXCERPT_LEAD = 60
HINT = 'Hint: "{}" looks like an IceProd expression that was not expanded; check the name/spelling and that the parameter exists.'


@dataclass
class ScriptProblem:
    """A problem found in a generated task script."""
    #: "syntax" for a shell syntax error, "unexpanded" for a leftover IceProd expression
    kind: Literal['syntax', 'unexpanded']
    #: 1-based line number in the script (None if unknown)
    line_no: int | None
    #: excerpt of the offending line
    line: str
    #: shell error message, or the unexpanded expressions
    message: str
    #: where in the config the line came from (task/tray/module)
    context: str = ''
    #: optional suggestion for the user
    hint: str = ''

    def format(self, multiline: bool = False) -> str:
        """
        Format the problem for users.

        Args:
            multiline: use newlines (for logs); single line is required for HTTP reasons

        Returns:
            str: user-facing message
        """
        title = 'a syntax error' if self.kind == 'syntax' else 'an unexpanded IceProd expression'
        where = f' ({self.context})' if self.context else ''
        at = f' at line {self.line_no}' if self.line_no else ''
        parts = [f'Generated task script has {title}{where}{at}: {self.message}']
        if self.line:
            parts.append(f'> {self.line}' if multiline else f'in: {self.line}')
        if self.hint:
            parts.append(self.hint)
        return ('\n  ' if multiline else ' ').join(parts)

    def __str__(self) -> str:
        return self.format()


def _excerpt(line: str, pos: int = 0, width: int = EXCERPT_WIDTH) -> str:
    """Shorten a long line to `width` characters around `pos`."""
    stripped = line.lstrip()
    pos = max(0, pos - (len(line) - len(stripped)))
    line = stripped.rstrip()
    if len(line) <= width:
        return line
    start = max(0, min(pos - EXCERPT_LEAD, len(line) - width))
    ret = line[start:start+width]
    if start > 0:
        ret = '...' + ret
    if start + width < len(line):
        ret += '...'
    return ret


def _context(lines: list[str], idx: int) -> str:
    """
    Find the tray and module that wrote line `idx` (0-based), using the
    comments WriteToScript puts before each tray and module.
    """
    tray = module = None
    for line in reversed(lines[:idx+1]):
        if module is None and (m := RE_MODULE_COMMENT.match(line)):
            module = m.group(1).strip()
        elif m := RE_TRAY_COMMENT.match(line):
            tray = m.group(1).strip()
            break
    parts = []
    if tray is not None:
        parts.append(f'tray "{tray}"')
    if module:
        parts.append(f'module "{module}"')
    return ', '.join(parts)


def _hint(line: str) -> tuple[str, int]:
    """Get a hint and position for the first IceProd expression in the line."""
    func = RE_IP_FUNCTION.search(line)
    lookup = RE_IP_LOOKUP.search(line)
    if func and (not lookup or func.start() <= lookup.start()):
        return HINT.format(func.group(0)), func.start()
    if lookup:
        return HINT.format(lookup.group(0)), lookup.start()
    return '', 0


def parse_shell_error(stderr: str) -> tuple[int | None, str]:
    """
    Parse the output of ``bash -n`` or ``sh -n`` (dash).

    Examples::

        task_runner.sh: line 43: syntax error near unexpected token `('
        task_runner.sh: 43: Syntax error: "(" unexpected

    Returns:
        tuple: (line number or None, error message)
    """
    first = next((line for line in stderr.splitlines() if line.strip()), '')
    if m := RE_SHELL_LINE.search(first):
        return int(m.group(1)), first[m.end():].strip()
    return None, first.strip()


def get_shell() -> str:
    """
    Get the shell to check with. Prefer bash: the worker's /bin/sh is
    bash on EL, while some submit hosts and containers use dash.
    """
    return shutil.which('bash') or '/bin/sh'


async def check_syntax(script: Path, timeout: float = 10.) -> ScriptProblem | None:
    """
    Check a script for shell syntax errors with ``bash -n``.

    This fails open: if the shell cannot be run or times out, a warning
    is logged and None is returned, so infrastructure problems never
    block a task.

    Args:
        script: path to the script
        timeout: max seconds for the check

    Returns:
        ScriptProblem if there is a syntax error, else None
    """
    shell = get_shell()
    try:
        proc = await asyncio.create_subprocess_exec(
            shell, '-n', str(script),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError:
        logger.warning('cannot run %s to check script syntax', shell, exc_info=True)
        return None
    try:
        _, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        logger.warning('script syntax check timed out for %s', script)
        proc.kill()
        await proc.wait()
        return None
    if proc.returncode == 0:
        return None

    stderr = stderr_bytes.decode('utf-8', errors='replace')
    line_no, message = parse_shell_error(stderr)
    lines = script.read_text(errors='replace').splitlines()
    line = context = hint = ''
    if line_no and 0 < line_no <= len(lines):
        raw = lines[line_no-1]
        hint, pos = _hint(raw)
        line = _excerpt(raw, pos)
        context = _context(lines, line_no-1)
    return ScriptProblem(
        kind='syntax',
        line_no=line_no,
        line=line,
        message=message or 'unknown shell syntax error',
        context=context,
        hint=hint,
    )


def find_unexpanded(script: Path, start_line: int = 1) -> list[ScriptProblem]:
    """
    Find IceProd expressions that were not expanded, such as
    ``$steering(typo)`` or ``$(typo)``.

    Comment lines and the script preamble are skipped.

    Args:
        script: path to the script
        start_line: first (1-based) line to check; use `WriteToScript.preamble_lines + 1`

    Returns:
        list of ScriptProblem
    """
    lines = script.read_text(errors='replace').splitlines()
    ret = []
    for idx in range(max(start_line, 1) - 1, len(lines)):
        raw = lines[idx]
        if raw.lstrip().startswith('#'):
            continue
        matches = [m for regex in (RE_IP_FUNCTION, RE_IP_LOOKUP) for m in regex.finditer(raw)]
        if not matches:
            continue
        first = min(matches, key=lambda m: m.start())
        hint, _ = _hint(raw[first.start():])
        exprs = sorted({m.group(0) for m in matches})
        ret.append(ScriptProblem(
            kind='unexpanded',
            line_no=idx+1,
            line=_excerpt(raw, first.start()),
            message=', '.join(exprs),
            context=_context(lines, idx),
            hint=hint,
        ))
    return ret


def _dummy_task(cfg: dict, task_index: int, jobs_submitted: int) -> config_mod.Task:
    """Build a Task with dummy ids, like `i3exec` testing mode."""
    d = config_mod.Dataset(
        dataset_id='datasetid',
        dataset_num=1,
        jobs_submitted=jobs_submitted,
        tasks_submitted=jobs_submitted*len(cfg['tasks']),
        tasks_per_job=len(cfg['tasks']),
        status='processing',
        priority=0,
        group='group',
        user='user',
        debug=False,
        config=cfg,
    )
    return config_mod.Task(
        dataset=d,
        job=config_mod.Job(d, 'jobid', 0, 'processing'),
        task_id='taskid',
        task_index=task_index,
        name=cfg['tasks'][task_index].get('name', str(task_index)),
        depends=[],
        requirements={},
        status='processing',
        site='site',
        stats={},
    )


async def check_config_scripts(config: dict, jobs_submitted: int = 1) -> list[ScriptProblem]:
    """
    Check the generated scripts for every task in a dataset config.

    Each task is converted with dummy values (job index 0) in a temporary
    directory, then checked for shell syntax errors and unexpanded IceProd
    expressions.

    If conversion itself fails, that is logged and the task is skipped:
    dummy values may differ from real ones, so it is not a reliable error.

    Args:
        config: dataset config, after `fill_defaults()` and `validate()`
        jobs_submitted: number of jobs in the dataset

    Returns:
        list of ScriptProblem (empty if everything is ok)
    """
    problems = []
    for i, task_cfg in enumerate(config.get('tasks', [])):
        task_name = task_cfg.get('name', str(i))
        # WriteToScript modifies the config options, so use a fresh copy
        cfg = copy.deepcopy(config)
        cfg.setdefault('options', {})
        with tempfile.TemporaryDirectory(prefix='iceprod_script_check_') as tmpdir:
            try:
                task = _dummy_task(cfg, i, max(int(jobs_submitted), 1))
                ws = WriteToScript(task, workdir=Path(tmpdir), logger=logger)
                script = await ws.convert(transfer=True)
            except Exception:
                logger.warning('cannot convert task %r for script check', task_name, exc_info=True)
                continue
            task_problems = []
            syntax_line = None
            if p := await check_syntax(script):
                task_problems.append(p)
                syntax_line = p.line_no
            # skip the line already reported as a syntax error
            task_problems.extend(
                p for p in find_unexpanded(script, start_line=ws.preamble_lines + 1)
                if p.line_no != syntax_line
            )
            for p in task_problems:
                p.context = f'task "{task_name}"' + (f', {p.context}' if p.context else '')
            problems.extend(task_problems)
    return problems


def format_problems(problems: list[ScriptProblem], limit: int = 5) -> str:
    """
    Format problems as a multi-line message for an HTTP error.

    The first line is a summary, because the HTTP status line only keeps
    the first line. The full message goes in the JSON body.

    Args:
        problems: list of ScriptProblem
        limit: max number of problems to include

    Returns:
        str
    """
    n = len(problems)
    lines = [f'Config has {n} task script problem{"s" if n != 1 else ""}:']
    lines.extend(p.format(multiline=True) for p in problems[:limit])
    if n > limit:
        lines.append(f'... and {n - limit} more')
    return '\n'.join(lines)
