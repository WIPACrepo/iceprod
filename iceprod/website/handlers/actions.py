"""
Website action handlers.

Dataset/task/job actions (reset, suspend, priority, ...) used to be called
directly from page JavaScript against the REST API, using an access token
embedded in the page.  These handlers replace that: pages submit plain
HTML forms (protected by Tornado XSRF), and the website calls the REST API
server-side with the user's own credentials.

Flow (mirrors dataset submission in `submit.py`):

* Short operations (priority, truncate): call REST, then 303 back to the
  originating page (Post/Redirect/Get).
* Long operations (reset, hard_reset, suspend, delete_logs): queue an
  `/actions/<type>` message, then 303 to `/actions/status/<type>/<id>`.
  That page reloads every second, and 303s back to the originating page
  when the action is complete.

Redirect safety: the return *path* is always derived server-side from the
action payload.  The user only influences an allowlisted query string
(`return_query`) and a browse/detail hint (`return_page`).
"""
import logging
import math
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode

import requests.exceptions
import tornado.web

from .base import PublicHandler, authenticated

logger = logging.getLogger('website-actions')

#: action service types the status page may poll
ACTION_TYPES = frozenset({'dataset_status', 'task_status', 'job_status', 'delete_logs'})

#: status changes supported by the dataset/task/job status services
STATUS_ACTIONS = frozenset({'reset', 'hard_reset', 'suspend'})

#: query args the browse/detail pages read (see task.py, job.py)
RETURN_QUERY_KEYS = frozenset({'status', 'name'})

#: allowed values for the `return_page` hint
RETURN_PAGES = frozenset({'browse', 'detail'})


def sanitize_return_query(return_query: str | None) -> str:
    """
    Reduce a user-supplied query string to allowlisted keys, re-encoded.

    Args:
        return_query: raw query string (without leading `?`)

    Returns:
        a safe, url-encoded query string (may be empty)
    """
    if not return_query:
        return ''
    try:
        pairs = parse_qsl(return_query, keep_blank_values=False, max_num_fields=20)
    except ValueError:
        return ''
    return urlencode([(k, v) for k, v in pairs if k in RETURN_QUERY_KEYS])


def _seg(val: Any) -> str:
    """Quote a value for use as a single url path segment."""
    return quote(str(val), safe='')


def build_return_url(action_type: str, payload: dict[str, Any], return_query: str | None = None, return_page: str | None = None) -> str:
    """
    Build the url to send the user back to after an action.

    The path only comes from the action payload, never user input.

    Args:
        action_type: action service type
        payload: action message payload
        return_query: user query string, sanitized here
        return_page: `browse` or `detail` hint for task/job actions

    Returns:
        a site-relative url
    """
    dataset_id = payload.get('dataset_id')
    if not dataset_id:
        return '/dataset'
    base = f'/dataset/{_seg(dataset_id)}'

    def _browse_or_detail(kind: str, ids: list[str] | None) -> str:
        # detail page only makes sense for exactly one id
        if ids and len(ids) == 1 and return_page != 'browse':
            return f'{base}/{kind}/{_seg(ids[0])}'
        return f'{base}/{kind}'

    if action_type == 'dataset_status':
        path = base
    elif action_type == 'delete_logs':
        task_id = payload.get('task_id')
        path = f'{base}/task/{_seg(task_id)}' if task_id else base
    elif action_type == 'task_status':
        path = _browse_or_detail('task', payload.get('task_ids'))
    elif action_type == 'job_status':
        path = _browse_or_detail('job', payload.get('job_ids'))
    else:
        path = base

    query = sanitize_return_query(return_query)
    return f'{path}?{query}' if query else path


def status_url(action_type: str, id_: str, return_query: str | None = None, return_page: str | None = None) -> str:
    """
    Build the action status page url, carrying the sanitized return args.

    Args:
        action_type: action service type
        id_: action message id
        return_query: user query string, sanitized here
        return_page: `browse` or `detail` hint (dropped if not allowlisted)

    Returns:
        a site-relative url
    """
    query = {}
    if q := sanitize_return_query(return_query):
        query['return_query'] = q
    if return_page in RETURN_PAGES:
        query['return_page'] = return_page
    url = f'/actions/status/{_seg(action_type)}/{_seg(id_)}'
    return f'{url}?{urlencode(query)}' if query else url


def http_error_message(e: requests.exceptions.HTTPError) -> str:
    """Extract a readable message from a REST error response."""
    try:
        return e.response.json()['error']
    except Exception:
        pass
    try:
        return e.response.text or str(e)
    except Exception:
        return str(e)


class ActionBase(PublicHandler):
    """Shared helpers for the action form handlers."""

    def _return_args(self) -> tuple[str, str | None]:
        """Read the (unsanitized) return_query and validated return_page from the form."""
        return_query = self.get_body_argument('return_query', default='') or ''
        return_page = self.get_body_argument('return_page', default=None)
        if return_page not in RETURN_PAGES:
            return_page = None
        return return_query, return_page

    def render_action_error(self, message: str, return_url: str, status_code: int = 400) -> None:
        """Render an error page with a link back to the originating page."""
        self.set_status(status_code)
        self.render(
            'action_status.html',
            status='error',
            progress=None,
            message=message,
            reload_url=None,
            return_url=return_url,
        )

    def redirect_303(self, url: str) -> None:
        """Post/Redirect/Get."""
        self.redirect(url, status=303)

    async def queue_action(self, action_type: str, args: dict[str, Any], return_url: str) -> None:
        """
        Queue an action via the action services, then 303 to the status page.

        On REST failure, renders an error page instead.
        """
        assert self.rest_client
        assert action_type in ACTION_TYPES
        try:
            ret = await self.rest_client.request('POST', f'/actions/{action_type}', args)
            id_ = ret['result']
        except requests.exceptions.HTTPError as e:
            logger.warning('failed to queue %s', action_type, exc_info=True)
            self.render_action_error(http_error_message(e), return_url)
            return

        return_query, return_page = self._return_args()
        self.redirect_303(status_url(action_type, id_, return_query, return_page))


class DatasetAction(ActionBase):
    """Handle POST /dataset/(\\w+)/actions"""

    @authenticated
    async def post(self, dataset_id: str):
        assert self.rest_client
        action = self.get_body_argument('action', default='')
        return_query, _ = self._return_args()
        return_url = build_return_url('dataset_status', {'dataset_id': dataset_id}, return_query)

        if action in STATUS_ACTIONS:
            await self.queue_action('dataset_status', {'dataset_id': dataset_id, 'action': action}, return_url)
        elif action == 'delete_logs':
            await self.queue_action('delete_logs', {'dataset_id': dataset_id}, return_url)
        elif action == 'priority':
            try:
                priority = float(self.get_body_argument('priority', default=''))
            except ValueError:
                self.render_action_error('priority is not a number', return_url)
                return
            # same rule as the REST API (`valid_priority`); checked here for a clear message.
            # float() accepts 'nan' and 'inf', which must not reach the database.
            if not math.isfinite(priority) or priority < 0:
                self.render_action_error('priority must be a finite number >= 0', return_url)
                return
            try:
                await self.rest_client.request('PUT', f'/datasets/{dataset_id}/priority', {'priority': priority})
            except requests.exceptions.HTTPError as e:
                self.render_action_error(http_error_message(e), return_url)
                return
            self.redirect_303(return_url)
        elif action == 'truncate':
            try:
                await self.rest_client.request('POST', f'/datasets/{dataset_id}/dataset_actions/truncate', {})
            except requests.exceptions.HTTPError as e:
                self.render_action_error(http_error_message(e), return_url)
                return
            self.redirect_303(return_url)
        else:
            self.render_action_error('invalid action', return_url)


class TaskAction(ActionBase):
    """Handle POST /dataset/(\\w+)/task/actions"""

    @authenticated
    async def post(self, dataset_id: str):
        action = self.get_body_argument('action', default='')
        task_ids = [t for t in self.get_body_arguments('task_ids') if t]
        return_query, return_page = self._return_args()
        payload = {'dataset_id': dataset_id, 'task_ids': task_ids}
        return_url = build_return_url('task_status', payload, return_query, return_page)

        # never send an empty id list: the service would act on the whole dataset
        if not task_ids:
            self.render_action_error('no task_ids', return_url)
        elif action in STATUS_ACTIONS:
            await self.queue_action('task_status', {**payload, 'action': action}, return_url)
        elif action == 'delete_logs':
            if len(task_ids) != 1:
                self.render_action_error('delete_logs requires exactly one task_id', return_url)
                return
            await self.queue_action('delete_logs', {'dataset_id': dataset_id, 'task_id': task_ids[0]}, return_url)
        else:
            self.render_action_error('invalid action', return_url)


class JobAction(ActionBase):
    """Handle POST /dataset/(\\w+)/job/actions"""

    @authenticated
    async def post(self, dataset_id: str):
        action = self.get_body_argument('action', default='')
        job_ids = [j for j in self.get_body_arguments('job_ids') if j]
        return_query, return_page = self._return_args()
        payload = {'dataset_id': dataset_id, 'job_ids': job_ids}
        return_url = build_return_url('job_status', payload, return_query, return_page)

        # never send an empty id list: the service would act on the whole dataset
        if not job_ids:
            self.render_action_error('no job_ids', return_url)
        elif action in STATUS_ACTIONS:
            await self.queue_action('job_status', {**payload, 'action': action}, return_url)
        else:
            self.render_action_error('invalid action', return_url)


class ActionStatus(ActionBase):
    """Handle GET /actions/status/(\\w+)/(\\w+)"""

    @authenticated
    async def get(self, action_type: str, id_: str):
        assert self.rest_client
        if action_type not in ACTION_TYPES:
            raise tornado.web.HTTPError(400, reason='invalid action type')

        return_query = self.get_query_argument('return_query', default='')
        return_page = self.get_query_argument('return_page', default=None)
        if return_page not in RETURN_PAGES:
            return_page = None

        try:
            ret = await self.rest_client.request('GET', f'/actions/{action_type}/{id_}')
        except requests.exceptions.HTTPError as e:
            logger.info('failed to get action status', exc_info=True)
            self.render_action_error(http_error_message(e), '/dataset')
            return

        status = ret.get('status', 'unknown')
        payload = ret.get('payload', {}) or {}
        return_url = build_return_url(action_type, payload, return_query, return_page)

        if status == 'complete':
            self.redirect_303(return_url)
            return
        if status == 'error':
            self.render_action_error(ret.get('error_message', '') or f'{action_type} failed', return_url, status_code=200)
            return

        # still queued/processing: reload this page (rebuilt from sanitized
        # parts rather than request.uri, since it is embedded in a script)
        self.render(
            'action_status.html',
            status=status,
            progress=payload.get('progress'),
            message='',
            reload_url=status_url(action_type, id_, return_query, return_page),
            return_url=return_url,
        )
