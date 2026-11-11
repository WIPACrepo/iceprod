Website
=======

The website is the main way to communicate with IceProd.  It handles several
different jobs:

* Human
    * dataset submission
    * dataset editing, other in progress actions
    * viewing progress
* Computer
    * running task communications
    * site-to-site communications
    * file downloads
    * file proxying

In order to handle the many hundreds or thousands of requests it could get,
the website was built on :ref:`async`.

Task Communication
------------------

Tasks communicate with the server using a json-rpc interface built into the
website.  For most communications, this involves talking with the database
using the internal RPC.

Site-to-Site Communication
--------------------------

Communication between sites also use the json-rpc interface in the website.

Human Interaction
-----------------

Pages never call the REST API from the browser.  Every change a user makes
(dataset, task, and job status changes, priority, truncate, log deletion) is
an ordinary HTML form posted back to the website.  The website handler then
calls the REST API on the server, using the user's own token, so the REST API
still applies the user's permissions.

Action endpoints (`iceprod/website/handlers/actions.py`):

=====================================  =========================================
Route                                  Purpose
=====================================  =========================================
``POST /dataset/{id}/actions``         dataset status, priority, truncate,
                                       delete all dataset logs
``POST /dataset/{id}/task/actions``    status of the selected tasks, delete one
                                       task's logs
``POST /dataset/{id}/job/actions``     status of the selected jobs
``GET /actions/status/{type}/{id}``    progress page for a queued action
=====================================  =========================================

Flow:

1. The form posts ``action``, the selected ids, and hints for where to go
   afterwards (``return_page``, ``return_query``).
2. Quick actions (priority, truncate) call the REST API directly and redirect
   back with ``303 See Other``.
3. Longer actions are queued with ``POST /actions/{type}`` on the REST API and
   processed by the action services (`iceprod/services/actions`).  The user is
   redirected (303) to the status page, which reloads every second until the
   action completes, then redirects back.  Errors are shown on the page.

The 303 redirects follow the Post/Redirect/Get pattern: reloading or going
back never resubmits the form.

Return URLs are always built on the server from the action type and ids.
``return_query`` is reduced to an allowlist of keys (``status``, ``name``), so
a form cannot redirect the user to an arbitrary address.

Nginx
-----

The website uses nginx as a front end proxy.  Nginx handles all SSL
certificate checking, static files, and file uploading before proxying the
request to Tornado.

Note that Apache can also be used separately, while Nginx config is built-in.

Security
--------

Details about user accounts are in :ref:`user_accounts`.

CSRF
""""

Cross-Site Request Forgery protection uses Tornado's ``xsrf_cookies``, which
is enabled whenever a cookie secret is configured.

* Every form includes the token from ``xsrf_form_html()`` (the
  ``ActionForm`` UI module adds it automatically).
* A POST with a missing token is rejected with 400; a wrong token with 403.

Login and Token Storage
"""""""""""""""""""""""

Login uses OpenID Connect (Keycloak).  On login, the website validates the
access token from the identity provider and stores the access and refresh
tokens in a server-side session (Redis, or in memory for testing).

The browser only receives a signed ``iceprod_username`` cookie that names the
session.  Tokens are never placed in pages, cookies, or JavaScript.

Token Refresh
"""""""""""""

Implemented in ``PublicHandler.get_current_user_async`` and
``PublicHandler.refresh_access_token``
(`iceprod/website/handlers/base.py`).

On each authenticated request:

* If the stored access token has more than ``REFRESH_MARGIN`` (60) seconds
  left, it is used as is.  No call is made to the identity provider.
* Otherwise, if there is a refresh token, the website makes one call to the
  token endpoint, validates the new access token, and stores the new tokens in
  the session.  If this fails, the user is treated as logged out: GET requests
  are redirected to the login page, and POST requests are rejected.
* Without a refresh token, an expired access token means the user is logged
  out.

The token endpoint is looked up once per process from the provider's
``.well-known/openid-configuration`` and cached.

The user's REST client is created with the current access token and with
``refresh_access_token`` as its token callback.  If the token expires during a
long request, the client refreshes it once, and the result is stored in the
session as above.

Token Verification
""""""""""""""""""

Tokens are signature-checked wherever a decision depends on them.  Two
places decode a token *without* verification, on purpose:

==================================  ================================  ==============================
Check                               Verified?                         Why
==================================  ================================  ==============================
access token at login               yes (``store_tokens``)            enters the session
access token after refresh          yes (``refresh_access_token``)    enters the session; must
                                                                      also name the logged in user
website groups (``auth_groups``)    yes                               used for page permissions
every REST API request              yes (REST server)                 enforces all permissions
refresh token at refresh            yes (identity provider)           grants new tokens
access token expiry                 no (``_access_token_valid``)      only decides whether to
                                                                      refresh early
refresh token scopes                no (``_token_scopes``)            only echoed back to the
                                                                      identity provider
==================================  ================================  ==============================

The unverified checks are safe because:

* Session tokens can only be written by the server, and every access token was
  validated when it was stored.
* Neither check grants access.  A bad token would only be sent on to the REST
  API (which rejects it) or to the identity provider (which validates the
  refresh token itself).

Validation allows 60 seconds of clock skew (the ``rest_tools`` default).

Content Security Policy
"""""""""""""""""""""""

Every page sends a ``Content-Security-Policy`` header (set in
``PublicHandler.prepare``):

* ``default-src 'none'``: anything not listed below is blocked.
* ``script-src`` / ``style-src``: this site, ``cdnjs.cloudflare.com``, and
  inline blocks carrying the per-request nonce.  Inline scripts must use the
  ``SecureScript`` UI module, which adds the nonce.
* ``connect-src 'self'``: page scripts may only make network requests
  (``fetch``, XHR, WebSocket) to the website itself.  The REST API is
  deliberately not listed, so even injected script cannot call it from the
  browser.

.. note::

   New pages must follow the form pattern in `Human Interaction`_.  A page
   that calls the REST API from JavaScript will be blocked by the browser
   (a CSP violation is logged in the browser console).

Known Limitations
"""""""""""""""""

* Concurrent refreshes: the session store keeps the last write.  If the
  identity provider rotates refresh tokens and two requests (for example, two
  tabs) refresh at the same moment, one may store a refresh token that is
  already invalid.  That user is asked to log in again once the access token
  expires.
* Token endpoint calls use the synchronous ``requests`` library and block the
  event loop.  They happen about once per access token lifetime per user.
* The cached token endpoint is only looked up again after a restart.
