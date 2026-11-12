from typing import TYPE_CHECKING

import tornado

if TYPE_CHECKING:
    # Define what your RequestHandler looks like
    class NonceHandler(tornado.web.RequestHandler):
        csp_nonce: str


class SecureScript(tornado.web.UIModule):
    """Renders a script tag with the current request's CSP nonce."""

    if TYPE_CHECKING:
        handler: NonceHandler

    def render(self, src: str | None = None, content: str | None = None) -> bytes:
        # Access the nonce stored in the handler
        nonce = self.handler.csp_nonce
        return self.render_string(
            "modules/secure_script.html", 
            nonce=nonce, src=src, content=content
        )

class SecureStyle(tornado.web.UIModule):
    """Renders a style tag with the current request's CSP nonce."""

    if TYPE_CHECKING:
        handler: NonceHandler

    def render(self, content: str | None = None) -> bytes:
        nonce = self.handler.csp_nonce
        return self.render_string(
            "modules/secure_style.html", 
            nonce=nonce, content=content
        )


class ActionForm(tornado.web.UIModule):
    """
    Renders a single-button POST form for a website action.

    Replaces JS buttons that called the REST API directly.  The form carries
    the XSRF token, the action name, any ids, and the current page's query
    string (`return_query`) so the user returns to the same filtered view.
    Rendered from its own template, so it is always auto-escaped even when
    the including page uses `{% autoescape None %}`.

    Args:
        url: form action url (one of the `/dataset/<id>/.../actions` routes)
        action: action name
        label: button text
        ids: ids to send as repeated hidden `id_field` inputs
        id_field: form field name for ids (`task_ids` | `job_ids`)
        confirm_msg: if set, ask the user to confirm before submitting
        return_page: `browse` | `detail` hint for the completion redirect
        css_class: optional button class
    """

    def render(  # type: ignore[override]
        self,
        url: str,
        action: str,
        label: str,
        ids: list[str] | None = None,
        id_field: str | None = None,
        confirm_msg: str | None = None,
        return_page: str | None = None,
        css_class: str | None = None,
    ) -> bytes:
        return self.render_string(
            "modules/action_form.html",
            url=url, action=action, label=label,
            ids=ids or [], id_field=id_field,
            confirm_msg=confirm_msg, return_page=return_page,
            css_class=css_class,
        )
