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
