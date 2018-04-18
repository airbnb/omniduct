from .base import BaseRenderer
from jinja2 import Template
import markdown


class MdRepr(object):

    def __init__(self, rendered):
        self.html = markdown.markdown(rendered)

    def _repr_html_(self):
        return self.html


class MdRenderer(BaseRenderer):

    name = 'md'

    def render_template(self, template, context=None, **kwargs):
        return MdRepr(Template(template).render(**context))
