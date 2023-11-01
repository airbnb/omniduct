from .base import BaseRenderer
from jinja2 import Template


class JinjaRenderer(BaseRenderer):

    name = 'jinja'
    
    def render_template(self, template, context=None, **kwargs):
        return Template(template).render(**context)
