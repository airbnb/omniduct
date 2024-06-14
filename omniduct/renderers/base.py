from abc import abstractmethod
from omniduct.utils.magics import MagicsProvider, process_line_arguments, process_line_cell_arguments


class BaseRenderer(MagicsProvider):

    @abstractmethod
    def render_template(self, statement):
        pass

    def _register_magics(self, base_name):
        """
        The following magic functions will be registered (assuming that
        the base name is chosen to be 'hive'):
        - Cell Magics:
            - `%%html`: For querying the database.
        - Line Magics:
            - `%hive`: For querying the database using a named template.

        Documentation for these magics is provided online.
        """
        from IPython.core.magic import register_cell_magic

        def template_render_magic(template, context=None, **kwargs):

            ip = get_ipython()

            if context is None:
                context = ip.user_ns

            return self.render_template(template, context)


        @register_cell_magic(base_name)
        @process_line_cell_arguments
        def render_magic(*args, **kwargs):
            return template_render_magic(*args, **kwargs)
