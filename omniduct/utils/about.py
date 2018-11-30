import base64
import textwrap

import jinja2
import six.moves.urllib as urllib

import omniduct


ABOUT_TEMPLATE_HTML = """
{%- if logo %}
<div><img src="{{logo}}" style="height: 200px;"/></div>
{%- endif %}
<b>{{name}}</b> v{{version}}<br/>

{%- if maintainers %}
<b>Maintainers:</b> {% for name, email in maintainers.items() %}{% if loop.index0 > 0 %}, {% endif %}<a href="mailto:{{email}}">{{name}}</a>{% endfor %}<br/>
{%- endif %}
{%- for name, value in attributes.items() %}
<b>{{name}}:</b> {% if value.startswith('http://') or value.startswith('https://') %}<a href='{{value}}'>{{value}}</a>{% else %}{{value}}{%endif%}<br />
{%- endfor %}
<b>Description:</b><br/>
{{description.strip()|replace('\n\n','<br/><br/>\n')}}
{%- if endorsements %}<br />
<br />
<b>Built upon:</b>
{%- for endorsement in endorsements %}
<div>
{%- if endorsement['logo'] -%}
<img src="{{endorsement['logo']}}" style="display: inline-block; height: 2em; width: 2em; object-size: contain; vertical-align: middle; margin-right: 0.5em;"/>
{%- else -%}
<span style='display: inline-block; width: 2em; height: 2em; vertical-align: middle; margin-right: 0.5em;'></span>
{%- endif -%}
{{endorsement['name']}} v{{endorsement['version'].strip()}}</div>
{%- endfor %}
{%- endif %}
"""

ABOUT_TEMPLATE_TEXT = """
{{name}}{% if version %} v{{version}}{% endif %}

{%- if maintainers %}
- Maintainers: {% for name, email in maintainers.items() %}{% if loop.index0 > 0 %}, {% endif %}{{name}} <{{email}}>{% endfor %}
{%- endif %}
{%- for name, value in attributes.items() %}
- {{name}}: {{value}}
{%- endfor %}

{{description}}
{% if endorsements %}
Built upon:
{%- for endorsement in endorsements %}
- {{endorsement['name']}} v{{endorsement['version'].strip()}}
{%- endfor %}
{%- endif %}
""".strip()


def show_about(name, version=None, logo=None, maintainers=None, attributes=None,
               description=None, endorsements=None, endorse_omniduct=True):
    """
    Output information about a project in HTML for notebooks and text otherwise.

    Args:
        name (str): The name of the project.
        version (str, None): The version of the project.
        logo (str, None): A local or remote uri for the project logo.
        maintainers (dict, None): A dictionary mapping name to email address for
            maintainers. If order is important pass an `OrderedDict`.
        attributes (dict, None): A dictionary mapping of project attributes to
            values. If order is important pass an `OrderedDict`.
        description (str, None): A description of the project.
        endorsements (list<dict>): A list of dependencies to highlight encoded
            as dictionaries of form: {'name': ..., 'version': ..., 'logo': ...}.
            Only `name` is required, and endorsements will be sorted by name.
        endorse_omniduct (bool): Whether to include omniduct in the list of
            endorsements (default: True).
    """

    endorsements = endorsements or []
    if endorse_omniduct:
        endorsements.append({
            'name': 'Omniduct',
            'version': omniduct.__version__,
            'logo': omniduct.__logo__
        })
    for endorsement in endorsements:
        endorsement['logo'] = get_image_url(endorsement.get('logo'))
    endorsements = sorted(endorsements, key=lambda x: x['name'])

    context = {
        'name': name,
        'version': version,
        'logo': get_image_url(logo),
        'maintainers': maintainers or {},
        'attributes': attributes or {},
        'description': textwrap.dedent(description).strip() if description else None,
        'endorsements': endorsements
    }

    try:
        from IPython import get_ipython
        from IPython.display import display, HTML
        ip = get_ipython()
        if ip is not None and ip.has_trait('kernel'):
            return display(HTML(jinja2.Template(ABOUT_TEMPLATE_HTML).render(**context)))
    except:
        pass

    # Textual fallback if HTML not running in a notebook
    print(textwrap.dedent(jinja2.Template(ABOUT_TEMPLATE_TEXT).render(**context)))


def get_image_url(uri):
    """
    Get a base64 URI if uri is a local path or pass through value otherwise.

    This is used to allow rendering of images in notebooks for local files,
    such as project logos.

    Args:
        uri (str, None): The local path of a logo or remote image.

    Returns:
        str: The uri of the image suitable for rendering in a notebook.
    """
    if not uri:
        return
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme in ('', 'file'):
        with open(parsed.path, 'rb') as image:
            return "data:image/png;base64,{}".format(base64.b64encode(image.read()).decode())
    return uri
