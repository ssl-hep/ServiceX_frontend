# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# the following is to propagate down to the pydantic class docstring builder
import os

os.environ["IN_SPHINX_BUILD"] = "1"

project = "ServiceX"
copyright = (
    "2026 Institute for Research and "
    "Innovation in Software for High Energy Physics (IRIS-HEP)"
)
author = "Institute for Research and Innovation in Software for High Energy Physics (IRIS-HEP)"
html_title = "ServiceX User Guide"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.doctest",
    "code_include.extension",
    "myst_parser",
    "sphinxcontrib.autodoc_pydantic",
    "sphinx_tabs.tabs",
    "sphinx_copybutton",
    "enum_tools.autoenum",
]

templates_path = ["_templates"]

html_css_files = [
    (
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css",
        {"crossorigin": "anonymous"},
    ),
    (
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css",
        {"crossorigin": "anonymous"},
    ),
    ("https://tryservicex.org/css/navbar.css", {"crossorigin": "anonymous"}),
    ("https://tryservicex.org/css/sphinx.css", {"crossorigin": "anonymous"}),
]

html_js_files = [
    (
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/js/bootstrap.bundle.min.js",
        {
            "integrity": "sha384-FKyoEForCGlyvwx9Hj09JcYn3nv7wiPVlz7YYwJrWVcXK/BmnVDxM+D2scQbITxI",
            "crossorigin": "anonymous",
        },
    ),
]

html_sidebars = {
    "**": [
        "sidebar/brand.html",
        "sidebar/navigation.html",
        "sidebar/scroll-start.html",
        "sidebar/scroll-end.html",
    ]
}


autoclass_content = "both"

autodoc_pydantic_model_show_json = False
autodoc_pydantic_field_list_validators = False
autodoc_pydantic_config_members = False
autodoc_pydantic_model_members = False
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_show_field_summary = False
autodoc_pydantic_model_undoc_members = False
autodoc_pydantic_settings_show_validator_summary = False
autodoc_pydantic_settings_show_validator_members = False
autodoc_pydantic_model_member_order = "bysource"

html_theme = "furo"
