# YAML Features

The YAML file format, with ServiceX extensions, provides a compact way to represent ServiceX queries. Two features in particular reduce duplication and improve clarity: definitional anchors/aliases, and file inclusion.

## Anchors and Aliases

Standard YAML provides _anchors_ and _aliases_. An _anchor_ is a string prefixed with `&` that names a particular YAML item; an _alias_ is the same string prefixed with `*` that reuses it (conceptually similar to creating and dereferencing a pointer in C/C++). Anchors must be defined before use.

An anchor can be defined once and reused as an alias many times. For example, a cut defined as `&DEF_mycut <cut definition>` can be referenced elsewhere as `*DEF_mycut`. By convention, anchors are grouped in a `Definitions` block at the start of the file to avoid clutter in the `General` and `Sample` blocks and to eliminate ordering concerns.

A configuration file using this feature:

```yaml
Definitions:
  - &DEF_query !PythonFunction |
        def run_query(input_filenames=None):
              return []

Sample:
  - Name: mysample
    Query: *DEF_query
    ...
```

## Including Other YAML Files

ServiceX extends the standard YAML format with `!include` syntax, which inserts the contents of another YAML file in place.

A main YAML file using inclusion:

```yaml
Definitions:
    !include definitions.yaml

Sample:
  - Name: mysample
    Query: *DEF_query
    ...
```

The included `definitions.yaml`:

```yaml
- &DEF_query !PythonFunction |
        def run_query(input_filenames=None):
            return []
```

Factoring definitions into a separate file and referencing them via anchors and aliases keeps the top-level file readable.

## String Handling

YAML provides several ways to embed multiline strings, which can produce unexpected results. The block scalar style — introducing a multiline string with a pipe (`|`) and using consistent indentation — is recommended. The [YAML multiline reference](https://yaml-multiline.info/) demonstrates the available modes.
