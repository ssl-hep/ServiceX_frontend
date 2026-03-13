# YAML Features

The YAML file format (with our extensions) provides a powerful and compact way of representing ServiceX queries. Two features in particular can be leveraged to make definitions clear and avoid duplication: the use of definitional anchors/aliases, and the inclusion of other YAML files.

## Using anchors and aliases
The standard YAML language has the concepts of _anchors_ and _aliases_. An _anchor_ is a string prefixed by "&" which then refers to the contents of that particular YAML item, and which can be reused later by using an _alias_ which is the same string prefixed with "*" (the syntax is conceptually similar to that of making a pointer and dereferencing it in C/C++). The anchor must be defined before being used as an alias.

One can define an anchor once and reuse it as an alias many times. So, for example, a cut may be defined as `&DEF_mycut <cut definition>` and then referred to in queries as `*DEF_mycut`. By convention (although not strictly required) the anchors are grouped in a `Definitions` block at the start of the YAML file; this avoids clutter in the `General` and `Sample` blocks and allows reordering of those blocks without having to worry about the ordering of anchors and aliases.

A configuration file using this feature would look like

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

## Including other YAML files
We have extended the standard YAML file format to allow the inclusion of the contents of another YAML file, with the `!include` syntax.

For example, we can have a main YAML file like this:
```yaml
Definitions:
    !include definitions.yaml

Sample:
  - Name: mysample
    Query: *DEF_query
    ...
```
which includes a `definitions.yaml` file that looks like this:
```yaml
- &DEF_query !PythonFunction |
        def run_query(input_filenames=None):
            return []
```
By factoring the files like this and using anchors and aliases, the top-level file can be kept readable.

## A note on string handling
YAML tries to provide ways to "naturally" embed multiline strings in the configuration files. This can sometimes lead to somewhat unexpected results. We recommend the "block scalar" style, introducing a multiline string by starting with a pipe (|) and using a constant indentation for each line of the string that follows. You might find [this site](https://yaml-multiline.info/) useful to demonstrate the various potential modes.
