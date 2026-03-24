# Handling Errors

:::{admonition} You Will Learn:
:class: note
- How errors are raised before a request is submitted
- How errors are reported for individual samples after submission
- How to check whether a result is valid before accessing it
:::

Errors can occur at several stages of a ServiceX request: before submission, during transformation, or when retrieving results.

## Errors Before Submission

Errors detected before submission raise an exception immediately and nothing is sent to the backend:

- If the request cannot be parsed, a `ValidationError` is raised.
- If the request contains invalid values, an appropriate exception (`ValueError`, `RuntimeError`, `TypeError`, or `NameError`) is raised.
- If the authentication information in the `.servicex` or `servicex.yaml` file is incorrect, an `AuthorizationError` is raised.

## Errors During Transformation

Sometimes errors occur during the transformation on the backend. These errors occur when the python code cannot check for them, for example, trying to open a container in an xAOD that is not there. These errors are a little tricky to find the full log. Here are the steps to find the log:

**Step 1:**

```{image} imgs/sever_side_log_1.png
:alt: Step 1 of finding the server-side transformation log
```


**Step 2:**

```{image} imgs/server_side_log_2.png
:alt: Step 2 of finding the server-side transformation log
```


**Step 3:**

```{image} imgs/server_side_log_3.png
:alt: Step 3 of finding the server-side transformation log
```

## Errors After Submission

When `deliver()` returns, each sample's result is a `GuardList`. A `GuardList` behaves like a normal list when the sample succeeded, but raises a `ReturnValueException` if accessed after a failure. This allows successful results to be accessed while failed ones are flagged.

The validity of a result can be checked before accessing it:

```python
results = deliver(spec)
for name, result in results.items():
    if result.valid():
        # process result
    else:
        # handle failure
```

Common causes of post-submission errors include canceled or failed transformations, incomplete file processing, and download failures (such as insufficient disk space or connection problems). In all cases, failed results are not cached — resubmitting will send a new transformation request.
