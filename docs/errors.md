# Error Handling

Sometimes things go wrong in a ServiceX query, due to a badly-formed query, unavailable input files, server error, or something else. Issues can arise either before submission, during submission, after submission, or when retrieving results. 

## Errors before submission
* if the request does not parse properly, a `ValidationError` will be raised and nothing will be submitted.
* if the request includes invalid values, an appropriate exception (`ValueError`, `RuntimeError`, `TypeError`, `NameError`) will be raised and nothing will be submitted.

## Errors during or after submission
The results of a `deliver` call are returned in a dictionary that maps the names of the various samples in the request to a list of file names or URLs. Technically this result is a type called `GuardList`: it behaves like a normal list if everything was successful with that particular sample, but if there is an exception raised, attempting to access the list object will raise a `ReturnValueException` in your code. We do this so that you can still access successful results while marking requests that failed.

The validity of a `GuardList` can be tested by testing the return value of the `.valid()` method:

```
results = deliver(spec)
for name, resultlist in results:
    if resultlist.valid():
        # do something with results
    else:
        # handle failure
```

Of course in proper Python fashion you may prefer to handle the exception from accessing invalid data rather than test each result value (i.e., requesting forgiveness rather than asking permission). A realistic analysis script probably wants to terminate if there is an error condition in any transformation request.

If an error occurs after submission, `deliver()` will return a dict unless a severe error occurs in the client code (e.g. you interrupt your code with Ctrl-C or a Jupyter kernel interrupt). If `deliver()` throws an exception otherwise, it should be considered a bug in the code. Various errors are handled as follows:
* if the authentication information in your `.servicex` file is incorrect, an `AuthorizationError` will be raised for each sample.
* if a request cannot be submitted at all (for example, somehow a unparseable query is sent) then a `RuntimeError` will be raised for the corresponding sample.
* if a transformation for a specific sample is canceled or ServiceX signals a fatal error on the backend, it will raise a `ServiceXException` for that sample.
* if a transformation for a specific sample does not fully process all files, partial results will be returned. The results will *not* be cached. An error message will be printed with a link to a web page which summarizes errors on the server associated with the transformation. This can be caused by any runtime error: frequent causes are input files that are unavailable or errors in the query that can only be checked at run time (e.g. requesting branches that do not exist).
* if an error occurs during download (lack of disk space, permission errors, problems connecting to the remote storage, etc.) an appropriate exception will be raised for the corresponding sample.