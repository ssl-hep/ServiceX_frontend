# Main frontend interface
import pandas as pd
from typing import Union, List
import aiohttp


async def get_data(selection_query: str, datasets: Union[str, List[str]],
                   servicex_endpoint: str = 'http://localhost:5000/servicex') -> pd.DataFrame:
    '''
    Return data from a query with data sets

    Arguments:
        selection_query     `qastle` string that specifies what columnes to extract, how to format
                            them, and how to format them.
        datasets            Dataset or datasets to run the query against.
        service_endpoint    The URL where the instance of ServivceX we are querying lives

    Returns:
        df                  Pandas DataFrame that contains the resulting flat data.
    '''
    if isinstance(datasets, str):
        datasets = [datasets]
    assert len(datasets) == 1

    # Build the query, get a request ID back.
    image = "sslhep/servicex_xaod_cpp_transformer:v0.2"
    json_query = {
        "did": datasets[0],
        "selection": selection_query,
        "image": image,
        "result-destination": "object-store",
        "result-format": "root-file",
        "chunk-size": 1000,
        "workers": 5
    }

    # Start the async context manager. We should use only one for the whole app, however,
    # that just isn't going to work here. The advantage is better handling of connections.
    # TODO: Option to pass in the connectino pool?
    async with aiohttp.ClientSession() as client:
        async with client.post(f'{servicex_endpoint}/transformation', data=json_query) as response:
            # TODO: Make sure to throw the correct type of exception
            assert response.status == 200
            request_id = (await response.json())["request_id"]

        # Load all the files as they become availible.

        # return the result
        return pd.DataFrame()
