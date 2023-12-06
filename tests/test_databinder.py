# import pytest

from servicex.databinder.databinder_configuration import load_databinder_config
from servicex.databinder.databinder_requests import DataBinderRequests
# from servicex.databinder.databinder_outputs import OutputHandler
# from servicex.databinder.databinder_deliver import DataBinderDeliver


def test_load_config():
    config = {
        "General":
        {
            "ServiceXName": "servicex",
            "Codegen": "python",
            "IgnoreServiceXCache": True
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Function": "DEF_a"
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Columns": "el_pt",
                "Codegen": "uproot"
            }
        ],
        "Definition":
        {
            "DEF_a": "a"
        }
    }
    assert type(load_databinder_config(config)) == dict


def test_requests_python_transformer():
    config = {
        "General":
        {
            "ServiceX": "testing4",
            "OutputFormat": "root",
            "Delivery": "objectstore"
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Codegen": "python",
                "Function": "DEF_a",
                "NFiles": "5",
                "IgnoreLocalCache": "False"
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Function": "DEF_a",
                "Codegen": "python",
                "IgnoreLocalCache": "False"
            }
        ]
    }
    reqs = DataBinderRequests(config).get_requests()
    assert reqs[1]["sample_name"] == "sampleB"
    assert len(reqs) == 2
    assert len(reqs[0].keys()) == 3


# @pytest.mark.asyncio
# async def test_deliver():
#     config = {
#         "General":
#         {
#             "ServiceX": "testing4",
#             "OutputFormat": "root",
#             "Delivery": "objectstore"
#         },
#         "Sample":
#         [
#             {
#                 "Name": "sampleA",
#                 "RucioDID": "user.kchoi:sampleA",
#                 "Codegen": "python",
#                 "Function": "DEF_a",
#                 "NFiles": "5",
#                 "IgnoreLocalCache": "False"
#             },
#             {
#                 "Name": "sampleB",
#                 "XRootDFiles": "root://a.root",
#                 "Function": "DEF_a",
#                 "Codegen": "python",
#                 "IgnoreLocalCache": "False"
#             }
#         ]
#     }
#     deliv = DataBinderDeliver(config)
#     o = await deliv.get_data()
