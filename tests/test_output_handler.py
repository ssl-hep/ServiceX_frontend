from pathlib import Path

from servicex import ServiceXSpec
from servicex.servicex_client import _output_handler


def test_output_directory(tmp_path):
    config = {
        "General": {
            "Codegen": "python",
            "Delivery": "LocalCache",
            "OutputDirectory": str(tmp_path)
        },
        "Sample": [
            {"Name": "sampleA", "RucioDID": "user.kchoi:sampleA", "Query": "a"},
        ],
    }
    config = ServiceXSpec(**config)

    _output_handler(config, [], [])
    assert Path(tmp_path, "servicex_fileset.yaml").exists()
