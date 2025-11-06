# Copyright (c) 2022, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from pathlib import Path
import getpass
from typing import Literal

import typer

init_app = typer.Typer(name="init", no_args_is_help=True)

text = {
    "atlas": {
        "url_text": (
            "Open this url and log into your UChicago ServiceX credentials or your "
            "ATLAS SSO: https://servicex.af.uchicago.edu/sign-in"
        ),
        "url": "https://servicex.af.uchicago.edu",
        "service_name": "servicex-uc-af",
    }
}


def run(source: Literal["atlas", "cms"]):
    data = text[source]
    print(data["url_text"])
    token = getpass.getpass("Paste your token here: ")

    downloads_path = Path.cwd() / "downloads"
    downloads_path.mkdir(exist_ok=True)

    with open("servicex.yaml", "w") as file:
        file.write(
            f"""
api_endpoints:
  - endpoint: {data["url"]}
    name: {data["service_name"]}
    token: {token}

cache_path: {downloads_path}
shortened_downloaded_filename: true
"""
        )


@init_app.callback()
def init():
    """
    Sub-commands for initializing ServiceX configurations.
    """
    pass


@init_app.command()
def atlas():
    """
    Initialize ATLAS ServiceX configuration.
    """
    run(source="atlas")


@init_app.command()
def cms():
    """
    Initialize CMS ServiceX configuration.
    """
    # Empty implementation for now
    pass
