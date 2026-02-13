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

import asyncio

import typer
from rich import get_console

from servicex.configuration import Configuration
from servicex.servicex_adapter import ServiceXAdapter

check_app = typer.Typer(
    name="check", invoke_without_command=True, no_args_is_help=False
)


@check_app.callback()
def check(
    _: typer.Context,
):
    console = get_console()
    config = Configuration.read()

    if not config.api_endpoints:
        console.print("[yellow]No ServiceX endpoints configured.[/yellow]")
        return

    console.print("\n[bold]Checking ServiceX endpoints...[/bold]\n")

    results = {}

    async def verify_endpoint(endpoint):
        adapter = ServiceXAdapter(url=endpoint.endpoint, refresh_token=endpoint.token)
        result = await adapter.verify_authentication()
        results[endpoint.endpoint] = result
        if result:
            console.print(f"[green]✓[/green] {endpoint.endpoint}")
        else:
            console.print(f"[red]✗[/red] {endpoint.endpoint}")
        return result

    async def verify_all_endpoints():
        tasks = [verify_endpoint(endpoint) for endpoint in config.api_endpoints]
        return await asyncio.gather(*tasks)

    asyncio.run(verify_all_endpoints())

    console.print()
