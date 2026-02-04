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
from typing import Literal, Optional, TypedDict
import asyncio

import typer
from rich import get_console
from rich.prompt import Prompt
from rich.panel import Panel

from servicex.servicex_adapter import ServiceXAdapter

init_app = typer.Typer(name="init", invoke_without_command=True, no_args_is_help=False)


class InitItem(TypedDict):
    name: str
    url: str
    service_name: str


class InitConfig(TypedDict):
    uchicago: InitItem


config: InitConfig = {
    "uchicago": {
        "name": "UChicago",
        "url": "https://servicex.af.uchicago.edu",
        "service_name": "servicex-uc-af",
    },
}


async def verify_token(url: str, token: str) -> bool:
    """Verify the token by authenticating with the ServiceX server."""
    console = get_console()
    try:
        adapter = ServiceXAdapter(url=url, refresh_token=token)
        result = await adapter.verify_authentication()
        if not result:
            console.print("[red]✗ Failed to authenticate with ServiceX server[/red]")
        return result
    except Exception as e:
        console.print(f"[red]✗ Failed to authenticate with ServiceX server:[/red] {e}")
        return False


def run(
    source: Optional[Literal["uchicago"]] = None,
    custom_url: Optional[str] = None,
    auth_disabled: bool = False,
):
    if source is None and custom_url is None:
        raise RuntimeError("An Access Facility source or specific url must be provided")

    console = get_console()
    if source is not None:
        data = config[source]
        url = data["url"]
        service_name = data["service_name"]
        name = data["name"]
    elif custom_url is not None:
        name = custom_url
        url = custom_url
        service_name = "custom-servicex"

    console.print()

    profile_url = f"{url}/profile"
    sign_in_url = f"{url}/sign-in"

    if custom_url:
        name = custom_url

    token = None
    if not auth_disabled:
        sign_in_message = (
            f"1. Open this URL to sign in to {name}:\n"
            f"   [cyan][link={sign_in_url}]{sign_in_url}[/link][/cyan]\n\n"
            f"2. After signing in, navigate to:\n"
            f"   [cyan][link={profile_url}]{profile_url}[/link][/cyan]\n\n"
            f"3. Copy your API token and paste it below"
        )

        console.print(
            Panel(
                sign_in_message,
                title="[bold]Get Your Token[/bold]",
                border_style="blue",
            )
        )

        # Get token from user
        console.print()
        token = getpass.getpass("Paste your token here: ")

        # Verify token with authenticated ping
        console.print("\n[yellow]⏳ Verifying token...[/yellow]")
        if not asyncio.run(verify_token(url, token)):
            console.print(
                "[red]✗ Failed to verify token. "
                "Please check your token and try again.[/red]"
            )
            raise typer.Exit(1)

        console.print("[green]✓ Token verified successfully![/green]")

    # Prompt for download directory
    console.print()
    default_download_dir = "./downloads"
    download_dir = Prompt.ask(
        "[bold]Select download directory[/bold]", default=default_download_dir
    )

    downloads_path = Path(download_dir).expanduser().resolve()
    downloads_path.mkdir(parents=True, exist_ok=True)

    # Write configuration file
    config_path = Path.cwd() / "servicex.yaml"
    with open(config_path, "w") as file:
        file.write(
            f"""api_endpoints:
  - endpoint: {url}
    name: {service_name}
    {"token: " + str(token) if not auth_disabled else ""}

cache_path: {downloads_path}
shortened_downloaded_filename: true
"""
        )

    # Success message
    console.print()
    success_message = (
        f"[bold green]✓ Success![/bold green]\n\n"
        f"ServiceX configuration has been created at:\n"
        f"  [cyan]{config_path}[/cyan]\n\n"
        f"Downloads will be saved to:\n"
        f"  [cyan]{downloads_path}[/cyan]"
    )
    console.print(
        Panel(
            success_message,
            border_style="green",
            title="[bold]Configuration Complete[/bold]",
        )
    )


@init_app.callback()
def init(
    ctx: typer.Context,
    custom_url: Optional[str] = typer.Option(
        None,
        "--url",
        help="Custom ServiceX URL (default: https://servicex.af.uchicago.edu)",
    ),
    auth_disabled: bool = typer.Option(
        False,
        "--auth-disabled",
        is_flag=True,
        help=(
            "Specify this if the ServiceX server is not using authentication "
            "(for example, a ServiceX instance running locally)"
        ),
    ),
):
    """
    Initialize ServiceX configuration. If no subcommand is provided,
    an interactive wizard will guide you through the setup.
    """

    if custom_url:
        run(custom_url=custom_url, auth_disabled=auth_disabled)
        return

    # Only run the wizard if no subcommand was provided
    if ctx.invoked_subcommand is None:
        console = get_console()
        console.print()
        welcome_message = (
            "[bold cyan]ServiceX Configuration Wizard[/bold cyan]\n\n"
            "This wizard will help you set up ServiceX for your experiment.\n"
            "You'll need to authenticate and configure your download settings."
        )
        console.print(Panel(welcome_message, border_style="cyan"))
        console.print()

        choice = Prompt.ask(
            "Select your experiment " "([cyan]A[/cyan] for UChicago)",
            choices=["A", "C", "a", "c"],
            default="A",
            show_choices=False,
        ).upper()

        console.print()
        key: Literal["uchicago"] | None = None
        if choice == "A":
            key = "uchicago"
        else:
            console.print("[red]✗ Invalid choice. ")
            raise typer.Exit(1)

        if key is None or key not in config:
            console.print("[red]✗ Invalid choice. ")
            raise typer.Exit(1)

        access_facility = config[key]
        console.print(f"[bold]Configuring for {access_facility['name']}...[/bold]\n")
        run(source=key)


@init_app.command()
def uchicago():
    """
    Initialize uchicago ServiceX configuration.
    """
    run(source="uchicago")
