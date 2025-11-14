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
from typing import Literal, Optional
import asyncio

import typer
from rich import get_console
from rich.prompt import Prompt
from rich.panel import Panel

from servicex.servicex_adapter import ServiceXAdapter

init_app = typer.Typer(name="init", invoke_without_command=True, no_args_is_help=False)

text = {
    "atlas": {
        "url_text": (
            "Open this url and log into your UChicago ServiceX credentials or your "
            "ATLAS SSO: https://servicex.af.uchicago.edu/sign-in"
        ),
        "url": "https://servicex.af.uchicago.edu",
        "service_name": "servicex-uc-af",
    },
    "cms": {
        "url_text": (
            "Open this url and log into your UChicago ServiceX credentials: "
            "https://servicex-cms.af.uchicago.edu/sign-in"
        ),
        "url": "https://servicex-cms.af.uchicago.edu",
        "service_name": "servicex-uc-cms",
    },
}


async def verify_token(url: str, token: str) -> bool:
    """Verify the token by pinging the ServiceX server."""
    console = get_console()
    try:
        adapter = ServiceXAdapter(url=url, refresh_token=token)
        await adapter.get_servicex_info()
        return True
    except Exception as e:
        console.print(f"[red]✗ Failed to authenticate with ServiceX server:[/red] {e}")
        return False


def run(source: Literal["atlas", "cms"], custom_url: Optional[str] = None):
    console = get_console()
    data = text[source]

    # Use custom URL if provided, otherwise use the default for the source
    url = custom_url if custom_url else data["url"]
    service_name = data["service_name"] if not custom_url else "custom-servicex"

    # Show sign-in URL in a panel
    console.print()
    profile_url = f"{url}/profile"

    if custom_url:
        sign_in_url = f"{custom_url}/sign-in"
        sign_in_message = (
            f"1. Open this URL to sign in:\n"
            f"   [cyan][link={sign_in_url}]{sign_in_url}[/link][/cyan]\n\n"
            f"2. After signing in, navigate to:\n"
            f"   [cyan][link={profile_url}]{profile_url}[/link][/cyan]\n\n"
            f"3. Copy your API token and paste it below"
        )
    else:
        sign_in_url = f"{data['url']}/sign-in"
        if source == "atlas":
            sign_in_message = (
                f"1. Open this URL to sign in with your UChicago ServiceX "
                f"credentials or ATLAS SSO:\n"
                f"   [cyan][link={sign_in_url}]{sign_in_url}[/link][/cyan]\n\n"
                f"2. After signing in, navigate to:\n"
                f"   [cyan][link={profile_url}]{profile_url}[/link][/cyan]\n\n"
                f"3. Copy your API token and paste it below"
            )
        else:
            sign_in_message = (
                f"1. Open this URL to sign in with your UChicago ServiceX "
                f"credentials:\n"
                f"   [cyan][link={sign_in_url}]{sign_in_url}[/link][/cyan]\n\n"
                f"2. After signing in, navigate to:\n"
                f"   [cyan][link={profile_url}]{profile_url}[/link][/cyan]\n\n"
                f"3. Copy your API token and paste it below"
            )

    console.print(
        Panel(sign_in_message, title="[bold]Get Your Token[/bold]", border_style="blue")
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
    default_download_dir = "./download"
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
    token: {token}

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
def init(ctx: typer.Context):
    """
    Initialize ServiceX configuration. If no subcommand is provided,
    an interactive wizard will guide you through the setup.
    """
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
            "Select your experiment "
            "([cyan]A[/cyan] for ATLAS, [cyan]C[/cyan] for CMS)",
            choices=["A", "C", "a", "c"],
            default="A",
            show_choices=False,
        ).upper()

        console.print()
        if choice == "A":
            console.print("[bold]Configuring for ATLAS...[/bold]\n")
            run(source="atlas")
        elif choice == "C":
            console.print("[bold]Configuring for CMS...[/bold]\n")
            run(source="cms")
        else:
            console.print(
                "[red]✗ Invalid choice. "
                "Please enter 'A' for ATLAS or 'C' for CMS.[/red]"
            )
            raise typer.Exit(1)


@init_app.command()
def atlas(
    url: Optional[str] = typer.Option(
        None,
        "--url",
        help="Custom ServiceX URL (default: https://servicex.af.uchicago.edu)",
    )
):
    """
    Initialize ATLAS ServiceX configuration.
    """
    run(source="atlas", custom_url=url)


@init_app.command()
def cms(
    url: Optional[str] = typer.Option(
        None,
        "--url",
        help="Custom ServiceX URL (default: https://servicex-cms.af.uchicago.edu)",
    )
):
    """
    Initialize CMS ServiceX configuration.
    """
    run(source="cms", custom_url=url)
