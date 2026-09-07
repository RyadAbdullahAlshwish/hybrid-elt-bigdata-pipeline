#!/usr/bin/env python3
"""
================================================================================
                    BIG DATA ELT PIPELINE - CONTROL CLI
================================================================================
Author     : Data Engineering & MLOps Team
Version    : 2.5.0 Enterprise Edition
Description: Production-ready Interactive CLI Orchestrator for Distributed ELT,
             PySpark cluster jobs, MongoDB management, and Data Quality testing.
================================================================================
"""

from __future__ import annotations

import atexit
import datetime
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Third-party styling & rich TUI
try:
    import pyfiglet
    from rich import box
    from rich.align import Align
    from rich.columns import Columns
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.prompt import Confirm, Prompt
    from rich.rule import Rule
    from rich.status import Status
    from rich.style import Style
    from rich.table import Table
    from rich.text import Text
    from rich.theme import Theme
except ImportError:
    print(
        "\033[91m[Error] Required libraries missing.\033[0m\n"
        "Please run: pip install rich pyfiglet"
    )
    sys.exit(1)

# Import actual configurations from the project
try:
    from config.settings import (
        MONGO_URI, MONGO_DATABASE, SPARK_MASTER,
        SPARK_EXECUTOR_MEMORY, SMALL_FILE_THRESHOLD_MB, BATCH_SIZE,
        SPARK_APP_NAME, SPARK_DRIVER_MEMORY
    )
except ImportError:
    # Fallback if config is missing or broken
    MONGO_URI = "mongodb://localhost:27017"
    MONGO_DATABASE = "orders_sample_100k"
    SPARK_MASTER = "local[*]"
    SPARK_EXECUTOR_MEMORY = "4g"
    SMALL_FILE_THRESHOLD_MB = 200
    BATCH_SIZE = 1000
    SPARK_APP_NAME = "OrdersPipeline"
    SPARK_DRIVER_MEMORY = "4g"

# -----------------------------------------------------------------------------
# Configuration & Theme Settings
# -----------------------------------------------------------------------------

CUSTOM_THEME = Theme({
    "banner": "bold cyan",
    "accent": "bold magenta",
    "highlight": "bold yellow",
    "success": "bold green",
    "warning": "bold yellow",
    "danger": "bold red",
    "muted": "dim white",
    "info": "bold deep_sky_blue1",
    "key": "bold cyan",
    "title": "bold white on navy_blue",
})

console = Console(theme=CUSTOM_THEME)


@dataclass
class SystemState:
    """Stores runtime configuration, stats, and environment health."""
    start_time: float = field(default_factory=time.time)
    jobs_executed: int = 0
    failed_jobs: int = 0
    last_command: Optional[str] = None
    last_status: str = "IDLE"
    mongo_uri: str = MONGO_URI
    mongo_database: str = MONGO_DATABASE
    spark_master: str = SPARK_MASTER
    spark_mem: str = SPARK_EXECUTOR_MEMORY
    spark_driver: str = SPARK_DRIVER_MEMORY
    spark_app: str = SPARK_APP_NAME
    threshold_mb: int = SMALL_FILE_THRESHOLD_MB
    batch_size: int = BATCH_SIZE
    default_sample_path: Path = Path("data/samples/orders_sample_100k.csv")
    default_huge_path: Path = Path("data/raw/orders_huge_mixed_quality.csv")


state = SystemState()

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

def clear_screen() -> None:
    """Clear terminal screen in a cross-platform manner."""
    os.system("cls" if os.name == "nt" else "clear")


def resize_console() -> None:
    """Resize the terminal window to perfectly fit the UI without full-screen."""
    if os.name == "nt":
        # عرض 135 عمود و 45 سطر يضمن ظهور جميع عناصر الواجهة بشكل سليم وبدون تداخل
        os.system("mode con cols=135 lines=45")


def get_terminal_width(fallback: int = 80) -> int:
    """Get current terminal width safely."""
    return shutil.get_terminal_size((fallback, 24)).columns


def render_banner() -> None:
    """Renders high-impact ASCII banner matching the exact reference image layout."""
    clear_screen()

    # العنوان الرئيسي الكبير والمفرغ بخط big
    try:
        raw_art = pyfiglet.figlet_format("BIG DATA  ELT", font="big").strip("\n")
    except Exception:
        raw_art = "=== BIG DATA ELT ==="

    ascii_text = Text(raw_art, style="cyan", no_wrap=True)
    centered_banner = Align.center(ascii_text)

    # 1. PySpark Panel
    spark_table = Table.grid(padding=(0, 1), expand=True)
    spark_table.add_column(justify="right", style="cyan", width=8)
    spark_table.add_column(justify="left", style="white")
    spark_table.add_row("Master:", f" {state.spark_master}")
    spark_table.add_row("Driver:", f" {state.spark_driver}")

    spark_panel = Panel(
        spark_table,
        title="PySpark",
        title_align="center",
        border_style="cyan",
        box=box.SQUARE,
        width=25,
        padding=(1, 1),
    )

    # 2. Middle Banner (H-ELT بخط مائل slant)
    try:
        middle_raw = pyfiglet.figlet_format("HYBRID ELT", font="standard").strip("\n")
    except Exception:
        middle_raw = "/// H - ELT ///"

    middle_text = Align.center(
        Text(middle_raw, justify="center", style="bold yellow", no_wrap=True),
        vertical="middle",
    )

    # 3. Python Core Panel
    python_table = Table.grid(padding=(0, 1), expand=True)
    python_table.add_column(justify="right", style="magenta", width=11)
    python_table.add_column(justify="left", style="white")
    python_table.add_row("Threshold:", f" {state.threshold_mb}MB")
    python_table.add_row("BatchSize:", f" {state.batch_size}")

    python_panel = Panel(
        python_table,
        title="Python Core",
        title_align="center",
        border_style="magenta",
        box=box.SQUARE,
        width=25,
        padding=(1, 1),
    )

    badges = Columns([spark_panel, middle_text, python_panel], align="center", expand=True)

    # شريط الحالة السفلي المطابق للصورة
    overview_table = Table.grid(expand=True)
    overview_table.add_column(ratio=1, justify="left")
    overview_table.add_column(ratio=1, justify="center")
    overview_table.add_column(ratio=1, justify="right")
    overview_table.add_row(
        f"  [cyan]DB:[/cyan] [white]{state.mongo_database}[/white]",
        f"[cyan]Jobs Run:[/cyan] [green]{state.jobs_executed}[/green]",
        f"[cyan]Failed:[/cyan] [red]{state.failed_jobs}[/red]  ",
    )

    main_panel = Panel(
        Group(
            centered_banner,
            Rule(style="cyan"),
            badges,
            Rule(style="cyan"),
            overview_table,
        ),
        title="BIG DATA PIPELINE PROJECT",
        title_align="center",
        border_style="cyan",
        box=box.SQUARE,
        padding=(1, 2),
        expand=True
    )

    console.print()
    console.print(main_panel)
    console.print()


def execute_job(
    command: str,
    description: str,
    wait_on_finish: bool = True,
    critical: bool = False
) -> bool:
    """
    Executes a shell or python command with real-time execution timer,
    visual indicators, and audit logging.
    """
    state.last_command = command
    state.last_status = "RUNNING"

    info_grid = Table.grid(padding=(0, 1), expand=True)
    info_grid.add_column(style="dim white", width=11)
    info_grid.add_column(style="bold white")
    info_grid.add_row("Task:", f"[bold cyan]{description.upper()}[/bold cyan]")
    info_grid.add_row("Command:", f"[info]{command}[/info]")
    info_grid.add_row("Started:", f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    task_panel = Panel(
        info_grid,
        title="TASK EXECUTION",
        title_align="left",
        border_style="yellow",
        box=box.SQUARE,
        padding=(0, 1),
    )

    console.print()
    console.print(task_panel)
    console.print()

    start_t = time.perf_counter()
    success = False

    try:
        process = subprocess.run(
            command,
            shell=True,
            check=False,
            stdout=None,
            stderr=None,
        )
        duration = time.perf_counter() - start_t

        if process.returncode == 0:
            success = True
            state.jobs_executed += 1
            state.last_status = "SUCCESS"
            console.print()
            console.print(
                Panel(
                    f"[bold green][SUCCESS][/bold green] Task [cyan bold]{description}[/cyan bold] finished cleanly in [bold yellow]{duration:.2f}s[/bold yellow].",
                    border_style="green",
                    box=box.SQUARE,
                    padding=(0, 2),
                )
            )
        else:
            state.failed_jobs += 1
            state.last_status = "FAILED"
            console.print()
            console.print(
                Panel(
                    f"[bold red][FAILED] (Exit Code {process.returncode})[/bold red]\n"
                    f"Task: [white bold]{description}[/white bold]\n"
                    f"Review the output above for error details.",
                    border_style="red",
                    box=box.SQUARE,
                    padding=(1, 2),
                )
            )
    except KeyboardInterrupt:
        console.print("\n[danger]Execution interrupted by operator (SIGINT).[/danger]")
        state.failed_jobs += 1
        state.last_status = "CANCELLED"
    except Exception as exc:
        state.failed_jobs += 1
        state.last_status = "ERROR"
        console.print(f"\n[danger]Unexpected Error: {exc}[/danger]")

    if wait_on_finish:
        console.print()
        Prompt.ask("[dim]Press [bold white]Enter[/bold white] to return to menu...[/dim]", default="")

    return success


def build_menu_table(items: List[Tuple[str, str, str]]) -> Table:
    """Builds a formatted Rich table for interactive options matching the image style."""
    tbl = Table(
        box=box.SIMPLE_HEAD,
        pad_edge=False,
        show_header=True,
        expand=True,
        border_style="green"
    )
    tbl.add_column("Key", style="bold cyan", justify="center", width=8)
    tbl.add_column("Operation", style="white", width=34)
    tbl.add_column("Description", style="dim white", justify="left")

    for key, title, desc in items:
        if key.startswith("sep"):
            tbl.add_row("", f"[dim cyan]{title}[/dim cyan]", "")
        else:
            tbl.add_row(f"[ {key} ]", title, desc)
    return tbl


# -----------------------------------------------------------------------------
# Main Application Loop
# -----------------------------------------------------------------------------

def main_menu() -> None:
    """Primary application orchestrator navigation (Unified View)."""
    while True:
        render_banner()

        options = [
            ("1", "Run Custom/Sample Pipeline", "Process test data sample (default 100k records)"),
            ("2", "Distributed HUGE Pipeline", "Launch PySpark job to process massive files"),
            ("3", "Generate Standard Sample", "Extract random stratified sample (100k rows)"),
            ("4", "Custom Sample Extract", "Specify row count and save path"),
            ("5", "Drop Database", "Delete MongoDB database completely to reset environment"),
            ("6", "Check Collection Stats", "Print collection names and document counts"),
            ("7", "Run Pytest Suite", "Run pytest checks for data integrity and transformations"),
            ("8", "Environment Diagnostics", "Check PySpark, Mongo, and library dependencies"),
            ("0", "Exit", "Close application"),
        ]

        table = build_menu_table(options)
        console.print(
            Panel(
                table,
                title="MAIN PROJECT MENU",
                title_align="center",
                border_style="green",
                box=box.SQUARE,
                padding=(0, 1),
                expand=True
            )
        )

        choice = Prompt.ask(
            "[bold green]Select action[/bold green]",
            choices=[opt[0] for opt in options if not opt[0].startswith("sep")],
            default="1",
        )

        if choice == "1":
            default_path = str(state.default_sample_path)
            file_path = Prompt.ask(
                " CSV File Path",
                default=default_path,
            )
            target = Path(file_path)
            if not target.exists():
                console.print(f"[warning] Warning: File '{target}' does not exist.[/warning]")
                if not Confirm.ask("Do you want to continue anyway?", default=False):
                    continue
            execute_job(
                command=f'python src/main.py --file-path "{file_path}"',
                description=f"Process Sample ({target.name})",
            )

        elif choice == "2":
            console.print()
            console.print(
                Panel(
                    "[bold yellow]SYSTEM REQUIREMENTS (Huge Data Mode)[/bold yellow]\n\n"
                    "PySpark is launched with distributed memory to process massive files.\n"
                    f"Target file: [bold cyan]{state.default_huge_path}[/bold cyan]\n"
                    "  * [bold white]Minimum RAM:[/bold white] 8 GB\n"
                    "  * [bold white]Minimum Disk Space:[/bold white] 15 GB",
                    border_style="yellow",
                    box=box.SQUARE,
                    padding=(1, 2),
                )
            )
            if Confirm.ask("Are you sure you want to start distributed processing?", default=False):
                execute_job(
                    command=f'python src/main.py --file-path "{state.default_huge_path}"',
                    description="Huge Data Processing (PySpark)",
                )

        elif choice == "3":
            execute_job(
                command="python src/create_small_sample.py",
                description="Standard Sample Extract (100k rows)",
            )

        elif choice == "4":
            rows = Prompt.ask("Enter required row count", default="50000")
            out_file = Prompt.ask("Enter save path and filename", default="data/samples/custom_sample.csv")
            cmd = f'python src/create_small_sample.py --rows {rows} --output "{out_file}"'
            execute_job(command=cmd, description=f"Custom Sample Extract ({rows} rows)")

        elif choice == "5":
            console.print()
            console.print(
                Panel(
                    f"[bold red]WARNING: IRREVERSIBLE DESTRUCTIVE ACTION[/bold red]\n\n"
                    f"Database: [bold yellow]{state.mongo_database}[/bold yellow] on [bold cyan]{state.mongo_uri}[/bold cyan]\n"
                    f"All collections, indexes, and data will be completely deleted!",
                    border_style="red",
                    box=box.SQUARE,
                    padding=(1, 2),
                )
            )
            typed_confirm = Prompt.ask("Type '[bold red]DROP[/bold red]' to confirm deletion")
            if typed_confirm.strip() == "DROP":
                drop_script = (
                    "from config.settings import MONGO_URI, MONGO_DATABASE; "
                    "from pymongo import MongoClient; "
                    "MongoClient(MONGO_URI).drop_database(MONGO_DATABASE); "
                    "print(f'[OK] Successfully purged database: {MONGO_DATABASE}')"
                )
                cmd = f'python -c "{drop_script}"'
                execute_job(command=cmd, description=f"Drop Database: {state.mongo_database}")
            else:
                console.print("[green]Aborted. No changes were made to the database.[/green]")
                time.sleep(1.2)

        elif choice == "6":
            stats_script = (
                "from config.settings import MONGO_URI, MONGO_DATABASE; "
                "from pymongo import MongoClient; "
                "db = MongoClient(MONGO_URI)[MONGO_DATABASE]; "
                "cols = db.list_collection_names(); "
                "print(f'Collections in [{MONGO_DATABASE}]:', cols); "
                "[print(f' - {c}: {db[c].estimated_document_count()} documents') for c in cols]"
            )
            execute_job(command=f'python -c "{stats_script}"', description="MongoDB Storage Statistics")

        elif choice == "7":
            execute_job(command="pytest tests/ -v", description="Run Automated Quality Tests (Pytest)")

        elif choice == "8":
            console.print()
            with console.status("[bold cyan]Checking environment and required packages...[/bold cyan]"):
                time.sleep(0.6)

            diag_table = Table(
                title="System & Environment Readiness Check",
                border_style="cyan",
                box=box.SQUARE,
                expand=True
            )
            diag_table.add_column("Component", style="white bold", justify="left")
            diag_table.add_column("Required Version", style="muted", justify="center")
            diag_table.add_column("Installed", style="info", justify="left")
            diag_table.add_column("Status", justify="center")

            def check_pkg(pkg_name: str) -> Tuple[str, str]:
                try:
                    mod = __import__(pkg_name)
                    ver = getattr(mod, "__version__", "Found")
                    return ver, "[green bold][ OK ][/green bold]"
                except ImportError:
                    return "Not Installed", "[red bold][ Missing ][/red bold]"

            packages = [
                ("pyspark", ">= 3.4.0"),
                ("pymongo", ">= 4.0.0"),
                ("pytest", ">= 7.0.0"),
                ("rich", ">= 13.0.0"),
            ]

            for pkg, req in packages:
                ver, health = check_pkg(pkg)
                diag_table.add_row(pkg, req, ver, health)

            console.print(diag_table)
            console.print()
            Prompt.ask("[dim]Press Enter to return...[/dim]", default="")

        elif choice == "0":
            terminate_cli()


def terminate_cli() -> None:
    """Graceful exit with summary statistics."""
    clear_screen()
    uptime = time.time() - state.start_time
    minutes, seconds = divmod(uptime, 60)

    summary_panel = Panel(
        Align.center(
            Group(
                Text("Thank you for using Big Data ELT Control Center", style="bold cyan"),
                Text(
                    f"Session Uptime: {int(minutes)}m {int(seconds)}s | "
                    f"Completed Jobs: {state.jobs_executed} | "
                    f"Failed: {state.failed_jobs}",
                    style="dim white",
                ),
            )
        ),
        border_style="cyan",
        box=box.SQUARE,
        padding=(1, 2),
    )
    console.print(summary_panel)
    sys.exit(0)


def sigint_handler(sig, frame) -> None:
    """Handle Ctrl+C gracefully without noisy stack trace."""
    console.print("\n\n[bold red][ABORT] Execution aborted by operator (Ctrl+C). Exiting safely...[/bold red]")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    try:
        resize_console()
        main_menu()
    except Exception as e:
        console.print_exception()
        sys.exit(1)