"""
Command-line interface for CHIRPS Zarr ingestion platform.

Provides CLI commands for:
- Bootstrap ingestion (initial historical backfill)
- Incremental updates (daily/monthly updates)
- Automatic mode selection (bootstrap vs incremental)
- Data querying and status commands
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.config import get_config
from src.orchestration.bootstrap_ingestion import BootstrapOrchestrator, BootstrapOrchestrationError
from src.orchestration.incremental_ingestion import IncrementalOrchestrator, IncrementalIngestionError
from src.utils.zarr_state import ZarrStateManager


def cmd_bootstrap(args):
    """Execute bootstrap ingestion command."""
    print("CHIRPS Zarr Bootstrap Ingestion")
    print("=" * 80)
    
    config = get_config()
    
    # Use provided dates or config defaults
    start_date = args.start_date if args.start_date else config.BOOTSTRAP_START_DATE
    end_date = args.end_date if args.end_date else config.BOOTSTRAP_END_DATE
    
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Zarr path: {config.ZARR_STORE_PATH}")
    print(f"Download concurrency: {config.DOWNLOAD_CONCURRENCY}")
    print("=" * 80)
    
    # Confirm before proceeding
    if not args.yes:
        response = input("\nProceed with bootstrap ingestion? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return 1
    
    try:
        orchestrator = BootstrapOrchestrator(
            config=config,
            start_date=start_date,
            end_date=end_date
        )
        
        total, successful, failed = orchestrator.run(skip_download=args.skip_download)
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total days: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Success rate: {successful/total*100:.1f}%" if total > 0 else "N/A")
        print("=" * 80)
        
        return 0 if failed == 0 else 1
        
    except BootstrapOrchestrationError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}", file=sys.stderr)
        return 1


def cmd_info(args):
    """Display information about the Zarr store."""
    config = get_config()
    zarr_path = config.ZARR_STORE_PATH
    
    print("CHIRPS Zarr Store Information")
    print("=" * 80)
    print(f"Path: {zarr_path}")
    print(f"Exists: {zarr_path.exists()}")
    
    if zarr_path.exists():
        try:
            # Use ZarrStateManager for comprehensive stats
            state_manager = ZarrStateManager(zarr_path, config)
            state_manager.print_summary()
            return 0
        except Exception as e:
            print(f"Error inspecting store: {e}")
            return 1
    
    print("=" * 80)
    return 0


def cmd_incremental(args):
    """Execute incremental ingestion command."""
    print("CHIRPS Zarr Incremental Ingestion")
    print("=" * 80)
    
    config = get_config()
    
    # Check if Zarr store exists
    state_manager = ZarrStateManager(config.ZARR_STORE_PATH, config)
    
    if not state_manager.exists():
        print("ERROR: Zarr store does not exist. Run bootstrap first.", file=sys.stderr)
        print(f"Run: python -m src.cli bootstrap", file=sys.stderr)
        return 1
    
    # Display current state
    latest_date = state_manager.get_latest_date()
    next_date = state_manager.get_next_expected_date()
    
    print(f"Current latest date: {latest_date}")
    print(f"Next expected date: {next_date}")
    print(f"Max days per run: {args.max_days}")
    
    if args.dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    
    print("=" * 80)
    
    # Confirm before proceeding (unless dry run or -y flag)
    if not args.yes and not args.dry_run:
        response = input("\nProceed with incremental ingestion? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return 1
    
    try:
        orchestrator = IncrementalOrchestrator(
            config=config,
            max_days_per_run=args.max_days
        )
        
        # Parse force date if provided
        force_date = None
        if args.force_date:
            force_date = datetime.strptime(args.force_date, '%Y-%m-%d').date()
        
        summary = orchestrator.run(
            force_date=force_date,
            dry_run=args.dry_run
        )
        
        # Print summary (already logged, but show key stats)
        print("\n" + "=" * 80)
        print("INGESTION SUMMARY")
        print("=" * 80)
        print(f"Successfully ingested: {summary['successful_ingestions']} days")
        print(f"Failed: {summary['failed_ingestions']} days")
        print(f"Skipped (duplicates): {summary['skipped_duplicates']} days")
        print(f"Duration: {summary['duration_seconds']:.2f}s")
        print(f"Zarr now contains: {summary['zarr_total_dates']} days")
        print(f"Next expected: {summary['next_expected_date']}")
        print("=" * 80)
        
        return 0 if summary['failed_ingestions'] == 0 else 1
        
    except IncrementalIngestionError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


def cmd_auto(args):
    """
    Automatically select and run the appropriate ingestion mode.
    
    Mode selection logic (per TDD Section 7.3):
    - If Zarr store does not exist → Bootstrap mode
    - If Zarr store exists → Incremental mode
    """
    print("CHIRPS Zarr Auto-Mode Ingestion")
    print("=" * 80)
    
    config = get_config()
    state_manager = ZarrStateManager(config.ZARR_STORE_PATH, config)
    
    if state_manager.exists():
        print("Mode: INCREMENTAL (Zarr store exists)")
        print("=" * 80)
        
        # Run incremental with same args
        return cmd_incremental(args)
    else:
        print("Mode: BOOTSTRAP (Zarr store does not exist)")
        print("=" * 80)
        
        # Run bootstrap with same args
        return cmd_bootstrap(args)


def cmd_status(args):
    """Display comprehensive status of the ingestion system."""
    config = get_config()
    zarr_path = config.ZARR_STORE_PATH
    
    print("CHIRPS Zarr Platform Status")
    print("=" * 80)
    
    # Zarr store status
    state_manager = ZarrStateManager(zarr_path, config)
    
    if state_manager.exists():
        stats = state_manager.get_coverage_stats()
        
        print("Zarr Store: EXISTS")
        print(f"  Path: {zarr_path}")
        print(f"  Bootstrap Complete: {stats['bootstrap_complete']}")
        print(f"  Date Range: {stats['earliest_date']} to {stats['latest_date']}")
        print(f"  Total Days: {stats['total_dates']}")
        print(f"  Coverage: {stats['coverage_percent']:.2f}%")
        print(f"  Has Gaps: {stats['has_gaps']}")
        
        if stats['missing_dates'] > 0:
            print(f"  Missing Days: {stats['missing_dates']}")
        
        if stats['latest_date']:
            from datetime import timedelta
            next_date = stats['latest_date'] + timedelta(days=1)
            print(f"  Next Expected: {next_date}")
    else:
        print("Zarr Store: DOES NOT EXIST")
        print("  Run bootstrap to initialize the store")
    
    print("\n" + "-" * 80)
    
    # Configuration
    print("Configuration:")
    print(f"  Raw data dir: {config.RAW_DIR}")
    print(f"  Zarr dir: {config.ZARR_DIR}")
    print(f"  Download concurrency: {config.DOWNLOAD_CONCURRENCY}")
    print(f"  Zarr chunking: time={config.ZARR_CHUNK_TIME}, "
          f"lat={config.ZARR_CHUNK_LAT}, lon={config.ZARR_CHUNK_LON}")
    
    print("=" * 80)
    return 0


def cmd_config(args):
    """Display current configuration."""
    config = get_config()
    
    print("CHIRPS Zarr Configuration")
    print("=" * 80)
    print(f"Base directory: {config.BASE_DIR}")
    print(f"Raw data directory: {config.RAW_DIR}")
    print(f"Interim directory: {config.INTERIM_DIR}")
    print(f"Zarr directory: {config.ZARR_DIR}")
    print(f"Zarr store path: {config.ZARR_STORE_PATH}")
    print()
    print(f"CHIRPS base URL: {config.CHIRPS_BASE_URL}")
    print(f"Download concurrency: {config.DOWNLOAD_CONCURRENCY}")
    print(f"Chunk size: {config.CHUNK_SIZE:,} bytes")
    print(f"Timeout: {config.TIMEOUT_SECONDS}s")
    print(f"Max retries: {config.MAX_RETRIES}")
    print()
    print(f"Zarr chunking: time={config.ZARR_CHUNK_TIME}, "
          f"lat={config.ZARR_CHUNK_LAT}, lon={config.ZARR_CHUNK_LON}")
    print(f"Zarr compression: {config.ZARR_COMPRESSOR}, level={config.ZARR_COMPRESSION_LEVEL}")
    print()
    print(f"Bootstrap date range: {config.BOOTSTRAP_START_DATE} to {config.BOOTSTRAP_END_DATE}")
    print(f"Metadata config: {config.METADATA_CONFIG_PATH}")
    print("=" * 80)
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CHIRPS Zarr Climate Data Platform CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-mode (automatically selects bootstrap or incremental)
  python -m src.cli auto
  
  # Bootstrap (initial historical backfill)
  python -m src.cli bootstrap --start-date 2023-01-01 --end-date 2024-12-31
  
  # Incremental update (check for new data)
  python -m src.cli incremental
  
  # Incremental with dry-run (check without downloading)
  python -m src.cli incremental --dry-run
  
  # Check system status
  python -m src.cli status
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Auto command (recommended)
    auto_parser = subparsers.add_parser(
        'auto',
        help='Automatically select bootstrap or incremental mode'
    )
    auto_parser.add_argument(
        '--max-days',
        type=int,
        default=31,
        help='Maximum days to ingest per run (default: 31)'
    )
    auto_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check availability without downloading/ingesting'
    )
    auto_parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Proceed without confirmation'
    )
    auto_parser.set_defaults(func=cmd_auto)
    
    # Bootstrap command
    bootstrap_parser = subparsers.add_parser(
        'bootstrap',
        help='Run bootstrap ingestion (initial historical backfill)'
    )
    bootstrap_parser.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Override bootstrap start date (YYYY-MM-DD)'
    )
    bootstrap_parser.add_argument(
        '--end-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Override bootstrap end date (YYYY-MM-DD)'
    )
    bootstrap_parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download phase (assume files already exist)'
    )
    bootstrap_parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Proceed without confirmation'
    )
    bootstrap_parser.set_defaults(func=cmd_bootstrap)
    
    # Incremental command
    incremental_parser = subparsers.add_parser(
        'incremental',
        help='Run incremental ingestion (update with new data)'
    )
    incremental_parser.add_argument(
        '--max-days',
        type=int,
        default=31,
        help='Maximum consecutive days to ingest (default: 31)'
    )
    incremental_parser.add_argument(
        '--force-date',
        type=str,
        help='Force start from specific date (YYYY-MM-DD), overrides automatic detection'
    )
    incremental_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check availability without downloading/ingesting'
    )
    incremental_parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Proceed without confirmation'
    )
    incremental_parser.set_defaults(func=cmd_incremental)
    
    # Status command
    status_parser = subparsers.add_parser(
        'status',
        help='Display comprehensive system and Zarr store status'
    )
    status_parser.set_defaults(func=cmd_status)
    
    # Info command
    info_parser = subparsers.add_parser(
        'info',
        help='Display detailed information about the Zarr store'
    )
    info_parser.set_defaults(func=cmd_info)
    
    # Config command
    config_parser = subparsers.add_parser(
        'config',
        help='Display current configuration'
    )
    config_parser.set_defaults(func=cmd_config)
    
    # Parse and execute
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
