"""
Command-line interface for CHIRPS Zarr ingestion platform.

Provides CLI commands for:
- Bootstrap ingestion
- Incremental updates (future)
- Data querying (future)
- Status/info commands
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.config import get_config
from src.orchestration.bootstrap_ingestion import BootstrapOrchestrator, BootstrapOrchestrationError


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
            from src.convert.tiff_to_zarr import TIFFToZarrConverter
            
            converter = TIFFToZarrConverter(config)
            info = converter.get_zarr_info(zarr_path)
            
            if "error" in info:
                print(f"Error reading store: {info['error']}")
            else:
                print(f"Dimensions: {info.get('dimensions', {})}")
                print(f"Variables: {info.get('variables', [])}")
                print(f"Time coverage: {info.get('time_coverage_start')} to {info.get('time_coverage_end')}")
                print(f"Bootstrap complete: {info.get('bootstrap_complete', False)}")
        except Exception as e:
            print(f"Error inspecting store: {e}")
    
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
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
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
    
    # Info command
    info_parser = subparsers.add_parser(
        'info',
        help='Display information about the Zarr store'
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
