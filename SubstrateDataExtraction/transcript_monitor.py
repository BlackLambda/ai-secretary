#!/usr/bin/env python3
"""
Transcript Monitor - Continuous Background Service
==================================================

Monitors for new meeting transcripts and saves them as individual text files.
Runs continuously with 5-minute intervals.

Usage:
    python transcript_monitor.py

    Press Ctrl+C to stop the monitor gracefully.

Features:
- Checks every 5 minutes for new transcripts
- Saves to output/meetings/ directory
- Logs to logs/transcript_monitor.log
- Tracks processed transcripts
- Auto-recovers from errors
"""

import sys
import os
import time
import logging
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.transcript_monitor import TranscriptMonitor


# ============================================================================
# CONFIGURATION
# ============================================================================

# Check interval in seconds (5 minutes = 300 seconds)
CHECK_INTERVAL = 300

# Log file
LOG_FILE = "logs/transcript_monitor.log"


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure logging to both file and console."""
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ============================================================================
# MAIN MONITOR LOOP
# ============================================================================

def main():
    """Main monitoring loop."""
    setup_logging()
    
    logging.info("=" * 80)
    logging.info("Transcript Monitor Started")
    logging.info("=" * 80)
    logging.info(f"Check interval: {CHECK_INTERVAL} seconds ({CHECK_INTERVAL // 60} minutes)")
    logging.info(f"Output directory: output/meetings/")
    logging.info(f"Log file: {LOG_FILE}")
    logging.info("Press Ctrl+C to stop")
    logging.info("=" * 80)
    
    # Initialize monitor
    try:
        monitor = TranscriptMonitor()
        logging.info("Monitor initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize monitor: {e}")
        logging.error("Exiting...")
        return
    
    # Main loop
    iteration = 0
    
    while True:
        try:
            iteration += 1
            logging.info(f"\n--- Check #{iteration} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            # Check and process new transcripts
            result = monitor.check_and_process_new_transcripts()
            
            # Log results
            if result['status'] == 'success':
                logging.info(f"✓ Check completed successfully")
                logging.info(f"  - Transcripts found: {result.get('total_found', 0)}")
                logging.info(f"  - Processed: {result['processed']}")
                logging.info(f"  - Skipped: {result['skipped']}")
            else:
                logging.error(f"✗ Check failed: {result.get('error', 'Unknown error')}")
            
            # Sleep until next check
            logging.info(f"Sleeping for {CHECK_INTERVAL // 60} minutes...")
            logging.info(f"Next check at: {datetime.fromtimestamp(time.time() + CHECK_INTERVAL).strftime('%Y-%m-%d %H:%M:%S')}")
            
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            logging.info("\n" + "=" * 80)
            logging.info("Received stop signal (Ctrl+C)")
            logging.info("Shutting down monitor gracefully...")
            logging.info("=" * 80)
            break
            
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            logging.error(f"Will retry in {CHECK_INTERVAL // 60} minutes...")
            
            try:
                time.sleep(CHECK_INTERVAL)
            except KeyboardInterrupt:
                logging.info("\nReceived stop signal during error recovery")
                logging.info("Shutting down monitor...")
                break
    
    logging.info("Monitor stopped. Goodbye!")


if __name__ == "__main__":
    main()
