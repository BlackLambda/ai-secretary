"""
Transcript Monitor Service
==========================

Monitors for new meeting transcripts and saves them as individual text files.

Features:
- Polls for new transcripts every 5 minutes
- Saves transcripts to output/meetings/ directory
- Tracks processed transcripts to avoid duplicates
- Overwrites existing files if transcript is updated
"""

import os
import json
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

from src.services.calendars import CalendarService


class TranscriptMonitor:
    """Service for monitoring and processing meeting transcripts."""

    def __init__(self, output_dir: str = "output/meetings", tracking_file: str = "output/transcript_tracking.json"):
        """
        Initialize the transcript monitor.

        Args:
            output_dir: Directory to save transcript text files
            tracking_file: JSON file to track processed transcripts
        """
        self.output_dir = output_dir
        self.tracking_file = tracking_file
        self.calendar_service = CalendarService()
        
        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Load or initialize tracking data
        self.tracking_data = self._load_tracking()

    def _load_tracking(self) -> Dict:
        """Load tracking data from file or create new if doesn't exist."""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load tracking file: {e}. Creating new one.")
        
        # Initialize new tracking data
        return {
            "last_check_time": datetime.now(timezone.utc).isoformat(),
            "processed_transcripts": {}
        }

    def _save_tracking(self):
        """Save tracking data to file."""
        try:
            # Ensure directory exists
            Path(self.tracking_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.tracking_file, 'w', encoding='utf-8') as f:
                json.dump(self.tracking_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"Failed to save tracking file: {e}")

    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename for safe file system usage.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Remove .mp4 extension
        if filename.lower().endswith('.mp4'):
            filename = filename[:-4]
        
        # Replace spaces with underscores
        filename = filename.replace(' ', '_')
        
        # Remove or replace other problematic characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Remove multiple consecutive underscores
        filename = re.sub(r'_+', '_', filename)
        
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        
        return filename

    def _generate_filename(self, transcript: Dict) -> str:
        """
        Generate filename from transcript metadata.
        
        Args:
            transcript: Transcript data from API
            
        Returns:
            Filename in format: YYYY-MM-DD_sanitized_name.txt
        """
        # Get file name
        file_name = transcript.get('FileName', 'unknown_meeting')
        
        # Get creation date
        created_time = transcript.get('ItemProperties', {}).get('Default', {}).get('Created', '')
        if created_time:
            try:
                date_obj = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
                date_str = date_obj.strftime('%Y-%m-%d')
            except:
                date_str = datetime.now().strftime('%Y-%m-%d')
        else:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        # Sanitize filename
        sanitized_name = self._sanitize_filename(file_name)
        
        # Combine date and name
        return f"{date_str}_{sanitized_name}.txt"

    def _format_transcript_content(self, transcript: Dict) -> str:
        """
        Format transcript data into readable text.
        
        Args:
            transcript: Transcript data from API
            
        Returns:
            Formatted text content
        """
        file_name = transcript.get('FileName', 'Unknown')
        created_time = transcript.get('ItemProperties', {}).get('Default', {}).get('Created', 'Unknown')
        text_content = transcript.get('FileContent', {}).get('Text', '')
        
        # Format the output
        output = f"""Meeting: {file_name.replace('.mp4', '')}
Date: {created_time}

========================================
TRANSCRIPT:
========================================

{text_content}
"""
        return output

    def _save_transcript_to_file(self, transcript: Dict, filename: str):
        """
        Save transcript to text file.
        
        Args:
            transcript: Transcript data from API
            filename: Target filename
        """
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            content = self._format_transcript_content(transcript)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logging.info(f"Saved transcript to: {filename}")
            
        except Exception as e:
            logging.error(f"Failed to save transcript {filename}: {e}")
            raise

    def _get_transcript_id(self, transcript: Dict) -> str:
        """
        Generate unique ID for transcript.
        
        Args:
            transcript: Transcript data from API
            
        Returns:
            Unique identifier
        """
        # Use combination of filename and created time
        file_name = transcript.get('FileName', '')
        created_time = transcript.get('ItemProperties', {}).get('Default', {}).get('Created', '')
        return f"{file_name}_{created_time}"

    def check_and_process_new_transcripts(self) -> Dict:
        """
        Check for new transcripts and process them.
        
        Returns:
            Dict with processing results
        """
        logging.info("Checking for new transcripts...")
        
        # Get last check time
        last_check_time = self.tracking_data.get('last_check_time')
        current_time = datetime.now(timezone.utc)
        
        # Convert last check time to datetime
        try:
            last_check_dt = datetime.fromisoformat(last_check_time.replace('Z', '+00:00'))
        except:
            # If parsing fails, use 24 hours ago
            from datetime import timedelta
            last_check_dt = current_time - timedelta(hours=24)
        
        # Format for API
        start_date = last_check_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_date = current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        logging.info(f"Checking transcripts from {start_date} to {end_date}")
        
        # Query API for transcripts
        try:
            transcripts = self.calendar_service.get_meeting_most_recent_transcripts(
                start_datetime=start_date,
                end_datetime=end_date,
                top=100
            )
        except Exception as e:
            logging.error(f"Failed to fetch transcripts: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'processed': 0,
                'skipped': 0
            }
        
        if not transcripts:
            logging.info("No transcripts found")
            self.tracking_data['last_check_time'] = current_time.isoformat()
            self._save_tracking()
            return {
                'status': 'success',
                'processed': 0,
                'skipped': 0
            }
        
        # Process each transcript
        processed_count = 0
        skipped_count = 0
        
        for transcript in transcripts:
            # Check if transcript has content
            text_content = transcript.get('FileContent', {}).get('Text', '')
            if not text_content or not text_content.strip():
                skipped_count += 1
                continue
            
            # Generate unique ID
            transcript_id = self._get_transcript_id(transcript)
            
            # Generate filename
            filename = self._generate_filename(transcript)
            
            try:
                # Save transcript (will overwrite if exists)
                self._save_transcript_to_file(transcript, filename)
                
                # Update tracking
                self.tracking_data['processed_transcripts'][transcript_id] = {
                    'filename': filename,
                    'processed_at': current_time.isoformat()
                }
                
                processed_count += 1
                logging.info(f"Processed: {filename}")
                
            except Exception as e:
                logging.error(f"Failed to process transcript {filename}: {e}")
                skipped_count += 1
        
        # Update last check time
        self.tracking_data['last_check_time'] = current_time.isoformat()
        self._save_tracking()
        
        result = {
            'status': 'success',
            'processed': processed_count,
            'skipped': skipped_count,
            'total_found': len(transcripts)
        }
        
        logging.info(f"Processing complete: {processed_count} processed, {skipped_count} skipped")
        
        return result


if __name__ == "__main__":
    # Test the monitor
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    monitor = TranscriptMonitor()
    result = monitor.check_and_process_new_transcripts()
    print(json.dumps(result, indent=2))
