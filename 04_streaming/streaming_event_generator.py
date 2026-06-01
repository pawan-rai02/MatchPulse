"""
Real-Time Streaming Event Generator for MatchPulse
===================================================

This script continuously generates soccer match events and writes them to S3
in real-time, simulating live match data for the Auto Loader streaming pipeline.

Key Features:
- Loads historical StatsBomb match data
- Streams events in small batches (realistic timing)
- Writes JSON files to S3 every few seconds
- Uses Databricks dbutils for S3 access (no AWS credentials needed)
- Graceful shutdown with SIGINT handling

Usage:
    python streaming_event_generator.py --match-id 3869685 --speed 1.0

Architecture:
    StatsBomb Data → Memory Buffer → S3 Writer (dbutils) → Auto Loader → DLT Pipeline
"""

import sys
import os
import json
import time
import argparse
import signal
from datetime import datetime
from pathlib import Path

# Add MatchPulse to path
sys.path.append('/Workspace/Users/pawanvirat32@gmail.com/MatchPulse')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.paths import EVENTS_BRONZE, STREAMING_PATH


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def convert_to_json_serializable(obj):
    """
    Recursively convert objects to JSON-serializable format.
    Handles datetime, Row objects, lists, and dicts.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    elif hasattr(obj, 'asDict'):  # PySpark Row object
        return convert_to_json_serializable(obj.asDict(recursive=True))
    else:
        return obj


class StreamingEventGenerator:
    """
    Generates real-time streaming events by reading historical match data
    and writing it to S3 in small batches.
    """
    
    def __init__(self, match_id: int, output_path: str, speed: float = 1.0, batch_size: int = 50):
        """
        Initialize the streaming event generator.
        
        Args:
            match_id: StatsBomb match ID to stream
            output_path: S3 path to write streaming events (e.g., s3a://bucket/path/)
            speed: Playback speed multiplier (1.0 = real-time, 2.0 = 2x speed)
            batch_size: Number of events per batch file
        """
        self.match_id = match_id
        self.output_path = output_path.rstrip('/')
        self.speed = speed
        self.batch_size = batch_size
        self.running = True
        self.events_written = 0
        self.start_time = None
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("StreamingEventGenerator") \
            .getOrCreate()
        
        # Get dbutils from SparkSession
        from pyspark.dbutils import DBUtils
        self.dbutils = DBUtils(self.spark)
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        
        print(f"✅ Initialized Streaming Event Generator")
        print(f"   Match ID: {match_id}")
        print(f"   Output: {output_path}")
        print(f"   Speed: {speed}x")
        print(f"   Batch Size: {batch_size} events")
    
    def _signal_handler(self, signum, frame):
        """Handle SIGINT (Ctrl+C) for graceful shutdown."""
        print("\n🛑 Shutdown signal received. Finishing current batch...")
        self.running = False
    
    def load_match_events(self):
        """Load all events for the specified match from bronze layer."""
        print(f"📥 Loading events for match {self.match_id}...")
        
        # Read from bronze events (Parquet format)
        events_df = (
            self.spark.read
            .format("parquet")
            .load(EVENTS_BRONZE)
            .filter(F.col("match_id") == self.match_id)
            .orderBy("minute", "second", "index")
        )
        
        event_count = events_df.count()
        print(f"✅ Loaded {event_count:,} events")
        
        if event_count == 0:
            raise ValueError(f"No events found for match_id={self.match_id}")
        
        return events_df
    
    def write_batch_to_s3(self, batch: list, batch_num: int):
        """
        Write a batch of events to S3 as a JSON file using dbutils.
        
        Args:
            batch: List of event dictionaries
            batch_num: Batch number for filename
        """
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"match_{self.match_id}_batch_{batch_num:05d}_{timestamp}.json"
        
        # Full S3 path for dbutils
        s3_file_path = f"{self.output_path}/{filename}"
        
        # Convert batch to JSON-serializable format (handle datetime objects)
        serializable_batch = [convert_to_json_serializable(event) for event in batch]
        
        # Convert batch to JSON Lines format (one event per line)
        json_lines = '\n'.join([json.dumps(event, cls=DateTimeEncoder) for event in serializable_batch])
        
        try:
            # Write to S3 using dbutils (uses cluster's S3 credentials automatically)
            self.dbutils.fs.put(s3_file_path, json_lines, overwrite=True)
            
            self.events_written += len(batch)
            return True
            
        except Exception as e:
            print(f"❌ Failed to write to S3: {e}")
            return False
    
    def stream_events(self):
        """
        Main streaming loop: read events and write them in batches.
        """
        # Load all events
        events_df = self.load_match_events()
        
        # Convert to list of dictionaries
        events_list = [row.asDict(recursive=True) for row in events_df.collect()]
        total_events = len(events_list)
        
        print(f"\n🚀 Starting streaming at {self.speed}x speed...")
        print(f"   Total events: {total_events:,}")
        print(f"   Batch size: {self.batch_size}")
        print(f"   Press Ctrl+C to stop\n")
        
        self.start_time = time.time()
        batch_num = 0
        current_batch = []
        last_minute = 0
        
        for i, event in enumerate(events_list):
            if not self.running:
                break
            
            # Add match_id to event (required by pipeline)
            event['match_id'] = self.match_id
            
            # Add event to current batch
            current_batch.append(event)
            
            # Write batch when it reaches batch_size OR when minute changes
            minute = event.get('minute', 0)
            minute_changed = (minute != last_minute) and (i > 0)
            batch_full = len(current_batch) >= self.batch_size
            
            if batch_full or minute_changed:
                # Write batch to S3
                success = self.write_batch_to_s3(current_batch, batch_num)
                
                if success:
                    batch_num += 1
                    elapsed = time.time() - self.start_time
                    progress = (self.events_written / total_events) * 100
                    
                    print(f"✅ Batch {batch_num:3d} | "
                          f"Events: {self.events_written:5,}/{total_events:,} ({progress:5.1f}%) | "
                          f"Minute: {minute:3d}' | "
                          f"Elapsed: {elapsed:6.1f}s")
                
                # Clear batch
                current_batch = []
                last_minute = minute
                
                # Simulate real-time delay (based on speed multiplier)
                # Sleep for a realistic interval between batches
                sleep_time = (3.0 / self.speed)  # 3 seconds per batch at 1x speed
                time.sleep(sleep_time)
        
        # Write remaining events
        if current_batch and self.running:
            self.write_batch_to_s3(current_batch, batch_num)
            batch_num += 1
        
        self._print_summary(batch_num)
    
    def _print_summary(self, total_batches: int):
        """Print streaming summary statistics."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        print("\n" + "=" * 80)
        print("🏁 STREAMING COMPLETE")
        print("=" * 80)
        print(f"Total Events Written : {self.events_written:,}")
        print(f"Total Batches        : {total_batches}")
        print(f"Total Time           : {elapsed:.1f}s")
        print(f"Events/Second        : {self.events_written / elapsed:.1f}" if elapsed > 0 else "N/A")
        print(f"Output Location      : {self.output_path}")
        print("=" * 80)


def main():
    """Main entry point for the streaming event generator."""
    parser = argparse.ArgumentParser(
        description="Generate real-time streaming events for MatchPulse"
    )
    
    parser.add_argument(
        "--match-id",
        type=int,
        default=3869685,
        help="StatsBomb match ID to stream (default: 3869685 - 2022 WC Final)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=STREAMING_PATH + "/match_events/",
        help="S3 output path for streaming events"
    )
    
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Playback speed multiplier (default: 1.0 = real-time)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of events per batch (default: 50)"
    )
    
    args = parser.parse_args()
    
    # Create generator and start streaming
    generator = StreamingEventGenerator(
        match_id=args.match_id,
        output_path=args.output,
        speed=args.speed,
        batch_size=args.batch_size
    )
    
    try:
        generator.stream_events()
    except Exception as e:
        print(f"\n❌ Error during streaming: {e}")
        raise


if __name__ == "__main__":
    main()
