#!/usr/bin/env python3
"""
Utility functions for the Fast processing Agent.

"""

import logging
import hashlib
import random
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any

# File status constants (matching Django FileStatus choices)
class FileStatus:
    REGISTERED = 'registered'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'
    DONE = 'done'


def validate_config(config: dict) -> None:
    """Validate the configuration parameters for message-driven agent."""
    required_keys = [
        "selection_fraction",
    ]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")

    if not (0.0 <= config["selection_fraction"] <= 1.0):
        raise ValueError("selection_fraction must be between 0.0 and 1.0")
    

def calculate_checksum(file_path: str, logger: logging.Logger) -> str:
    """
    Calculate MD5 checksum of file.

    Args:
        file_path: Path to the file as string
        logger: Logger instance

    Returns:
        MD5 checksum string
    """
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating checksum for {file_path}: {e}")
        return ""


def simulate_tf_subsamples(stf_file: Dict[str, Any], fast_processing: dict, config: dict, logger: logging.Logger, agent_name: str, force_sample: bool = False) -> List[Dict[str, Any]]:
    """
    Simulate creation of Time Frame (TF) subsamples from a Super Time Frame (STF) file.

    The total TFs to sample is tf_count * tf_size_fraction. This is divided into files
    of tfs_per_subsample TFs each, giving n_files = floor(sampled_tfs / tfs_per_subsample).
    Files are spread evenly across the STF range with a random offset within each partition,
    guaranteeing no overlaps.

    Args:
        stf_file: STF data dictionary (follows the keys from daq agent)
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        List of TF metadata dictionaries
    """
    try:
        selection_fraction = fast_processing.get("selection_fraction", config.get("selection_fraction", 1.0))
        if not force_sample and random.random() >= selection_fraction:
            logger.debug(f"STF file {stf_file.get('filename')} skipped by selection_fraction={selection_fraction}")
            return []

        tf_size_fraction = fast_processing.get("tf_size_fraction", config.get("tf_size_fraction", 0.15))
        tfs_per_subsample = fast_processing.get("tfs_per_subsample", config.get("tfs_per_subsample", 20))
        tf_sequence_start = fast_processing.get("tf_sequence_start", config.get("tf_sequence_start", 1))

        tf_count = stf_file.get("tf_count") or fast_processing.get("tf_count_per_stf", config.get("tf_count_per_stf", 1000))
        total_sampled = int(tf_count * tf_size_fraction)
        n_files = max(1, total_sampled // tfs_per_subsample)
        partition_width = tf_count // n_files

        tf_subsamples = []
        base_filename = stf_file.get("filename", "unknown").rsplit('.', 1)[0]

        for i in range(n_files):
            sequence_number = tf_sequence_start + i
            partition_start = i * partition_width
            partition_end = partition_start + partition_width - 1 if i < n_files - 1 else tf_count - 1

            partition_size = partition_end - partition_start + 1
            sample_size = min(tfs_per_subsample, partition_size)
            max_start = partition_end - sample_size + 1
            tf_first = random.randint(partition_start, max_start) if max_start > partition_start else partition_start
            tf_last = tf_first + sample_size - 1

            tf_filename = f"{base_filename}_tf_{sequence_number:03d}.tf"

            tf_metadata = {
                "tf_filename": tf_filename,
                "tf_first": tf_first,
                "tf_last": tf_last,
                "tf_count": tf_last - tf_first + 1,
                "file_size_bytes": tfs_per_subsample,
                "sequence_number": sequence_number,
                "stf_parent": stf_file.get("filename"),
                "metadata": {
                    "simulation": True,
                    "created_from": stf_file.get('filename'),
                    "tf_size_fraction": tf_size_fraction,
                    "tfs_per_subsample": tfs_per_subsample,
                    "agent_name": agent_name,
                    "state": stf_file.get('state'),
                    "substate": stf_file.get('substate'),
                    "start": stf_file.get('start'),
                    "end": stf_file.get('end'),
                }
            }

            tf_subsamples.append(tf_metadata)

        return tf_subsamples

    except Exception as e:
        logger.error(f"Unexpected error simulating TF subsamples: {e}")
        return []


def record_tf_file(tf_metadata: Dict[str, Any], config: dict, agent, logger: logging.Logger) -> Dict[str, Any]:
    """
    Record a Time Frame (TF) file in the database using REST API.
    
    Args:
        tf_metadata: TF metadata dictionary from simulate_tf_subsamples
        config: Configuration dictionary
        agent: BaseAgent instance for API access
        logger: Logger instance
        
    Returns:
        FastMonFile data dictionary or None if failed
    """
    try:
        # Prepare FastMonFile data for API
        tf_file_data = {
            "stf_file": tf_metadata.get("stf_parent", None),  # STF filename as parent identifier
            "tf_filename": tf_metadata["tf_filename"],
            "tf_first": tf_metadata["tf_first"],
            "tf_last": tf_metadata["tf_last"],
            "tf_count": tf_metadata["tf_count"],
            "file_size_bytes": tf_metadata["file_size_bytes"],
            "status": FileStatus.REGISTERED,
            "metadata": tf_metadata.get("metadata", {})
        }
        
        # Check if TF file already registered
        tf_filename = tf_metadata["tf_filename"]
        existing = agent.call_monitor_api('GET', f'/fastmon-files/?tf_filename={tf_filename}')
        if existing:
            match = next((r for r in existing if r.get('tf_filename') == tf_filename), None)
            if match:
                logger.info(f"TF file {tf_filename} already registered with ID {match.get('tf_file_id')}, skipping")
                return {**match, '_already_registered': True}

        # Create TF file record via FastMonFile API
        tf_file = agent.call_monitor_api('post', '/fastmon-files/', tf_file_data)
        tf_file_id = tf_file.get('tf_file_id') or tf_file.get('id') or 'unknown'
        logger.debug(f"Recorded TF file: {tf_metadata['tf_filename']} -> {tf_file_id}")
        return tf_file
        
    except Exception as e:
        logger.error(f"Error recording TF file {tf_metadata['tf_filename']}: {e}")
        return {}
