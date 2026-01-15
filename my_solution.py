#!/usr/bin/env python3
"""
Routing Cycle Detector
Finds the longest routing cycle in a claim routing file.
Downloads data from URL before processing.
"""

import sys
import os
import argparse
import re
import urllib.request
import urllib.error
from collections import defaultdict


def extract_google_drive_id(url):
    """Extract file ID from Google Drive URL."""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'/d/([a-zA-Z0-9_-]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def download_file(url, dest_folder="data"):
    """
    Download file from URL (supports Google Drive links).
    Returns the path to the downloaded file.
    """
    # Create destination folder if it doesn't exist
    os.makedirs(dest_folder, exist_ok=True)
    
    # Check if it's a Google Drive URL
    if "drive.google.com" in url:
        file_id = extract_google_drive_id(url)
        if not file_id:
            raise ValueError("Could not extract file ID from Google Drive URL")
        
        # Use the drive.usercontent.google.com endpoint for large files
        download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
        filename = "large_input_v1.txt"
    else:
        download_url = url
        filename = os.path.basename(url.split('?')[0]) or "downloaded_data.txt"
    
    dest_path = os.path.join(dest_folder, filename)
    
    print(f"Downloading from: {download_url}")
    print(f"Saving to: {dest_path}")
    
    try:
        # Create request with headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        request = urllib.request.Request(download_url, headers=headers)
        
        with urllib.request.urlopen(request, timeout=300) as response:
            total_size = response.headers.get('Content-Length')
            if total_size:
                total_size = int(total_size)
                print(f"File size: {total_size / (1024*1024):.2f} MB")
            
            # Download with progress indicator
            downloaded = 0
            chunk_size = 1024 * 1024  # 1MB chunks
            
            with open(dest_path, 'wb') as out_file:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}% ({downloaded / (1024*1024):.2f} MB)", end='', flush=True)
                    else:
                        print(f"\rDownloaded: {downloaded / (1024*1024):.2f} MB", end='', flush=True)
            
            print()  # New line after progress
        
        # Verify file was downloaded
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            file_size = os.path.getsize(dest_path)
            print(f"Download complete! File size: {file_size / (1024*1024):.2f} MB")
            
            # Quick validation - check if it's HTML (error page)
            with open(dest_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
                if first_line.strip().startswith('<!DOCTYPE html>') or first_line.strip().startswith('<html'):
                    raise RuntimeError("Download failed - received HTML page instead of data file")
            
            return dest_path
        else:
            raise RuntimeError("Download failed - file is empty or doesn't exist")
            
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download file: {e}")


def find_longest_cycle_in_graph(graph):
    """
    Find the longest simple cycle in a directed graph using DFS.
    Returns the length of the longest cycle found.
    """
    if not graph:
        return 0
    
    nodes = set(graph.keys())
    for destinations in graph.values():
        nodes.update(destinations)
    
    longest = 0
    
    def dfs(start, current, visited, path_length):
        nonlocal longest
        
        if current not in graph:
            return
        
        for neighbor in graph[current]:
            if neighbor == start and path_length >= 1:
                # Found a cycle back to start
                longest = max(longest, path_length + 1)
            elif neighbor not in visited:
                visited.add(neighbor)
                dfs(start, neighbor, visited, path_length + 1)
                visited.remove(neighbor)
    
    # Try starting from each node
    for start_node in graph:
        visited = {start_node}
        dfs(start_node, start_node, visited, 0)
    
    return longest


def process_file(filepath):
    """
    Stream the file and build graphs per (claim_id, status_code).
    Returns dictionary mapping (claim_id, status_code) -> graph
    """
    # Dictionary: (claim_id, status_code) -> {source -> [destinations]}
    graphs = defaultdict(lambda: defaultdict(list))
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) != 4:
                continue
            
            source, dest, claim_id, status_code = parts
            key = (claim_id, status_code)
            graphs[key][source].append(dest)
    
    return graphs


def find_longest_routing_cycle(filepath):
    """
    Main function to find the longest routing cycle.
    Returns (claim_id, status_code, cycle_length)
    """
    graphs = process_file(filepath)
    
    best_claim_id = None
    best_status_code = None
    best_length = 0
    
    for (claim_id, status_code), graph in graphs.items():
        cycle_length = find_longest_cycle_in_graph(dict(graph))
        
        if cycle_length > best_length:
            best_length = cycle_length
            best_claim_id = claim_id
            best_status_code = status_code
    
    return best_claim_id, best_status_code, best_length


def main():
    parser = argparse.ArgumentParser(
        description="Routing Cycle Detector - Downloads data and finds the longest routing cycle"
    )
    parser.add_argument(
        "data_url",
        help="URL to download the input data file (supports Google Drive links)"
    )
    parser.add_argument(
        "--dest-folder",
        default="data",
        help="Destination folder for downloaded file (default: data)"
    )
    
    args = parser.parse_args()
    
    try:
        # Download the file
        print("=== Downloading Data ===")
        filepath = download_file(args.data_url, args.dest_folder)
        
        # Process the downloaded file
        print("\n=== Processing Data ===")
        claim_id, status_code, cycle_length = find_longest_routing_cycle(filepath)
        
        if claim_id is not None:
            print(f"\n=== Result ===")
            print(f"{claim_id},{status_code},{cycle_length}")
        else:
            print("No cycles found", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
