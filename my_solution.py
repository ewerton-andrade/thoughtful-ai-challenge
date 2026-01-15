#!/usr/bin/env python3
"""
Routing Cycle Detector
Finds the longest routing cycle in a claim routing file.
"""

import sys
from collections import defaultdict


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
    if len(sys.argv) != 2:
        print("Usage: python3 my_solution.py <input_file>", file=sys.stderr)
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    claim_id, status_code, cycle_length = find_longest_routing_cycle(filepath)
    
    if claim_id is not None:
        print(f"{claim_id},{status_code},{cycle_length}")
    else:
        print("No cycles found", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
