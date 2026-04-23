"""
Route Optimization V7 - Linked Reality
Imports emission factors directly from add_distance_and_emissions.py to ensure 
mathematical consistency without altering the upstream data pipeline.
"""

import json
import os
from collections import defaultdict
from heapq import heappush, heappop
from tabulate import tabulate

# ==========================================
# IMPORT SHARED DATA
# ==========================================
import add_distance_and_emissions as shared_data

# ==========================================
# REAL-WORLD LOGISTICS PARAMETERS
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROUTES_FILE = os.path.join(BASE_DIR, 'routes.json')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'recommendations.json')

MAX_HOPS = 3 
MAX_CIRCUITY = 1.8  # Detour cannot exceed 1.8x the baseline distance

# Average speeds (km/h) to calculate Transit Time Impact
SPEEDS_KMH = {
    'Air': 800,
    'Road': 65,
    'Rail': 42,
    'Ocean': 34
}

# ==========================================
# DATA LOADING & GRAPH
# ==========================================
def load_routes():
    with open(ROUTES_FILE, 'r') as f:
        routes = json.load(f)
    if isinstance(routes, list): return routes
    elif 'routes' in routes: return routes['routes']
    return routes

def build_route_graph(routes):
    graph = defaultdict(lambda: defaultdict(list))
    direct_routes = {}
    for route in routes:
        origin, dest = route['fromISO'], route['toISO']
        graph[origin][dest].append(route)
        key = (origin, dest)
        if key not in direct_routes or route['total_emissions_kgco2e'] < direct_routes[key]['total_emissions_kgco2e']:
            direct_routes[key] = route
    return graph, direct_routes

# ==========================================
# DETERMINISTIC MATH (LINKED)
# ==========================================
def calculate_edge_cost(route_data, shipment_volume_tonnes):
    """
    Calculates edge cost using the calculate_total_emissions function from
    add_distance_and_emissions.py to ensure mathematical consistency.
    """
    mode = route_data['mode']
    
    # Pull the multiplier dynamically from the shared file
    factor = shared_data.EMISSION_FACTORS.get(mode, 0.000015) 
    
    # Convert tonnes to kg (since enrichment script uses netWgt in kg)
    volume_kg = shipment_volume_tonnes * 1000
    
    # Use the shared calculation function for consistency
    return shared_data.calculate_total_emissions(volume_kg, route_data['distance_km'], factor)

def calculate_transit_hours(distance, mode):
    speed = SPEEDS_KMH.get(mode, 35)
    return distance / speed

# ==========================================
# YEN'S ALGORITHM WITH GEOGRAPHIC CHECKS
# ==========================================
def find_k_shortest_paths(graph, origin, destination, shipment_volume_tonnes, baseline_dist, k=4):
    def dijkstra(graph, origin, destination, blocked_edges=None):
        if blocked_edges is None: blocked_edges = set()
        queue = [(0, 0, 0, 0, [origin], [])]
        visited = set()
        
        while queue:
            emissions, _, hops, current_dist, path, routes = heappop(queue)
            current = path[-1]
            
            # GEOGRAPHIC REALITY CHECK
            if current_dist > (baseline_dist * MAX_CIRCUITY):
                continue
                
            if hops >= MAX_HOPS: continue
            if current == destination:
                return {'path': path, 'total_emissions': emissions, 'routes': routes, 'num_hops': hops, 'total_dist': current_dist}
            
            state = (current, hops)
            if state in visited: continue
            visited.add(state)
            
            if current in graph:
                for neighbor, route_list in graph[current].items():
                    if (current, neighbor) in blocked_edges: continue
                    
                    best_route = min(route_list, key=lambda r: calculate_edge_cost(r, shipment_volume_tonnes))
                    edge_cost = calculate_edge_cost(best_route, shipment_volume_tonnes)
                    edge_dist = best_route['distance_km']
                    
                    heappush(queue, (
                        emissions + edge_cost, 
                        id(path), # This integer breaks the tie if emissions are equal
                        hops + 1, 
                        current_dist + edge_dist, 
                        path + [neighbor], 
                        routes + [best_route]
                    ))
        return None
    
    paths = []
    shortest = dijkstra(graph, origin, destination)
    if not shortest: return []
    paths.append(shortest)
    
    candidates = []
    for i in range(k - 1):
        for j in range(len(paths[-1]['path']) - 1):
            root_path = paths[-1]['path'][:j+1]
            blocked = set()
            for prev_path in paths:
                if prev_path['path'][:j+1] == root_path and j < len(prev_path['path']) - 1:
                    blocked.add((prev_path['path'][j], prev_path['path'][j+1]))
            
            spur_node = root_path[-1]
            spur_path = dijkstra(graph, spur_node, destination, blocked)
            
            if spur_path:
                total_path = root_path[:-1] + spur_path['path']
                root_emissions = 0
                root_dist = 0
                root_routes = []
                for idx in range(len(root_path) - 1):
                    src, dst = root_path[idx], root_path[idx+1]
                    best = min(graph[src][dst], key=lambda r: calculate_edge_cost(r, shipment_volume_tonnes))
                    root_emissions += calculate_edge_cost(best, shipment_volume_tonnes)
                    root_dist += best['distance_km']
                    root_routes.append(best)
                
                total_dist = root_dist + spur_path['total_dist']
                candidate = {
                    'path': total_path,
                    'total_emissions': root_emissions + spur_path['total_emissions'],
                    'routes': root_routes + spur_path['routes'],
                    'num_hops': len(total_path) - 1,
                    'total_dist': total_dist
                }
                
                if candidate['num_hops'] <= MAX_HOPS and candidate['total_dist'] <= (baseline_dist * MAX_CIRCUITY):
                    is_duplicate = any(p['path'] == candidate['path'] for p in paths)
                    if not is_duplicate:
                        is_duplicate = any(c[2]['path'] == candidate['path'] for c in candidates)
                    if not is_duplicate:
                        heappush(candidates, (candidate['total_emissions'], id(candidate), candidate))
        
        if candidates:
            _, _, best_candidate = heappop(candidates)
            paths.append(best_candidate)
        else: break
    return paths

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("=" * 80)
    print("ROUTE OPTIMIZATION V7 - LINKED DATA ARCHITECTURE")
    print("=" * 80)
    
    routes = load_routes()
    graph, direct_routes = build_route_graph(routes)
    recommendations = {}
    summary = []
    
    print(f"\nEvaluating {len(direct_routes):,} routes utilizing shared emission factors...")
    
    for idx, ((origin, dest), baseline_route) in enumerate(direct_routes.items()):
        baseline_emissions = baseline_route['total_emissions_kgco2e']
        volume = baseline_route['volume_tonnes']
        baseline_dist = baseline_route['distance_km']
        
        baseline_time = calculate_transit_hours(baseline_dist, baseline_route['mode'])
        
        paths = find_k_shortest_paths(graph, origin, dest, volume, baseline_dist, k=4)
        if not paths: continue
        
        key = f"{origin}_{dest}"
        recommendations[key] = []
        
        alt_count = 1
        for path_info in paths:
            if len(path_info['path']) == 2 and path_info['path'][0] == origin and path_info['path'][1] == dest:
                continue
                
            opt_emissions = path_info['total_emissions']
            savings = ((baseline_emissions - opt_emissions) / baseline_emissions) * 100 if baseline_emissions else 0
            
            opt_time = sum(calculate_transit_hours(r['distance_km'], r['mode']) for r in path_info['routes'])
            time_diff_days = (opt_time - baseline_time) / 24
            
            hops = [{
                'from': route['fromISO'], 
                'to': route['toISO'], 
                'mode': route['mode'],
                'distance_km': route['distance_km']
            } for route in path_info['routes']]
            
            recommendations[key].append({
                'rank': alt_count,
                'path': path_info['path'],
                'path_str': ' → '.join(path_info['path']),
                'total_emissions': round(opt_emissions, 2),
                'savings_pct': round(savings, 2),
                'time_impact_days': round(time_diff_days, 1),
                'num_hops': path_info['num_hops'],
                'hops': hops
            })
            
            if alt_count == 1:
                time_str = f"+{time_diff_days:.1f}d" if time_diff_days > 0 else f"{time_diff_days:.1f}d"
                summary.append([origin, dest, f"{savings:.1f}%", time_str, ' -> '.join(path_info['path'])])
            
            alt_count += 1
            if alt_count > 3: break

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(recommendations, f, indent=2)
    
    summary.sort(key=lambda x: float(x[2].strip('%')), reverse=True)
    print("\n" + tabulate(summary[:20], headers=["From", "To", "CO2 Savings", "Time Impact", "Best Realistic Path"], tablefmt="github"))
    print(f"\nDone! Route optimization complete. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()