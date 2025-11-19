import math
import random
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass
import copy

# Re-using your dataclass or defining a compatible interface
@dataclass
class OptimizationJob:
    id: str
    lat: float
    lon: float
    duration: float
    priority_score: float # Higher is more important

class SmartRouter:
    """
    Replaces the Greedy Nearest Neighbor approach with a Simulated Annealing
    optimizer to reduce drive time and fit more jobs.
    """

    def __init__(self, avg_speed_mph: float = 55.0):
        self.avg_speed = avg_speed_mph

    def haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Standard distance calculation."""
        R = 3958.8
        try:
            lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            return R * c
        except (ValueError, TypeError):
            return 0.0

    def calculate_route_cost(self, route: List[OptimizationJob], start_loc: Tuple[float, float]) -> Tuple[float, float]:
        """
        Calculates total time (drive + work) and drive distance.
        Returns: (total_hours, total_miles)
        """
        if not route:
            return 0.0, 0.0

        current_lat, current_lon = start_loc
        total_miles = 0.0
        total_work = 0.0

        for job in route:
            dist = self.haversine(current_lat, current_lon, job.lat, job.lon)
            total_miles += dist
            total_work += job.duration
            current_lat, current_lon = job.lat, job.lon

        drive_hours = total_miles / self.avg_speed
        total_hours = drive_hours + total_work
        return total_hours, total_miles

    def build_route(self, 
                   available_jobs: List[Any], 
                   start_location: Tuple[float, float], 
                   max_daily_hours: float = 10.0) -> Tuple[List[Any], float, float, Tuple[float, float]]:
        """
        The main entry point. Attempts to find the best subset of jobs 
        and the best order.
        """
        
        # 1. Convert your Job objects to OptimizationJob
        opt_jobs = []
        for j in available_jobs:
            # Handle dict or object input
            if isinstance(j, dict):
                opt_jobs.append(OptimizationJob(
                    id=j.get('work_order'), lat=j.get('latitude'), lon=j.get('longitude'),
                    duration=j.get('duration', 2.0), priority_score=10 if j.get('jp_priority') == 'Urgent' else 1
                ))
            else:
                opt_jobs.append(OptimizationJob(
                    id=j.work_order, lat=j.latitude, lon=j.longitude,
                    duration=j.duration, priority_score=10 if j.jp_priority == 'Urgent' else 1
                ))

        # 2. Initial Solution: Use your existing Greedy method (Nearest Neighbor)
        # This gives us a valid starting point.
        current_route = self._greedy_initial_solution(opt_jobs, start_location, max_daily_hours)
        
        # 3. Optimize with Simulated Annealing
        # We try to swap jobs in the sequence or swap a job in the route with one currently left out
        best_route = self._simulated_annealing(current_route, opt_jobs, start_location, max_daily_hours)

        # 4. Map back to original objects
        final_job_ids = [j.id for j in best_route]
        final_jobs_original = [j for j in available_jobs if (j.get('work_order') if isinstance(j, dict) else j.work_order) in final_job_ids]
        
        # Sort original objects to match optimization order
        final_jobs_ordered = []
        for obj_id in final_job_ids:
            for original in final_jobs_original:
                oid = original.get('work_order') if isinstance(original, dict) else original.work_order
                if oid == obj_id:
                    final_jobs_ordered.append(original)
                    break
        
        hours, miles = self.calculate_route_cost(best_route, start_location)
        drive_hours = miles / self.avg_speed
        
        last_loc = start_location
        if best_route:
            last_loc = (best_route[-1].lat, best_route[-1].lon)

        return final_jobs_ordered, (hours - drive_hours), drive_hours, last_loc

    def _greedy_initial_solution(self, pool: List[OptimizationJob], start_loc, max_hours):
        """Recreates your logic to get a baseline."""
        route = []
        remaining = pool.copy()
        curr = start_loc
        current_time = 0
        
        while remaining:
            # Find nearest
            nearest = min(remaining, key=lambda x: self.haversine(curr[0], curr[1], x.lat, x.lon))
            dist = self.haversine(curr[0], curr[1], nearest.lat, nearest.lon)
            time_cost = (dist / self.avg_speed) + nearest.duration
            
            if current_time + time_cost <= max_hours:
                route.append(nearest)
                current_time += time_cost
                curr = (nearest.lat, nearest.lon)
                remaining.remove(nearest)
            else:
                break
        return route

    def _simulated_annealing(self, initial_route: List[OptimizationJob], all_pool: List[OptimizationJob], start_loc, max_hours):
        """
        Tries to improve the route by swapping order.
        Objective: Minimize Distance while maximizing Priority Score.
        """
        current_route = copy.deepcopy(initial_route)
        best_route = copy.deepcopy(initial_route)
        
        T = 100.0
        T_min = 0.1
        alpha = 0.99
        
        while T > T_min:
            i = random.randint(0, len(current_route) - 1) if current_route else 0
            j = random.randint(0, len(current_route) - 1) if current_route else 0
            
            if not current_route:
                break

            # Create Neighbor: Swap two stops
            new_route = copy.deepcopy(current_route)
            new_route[i], new_route[j] = new_route[j], new_route[i]
            
            # Calculate Costs
            curr_h, curr_m = self.calculate_route_cost(current_route, start_loc)
            new_h, new_m = self.calculate_route_cost(new_route, start_loc)
            
            # Energy = Distance (we want to minimize)
            # If we wanted to include priority, we'd subtract priority from energy
            current_energy = curr_m
            new_energy = new_m
            
            # If valid (time constraint) and better energy (or prob acceptance)
            if new_h <= max_hours:
                delta = new_energy - current_energy
                if delta < 0 or random.random() < math.exp(-delta / T):
                    current_route = new_route
                    if new_energy < self.calculate_route_cost(best_route, start_loc)[1]:
                        best_route = copy.deepcopy(new_route)
            
            T *= alpha
            
        return best_route