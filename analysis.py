import sqlite3
import statistics
import os
import calendar
from datetime import date

DB_FILENAME = "glpi_timesolve.db"

def seconds_to_hm(seconds):
    """Convert seconds to hours and minutes string."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"

def analyze_group_times_db(db_path):
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Query to get all durations for each group
        cursor.execute("SELECT group_name, total_seconds FROM group_durations")
        rows = cursor.fetchall()
        
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return

    # Dictionary to store list of durations for each group
    group_times = {}

    for row in rows:
        group_name = row['group_name']
        duration = row['total_seconds']
        
        if group_name not in group_times:
            group_times[group_name] = []
        group_times[group_name].append(duration)

    # Print header
    print(f"{'Group Name':<30} | {'Average Time':<15} | {'Median Time':<15} | {'Count':<5}")
    print("-" * 75)

    # Calculate and print stats for each group
    for group_name, times in sorted(group_times.items()):
        if not times:
            continue
            
        avg_seconds = statistics.mean(times)
        median_seconds = statistics.median(times)
        count = len(times)
        
        avg_str = seconds_to_hm(avg_seconds)
        median_str = seconds_to_hm(median_seconds)
        
        print(f"{group_name:<30} | {avg_str:<15} | {median_str:<15} | {count:<5}")

def analyze_tickets_with_assignments_since(db_path, start_date="2025-01-01", end_date=None):
    """
    Analyzes group durations for tickets that have at least one assignment 
    starting on or after the specified date (and optionally before end_date).
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Query to get durations for groups, but only for tickets that have 
        # an assignment starting on or after start_date
        if end_date:
            query = '''
                SELECT gd.group_name, gd.total_seconds 
                FROM group_durations gd
                WHERE gd.ticket_id IN (
                    SELECT DISTINCT ticket_id 
                    FROM assignment_periods 
                    WHERE start_time >= ? AND start_time <= ?
                )
            '''
            params = (start_date, end_date)
        else:
            query = '''
                SELECT gd.group_name, gd.total_seconds 
                FROM group_durations gd
                WHERE gd.ticket_id IN (
                    SELECT DISTINCT ticket_id 
                    FROM assignment_periods 
                    WHERE start_time >= ?
                )
            '''
            params = (start_date,)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return

    # Dictionary to store list of durations for each group
    group_times = {}

    for row in rows:
        group_name = row['group_name']
        duration = row['total_seconds']
        
        if group_name not in group_times:
            group_times[group_name] = []
        group_times[group_name].append(duration)

    if end_date:
        print(f"\nAnalysis for tickets with assignments between {start_date} and {end_date}")
    else:
        print(f"\nAnalysis for tickets with assignments since {start_date}")
    print(f"{'Group Name':<30} | {'Average Time':<15} | {'Median Time':<15} | {'Count':<5}")
    print("-" * 75)

    for group_name, times in sorted(group_times.items()):
        if not times:
            continue
            
        avg_seconds = statistics.mean(times)
        median_seconds = statistics.median(times)
        count = len(times)
        
        avg_str = seconds_to_hm(avg_seconds)
        median_str = seconds_to_hm(median_seconds)
        
        print(f"{group_name:<30} | {avg_str:<15} | {median_str:<15} | {count:<5}")

if __name__ == "__main__":
    # analyze_group_times_db(DB_FILENAME)
    
    print("Monthly Analysis for 2025:")
    for month in range(1, 13):
        start_date = f"2025-{month:02d}-01"
        last_day = calendar.monthrange(2025, month)[1]
        end_date = f"2025-{month:02d}-{last_day} 23:59:59"
        
        analyze_tickets_with_assignments_since(DB_FILENAME, start_date, end_date)
    
    print("\nOverall Analysis for Tickets with Assignments since 2025-01-01:")
    analyze_tickets_with_assignments_since(DB_FILENAME, "2025-01-01")