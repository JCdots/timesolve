import os
import json
import datetime
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from dataclasses import dataclass, asdict
from glpi_methods import authenticate_glpi, get_ticket_subitems, kill_session
from dotenv import load_dotenv

load_dotenv()

# Environment Configuration
GLPI_URL = os.getenv("DEFAULT_GLPI_URL")
APP_TOKEN = os.getenv("DEFAULT_APP_TOKEN")
USER = os.getenv("DEFAULT_USER")
PASSWORD = os.getenv("DEFAULT_PASS")

# Business Configuration
TICKET_ID = 3
TARGET_GROUP_ID = 1
OBSERVER_TYPE = 3

# Constants
GROUP_ASSIGNED_ACTION = 15
GROUP_UNASSIGNED_ACTION = 16
GLPI_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DISPLAY_DATE_FORMAT = "%d-%m-%Y %H:%M:%S"


@dataclass
class GroupInfo:
    """Represents a group with its ID and name."""
    id: int
    name: str = "Unknown"
    
    def __str__(self):
        return f"{self.name} ({self.id})"
    
    def __hash__(self):
        return hash(self.id)


@dataclass
class AssignmentPeriod:
    """Represents a single assignment period."""
    assigned: datetime.datetime
    unassigned: datetime.datetime
    duration: datetime.timedelta
    still_assigned: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'assigned': self.assigned.isoformat(),
            'unassigned': self.unassigned.isoformat(),
            'duration_seconds': self.duration.total_seconds(),
            'still_assigned': self.still_assigned
        }


class GLPISession:
    """Context manager for GLPI API sessions with automatic cleanup."""
    
    def __init__(self, url: str, user: str, password: str, app_token: Optional[str] = None):
        self.url = url
        self.user = user
        self.password = password
        self.app_token = app_token
        self.session_token = None
    
    def __enter__(self):
        self.session_token, error = authenticate_glpi(
            self.url,
            login=self.user,
            password=self.password,
            app_token=self.app_token
        )
        if not self.session_token:
            raise ConnectionError(f"Authentication failed: {error}")
        return self.session_token
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session_token:
            kill_session(self.url, self.session_token, app_token=self.app_token)


# ============================================================================
# Date/Time Utilities
# ============================================================================

def parse_glpi_date(date_str: str) -> datetime.datetime:
    """Parse GLPI date string to datetime object."""
    return datetime.datetime.strptime(date_str, GLPI_DATE_FORMAT)


def format_datetime(dt: datetime.datetime) -> str:
    """Format datetime for human-readable display."""
    return dt.strftime(DISPLAY_DATE_FORMAT)


def get_current_time() -> datetime.datetime:
    """Get current time without microseconds."""
    return datetime.datetime.now().replace(microsecond=0)


# ============================================================================
# Log Parsing Utilities
# ============================================================================

def extract_group_info_from_value(value: str) -> Optional[GroupInfo]:
    """
    Extract group ID and name from GLPI log value.
    
    Format: "GroupName (123)" -> GroupInfo(id=123, name="GroupName")
    """
    if not value or not value.strip():
        return None
    
    try:
        start = value.rfind('(')
        end = value.rfind(')')
        
        if start != -1 and end != -1 and start < end:
            group_id = int(value[start + 1:end])
            group_name = value[:start].strip()
            return GroupInfo(id=group_id, name=group_name)
    except (ValueError, AttributeError):
        pass
    
    return None


def is_assignment_log(log: Dict[str, Any], group_id: int) -> bool:
    """Check if log entry is an assignment for the specified group."""
    if log.get('itemtype_link') != 'Group':
        return False
    
    if int(log.get('linked_action', 0)) != GROUP_ASSIGNED_ACTION:
        return False
    
    group_info = extract_group_info_from_value(log.get('new_value', ''))
    return group_info and group_info.id == group_id


def is_unassignment_log(log: Dict[str, Any], group_id: int) -> bool:
    """Check if log entry is an unassignment for the specified group."""
    if log.get('itemtype_link') != 'Group':
        return False
    
    if int(log.get('linked_action', 0)) != GROUP_UNASSIGNED_ACTION:
        return False
    
    group_info = extract_group_info_from_value(log.get('old_value', ''))
    return group_info and group_info.id == group_id


def get_all_groups_from_logs(logs: List[Dict[str, Any]]) -> Dict[int, GroupInfo]:
    """Extract all unique groups mentioned in logs."""
    groups = {}
    
    for log in logs:
        if log.get('itemtype_link') != 'Group':
            continue
        
        linked_action = int(log.get('linked_action', 0))
        
        # Check assignment
        if linked_action == GROUP_ASSIGNED_ACTION:
            group_info = extract_group_info_from_value(log.get('new_value', ''))
        # Check unassignment
        elif linked_action == GROUP_UNASSIGNED_ACTION:
            group_info = extract_group_info_from_value(log.get('old_value', ''))
        else:
            continue
        
        if group_info and group_info.id not in groups:
            groups[group_info.id] = group_info
    
    return groups


# ============================================================================
# Time Calculation Engine
# ============================================================================

def calculate_group_time(logs: List[Dict[str, Any]], group_info: GroupInfo, verbose: bool = False) -> Tuple[datetime.timedelta, List[AssignmentPeriod]]:
    """
    Calculate total time a group spent assigned to a ticket.
    
    Args:
        logs: Sorted list of log entries (chronological order)
        group_info: Group to track
        verbose: Print assignment history
    
    Returns:
        (total_duration, list_of_periods)
    """
    total_duration = datetime.timedelta()
    current_assignment_start = None
    periods = []
    
    if verbose:
        print(f"\n--- Assignment History: {group_info} ---")
    
    for log in logs:
        date_mod = parse_glpi_date(log['date_mod'])
        
        # Assignment event
        if is_assignment_log(log, group_info.id):
            if current_assignment_start is None:  # Not already assigned
                current_assignment_start = date_mod
                if verbose:
                    print(f"[{format_datetime(date_mod)}] Assigned")
        
        # Unassignment event
        elif is_unassignment_log(log, group_info.id):
            if current_assignment_start is not None:  # Was assigned
                duration = date_mod - current_assignment_start
                total_duration += duration
                periods.append(AssignmentPeriod(
                    assigned=current_assignment_start,
                    unassigned=date_mod,
                    duration=duration
                ))
                current_assignment_start = None
                if verbose:
                    print(f"[{format_datetime(date_mod)}] Unassigned (Duration: {duration})")
    
    # Handle still-assigned case
    if current_assignment_start is not None:
        now = get_current_time()
        duration = now - current_assignment_start
        total_duration += duration
        periods.append(AssignmentPeriod(
            assigned=current_assignment_start,
            unassigned=now,
            duration=duration,
            still_assigned=True
        ))
        if verbose:
            print(f"[{format_datetime(now)}] Still Assigned (Duration: {duration})")
    
    return total_duration, periods


def calculate_all_groups(logs: List[Dict[str, Any]], verbose: bool = False) -> Dict[int, Dict[str, Any]]:
    """
    Calculate time for all groups in the logs.
    
    Returns:
        Dictionary: {group_id: {'group_info': GroupInfo, 'total_duration': timedelta, 'periods': list}}
    """
    sorted_logs = sorted(logs, key=lambda x: x['date_mod'])
    groups = get_all_groups_from_logs(sorted_logs)
    
    if verbose and groups:
        print(f"\nFound {len(groups)} groups:")
        for gid in sorted(groups.keys()):
            print(f"  - {groups[gid]}")
    
    results = {}
    for group_id in sorted(groups.keys()):
        group_info = groups[group_id]
        total_duration, periods = calculate_group_time(sorted_logs, group_info, verbose)
        
        results[group_id] = {
            'group_info': group_info,
            'total_duration': total_duration,
            'periods': periods,
            'assignment_count': len(periods)
        }
    
    return results


# ============================================================================
# Data Fetching
# ============================================================================

def fetch_ticket_logs(session_token: str, ticket_id: int) -> Optional[List[Dict[str, Any]]]:
    """Fetch logs for a ticket. Returns None on error."""
    logs, error = get_ticket_subitems(
        GLPI_URL, session_token, ticket_id, "Log", 
        app_token=APP_TOKEN, range_param='0-999'
    )
    
    if error:
        print(f"[ERROR] Ticket #{ticket_id}: {error}")
        return None
    
    return logs


# ============================================================================
# Analysis Functions
# ============================================================================

def analyze_ticket(session_token: str, ticket_id: int, verbose: bool = True) -> Optional[Dict[int, Dict[str, Any]]]:
    """
    Analyze all groups for a single ticket.
    
    Returns:
        Dictionary of results, or None if error
    """
    logs = fetch_ticket_logs(session_token, ticket_id)
    if logs is None:
        return None
    
    results = calculate_all_groups(logs, verbose)
    
    if verbose and results:
        print_ticket_summary(ticket_id, results)
    
    return results


def analyze_multiple_tickets(session_token: str, ticket_ids: List[int], verbose: bool = False) -> Dict[int, Dict[int, Dict[str, Any]]]:
    """
    Analyze multiple tickets and aggregate results.
    
    Returns:
        {ticket_id: {group_id: {...}}}
    """
    print(f"\n{'=' * 90}")
    print(f"Analyzing {len(ticket_ids)} tickets: {ticket_ids}")
    print(f"{'=' * 90}")
    
    all_results = {}
    group_aggregates = defaultdict(lambda: {
        'total_duration': datetime.timedelta(),
        'ticket_count': 0,
        'assignment_count': 0,
        'group_info': None
    })
    
    for ticket_id in ticket_ids:
        print(f"\n>>> Ticket #{ticket_id}")
        results = analyze_ticket(session_token, ticket_id, verbose)
        
        if results:
            all_results[ticket_id] = results
            
            # Aggregate
            for group_id, data in results.items():
                group_aggregates[group_id]['total_duration'] += data['total_duration']
                group_aggregates[group_id]['ticket_count'] += 1
                group_aggregates[group_id]['assignment_count'] += data['assignment_count']
                if group_aggregates[group_id]['group_info'] is None:
                    group_aggregates[group_id]['group_info'] = data['group_info']
    
    # Print aggregate summary
    if group_aggregates:
        print_aggregate_summary(ticket_ids, group_aggregates)
    
    return all_results


def analyze_ticket_range(session_token: str, start_id: int, end_id: int, verbose: bool = False) -> Dict[int, Dict[int, Dict[str, Any]]]:
    """Analyze a range of ticket IDs."""
    ticket_ids = list(range(start_id, end_id + 1))
    print(f"Ticket range: {start_id} to {end_id} ({len(ticket_ids)} tickets)")
    return analyze_multiple_tickets(session_token, ticket_ids, verbose)


# ============================================================================
# Display/Output Functions
# ============================================================================

def print_ticket_summary(ticket_id: int, results: Dict[int, Dict[str, Any]]) -> None:
    """Print summary table for a single ticket."""
    print("\n" + "=" * 90)
    print(f"SUMMARY - Ticket #{ticket_id}")
    print("=" * 90)
    print(f"{'Group ID':<12} {'Group Name':<25} {'Assignments':<15} {'Total Time':<20}")
    print("-" * 90)
    
    for group_id in sorted(results.keys()):
        data = results[group_id]
        print(f"{group_id:<12} {data['group_info'].name:<25} {data['assignment_count']:<15} {str(data['total_duration']):<20}")
    
    print("=" * 90)


def print_aggregate_summary(ticket_ids: List[int], aggregates: Dict[int, Dict[str, Any]]) -> None:
    """Print aggregate summary across multiple tickets."""
    print("\n" + "=" * 100)
    print(f"AGGREGATE SUMMARY - {len(ticket_ids)} Tickets")
    print("=" * 100)
    print(f"{'Group ID':<12} {'Group Name':<25} {'Tickets':<10} {'Assignments':<15} {'Total Time':<20}")
    print("-" * 100)
    
    for group_id in sorted(aggregates.keys()):
        data = aggregates[group_id]
        print(f"{group_id:<12} {data['group_info'].name:<25} {data['ticket_count']:<10} {data['assignment_count']:<15} {str(data['total_duration']):<20}")
    
    print("=" * 100)


def export_to_json(results: Dict[int, Dict[int, Dict[str, Any]]], filename: str = 'ticket_analysis.json') -> None:
    """Export results to JSON file."""
    export_data = {
        'generated_at': datetime.datetime.now().isoformat(),
        'tickets': {}
    }
    
    for ticket_id, groups in results.items():
        export_data['tickets'][ticket_id] = {}
        
        for group_id, data in groups.items():
            export_data['tickets'][ticket_id][group_id] = {
                'group_name': data['group_info'].name,
                'total_duration_seconds': data['total_duration'].total_seconds(),
                'total_duration_str': str(data['total_duration']),
                'assignment_count': data['assignment_count'],
                'periods': [p.to_dict() for p in data['periods']]
            }
    
    with open(filename, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print(f"\n[EXPORT] Results saved to {filename}")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main execution function."""
    try:
        with GLPISession(GLPI_URL, USER, PASSWORD, APP_TOKEN) as session_token:
            print(f"[AUTH] Session: {session_token}\n")
            
            # Example 1: Single ticket analysis
            print("=" * 90)
            print("EXAMPLE 1: Single Ticket Analysis")
            print("=" * 90)
            analyze_ticket(session_token, TICKET_ID, verbose=True)
            
            # Example 2: Multiple specific tickets
            print("\n\n" + "=" * 90)
            print("EXAMPLE 2: Multiple Tickets Analysis")
            print("=" * 90)
            results = analyze_multiple_tickets(session_token, [1, 2, 3], verbose=False)
            
            # Example 3: Ticket range
            print("\n\n" + "=" * 90)
            print("EXAMPLE 3: Ticket Range Analysis")
            print("=" * 90)
            analyze_ticket_range(session_token, start_id=1, end_id=5, verbose=False)
            
            # Example 4: Export to JSON
            if results:
                export_to_json(results)
            
    except ConnectionError as e:
        print(f"[ERROR] Connection: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected: {e}")
        raise


if __name__ == "__main__":
    main()