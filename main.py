import os
import datetime
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
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


class GroupInfo:
    """Represents a group with its ID and name."""
    
    def __init__(self, group_id: int, name: str = "Unknown"):
        self.id = group_id
        self.name = name
    
    def __str__(self):
        return f"{self.name} ({self.id})"
    
    def __repr__(self):
        return f"GroupInfo(id={self.id}, name='{self.name}')"
    
    def __eq__(self, other):
        if isinstance(other, GroupInfo):
            return self.id == other.id
        return False
    
    def __hash__(self):
        return hash(self.id)


class GLPISession:
    """Context manager for GLPI API sessions."""
    
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


def parse_glpi_date(date_str: str) -> datetime.datetime:
    """Parse GLPI date string to datetime object."""
    return datetime.datetime.strptime(date_str, GLPI_DATE_FORMAT)


def format_datetime(dt: datetime.datetime) -> str:
    """Format datetime for display."""
    return dt.strftime(DISPLAY_DATE_FORMAT)


def extract_group_info_from_value(value: str) -> Optional[GroupInfo]:
    """
    Extract group ID and name from GLPI log value string like 'GroupName (123)'.
    
    Args:
        value: Log value string in format "GroupName (ID)"
    
    Returns:
        GroupInfo object with ID and name, or None if parsing fails
    """
    if not value or not value.strip():
        return None
    
    try:
        # Find the last occurrence of parentheses (in case name has parentheses)
        start = value.rfind('(')
        end = value.rfind(')')
        
        if start != -1 and end != -1 and start < end:
            group_id = int(value[start + 1:end])
            group_name = value[:start].strip()
            return GroupInfo(group_id, group_name)
    except (ValueError, AttributeError):
        pass
    
    return None


def is_group_currently_assigned(groups: List[Dict[str, Any]], group_id: int, type_id: int) -> bool:
    """Check if a specific group is assigned with a specific type."""
    return any(
        int(g.get('groups_id', 0)) == group_id and int(g.get('type', 0)) == type_id
        for g in groups
    )


def get_all_groups_from_logs(logs: List[Dict[str, Any]]) -> Dict[int, GroupInfo]:
    """
    Extract all unique groups mentioned in logs.
    
    Args:
        logs: List of log entries
    
    Returns:
        Dictionary mapping group_id to GroupInfo object
    """
    groups = {}
    
    for log in logs:
        if log.get('itemtype_link') == 'Group':
            linked_action = int(log.get('linked_action', 0))
            
            if linked_action == GROUP_ASSIGNED_ACTION:
                group_info = extract_group_info_from_value(log.get('new_value', ''))
                if group_info and group_info.id not in groups:
                    groups[group_info.id] = group_info
            elif linked_action == GROUP_UNASSIGNED_ACTION:
                group_info = extract_group_info_from_value(log.get('old_value', ''))
                if group_info and group_info.id not in groups:
                    groups[group_info.id] = group_info
    
    return groups


def calculate_time_spent_for_group(
    logs: List[Dict[str, Any]], 
    group_info: GroupInfo, 
    verbose: bool = True
) -> Tuple[datetime.timedelta, List[Dict[str, Any]]]:
    """
    Calculate total time spent by a specific group.
    
    Args:
        logs: Sorted list of log entries
        group_info: GroupInfo object with ID and name
        verbose: Whether to print assignment history
    
    Returns:
        Tuple of (total_duration, assignment_periods)
        assignment_periods is a list of dicts with 'assigned', 'unassigned', and 'duration' keys
    """
    total_duration = datetime.timedelta()
    last_assign_date = None
    is_assigned = False
    assignment_periods = []
    
    if verbose:
        print(f"\n--- Assignment History for {group_info} ---")
    
    for log in logs:
        date_mod = parse_glpi_date(log.get('date_mod'))
        
        # Handle assignment
        if log.get('itemtype_link') == 'Group' and int(log.get('linked_action', 0)) == GROUP_ASSIGNED_ACTION:
            assigned_group = extract_group_info_from_value(log.get('new_value', ''))
            if assigned_group and assigned_group.id == group_info.id and not is_assigned:
                last_assign_date = date_mod
                is_assigned = True
                if verbose:
                    print(f"[{format_datetime(date_mod)}] Assigned")
        
        # Handle unassignment
        elif log.get('itemtype_link') == 'Group' and int(log.get('linked_action', 0)) == GROUP_UNASSIGNED_ACTION:
            unassigned_group = extract_group_info_from_value(log.get('old_value', ''))
            if unassigned_group and unassigned_group.id == group_info.id and is_assigned and last_assign_date:
                duration = date_mod - last_assign_date
                total_duration += duration
                assignment_periods.append({
                    'assigned': last_assign_date,
                    'unassigned': date_mod,
                    'duration': duration
                })
                is_assigned = False
                last_assign_date = None
                if verbose:
                    print(f"[{format_datetime(date_mod)}] Unassigned (Duration: {duration})")
    
    # Handle currently assigned case
    if is_assigned and last_assign_date:
        now = datetime.datetime.now().replace(microsecond=0)
        duration = now - last_assign_date
        total_duration += duration
        assignment_periods.append({
            'assigned': last_assign_date,
            'unassigned': now,
            'duration': duration,
            'still_assigned': True
        })
        if verbose:
            print(f"[{format_datetime(now)}] Still Assigned (Current stint: {duration})")
    
    return total_duration, assignment_periods


def calculate_all_groups_time(
    logs: List[Dict[str, Any]], 
    verbose: bool = True
) -> Dict[int, Dict[str, Any]]:
    """
    Calculate time spent for all groups found in the logs.
    
    Args:
        logs: List of log entries
        verbose: Whether to print detailed history for each group
    
    Returns:
        Dictionary mapping group_id to dict with 'group_info', 'total_duration' and 'periods'
    """
    # Sort logs chronologically once
    sorted_logs = sorted(logs, key=lambda x: x['date_mod'])
    
    # Get all unique groups with their names
    groups = get_all_groups_from_logs(sorted_logs)
    
    if verbose:
        print(f"\nFound {len(groups)} unique groups in ticket history:")
        for group_id in sorted(groups.keys()):
            print(f"  - {groups[group_id]}")
    
    # Calculate time for each group
    results = {}
    for group_id in sorted(groups.keys()):
        group_info = groups[group_id]
        total_duration, periods = calculate_time_spent_for_group(sorted_logs, group_info, verbose)
        results[group_id] = {
            'group_info': group_info,
            'total_duration': total_duration,
            'periods': periods,
            'assignment_count': len(periods)
        }
    
    return results


def check_group_assignment(session_token: str, ticket_id: int, group_id: int, type_id: int) -> None:
    """Check and display current group assignment status."""
    groups, error = get_ticket_subitems(
        GLPI_URL, session_token, ticket_id, "Group_Ticket", app_token=APP_TOKEN
    )
    
    if error:
        print(f"Error fetching groups: {error}")
        return
    
    is_assigned = is_group_currently_assigned(groups, group_id, type_id)
    status = "CURRENTLY assigned" if is_assigned else "NOT currently assigned"
    print(f"Group {group_id} is {status} as type {type_id}.")


def analyze_single_group_time(session_token: str, ticket_id: int, group_id: int) -> None:
    """Analyze and display time spent on a ticket by a single group."""
    logs, error = get_ticket_subitems(
        GLPI_URL, session_token, ticket_id, "Log", app_token=APP_TOKEN, range_param='0-999'
    )
    
    if error:
        print(f"Error fetching logs: {error}")
        return
    
    sorted_logs = sorted(logs, key=lambda x: x['date_mod'])
    
    # Find the group info
    all_groups = get_all_groups_from_logs(sorted_logs)
    if group_id not in all_groups:
        print(f"Group {group_id} not found in ticket logs.")
        return
    
    group_info = all_groups[group_id]
    total_duration, _ = calculate_time_spent_for_group(sorted_logs, group_info, verbose=True)
    print(f"\nTotal Time Spent by {group_info}: {total_duration}")


def analyze_all_groups_time(session_token: str, ticket_id: int, verbose: bool = True) -> Dict[int, Dict[str, Any]]:
    """
    Analyze and display time spent on a ticket by all groups.
    
    Args:
        session_token: Valid GLPI session token
        ticket_id: ID of the ticket to analyze
        verbose: Whether to print detailed history
    
    Returns:
        Dictionary with group time analysis results
    """
    logs, error = get_ticket_subitems(
        GLPI_URL, session_token, ticket_id, "Log", app_token=APP_TOKEN, range_param='0-999'
    )
    
    if error:
        print(f"Error fetching logs for ticket {ticket_id}: {error}")
        return {}
    
    results = calculate_all_groups_time(logs, verbose)
    
    if verbose:
        # Summary table
        print("\n" + "=" * 90)
        print(f"SUMMARY - Time Spent by All Groups (Ticket #{ticket_id})")
        print("=" * 90)
        print(f"{'Group ID':<12} {'Group Name':<25} {'Assignments':<15} {'Total Time':<20}")
        print("-" * 90)
        
        for group_id in sorted(results.keys()):
            data = results[group_id]
            group_info = data['group_info']
            assignment_count = data['assignment_count']
            total_time = data['total_duration']
            print(f"{group_id:<12} {group_info.name:<25} {assignment_count:<15} {str(total_time):<20}")
        
        print("=" * 90)
    
    return results


def analyze_multiple_tickets(
    session_token: str, 
    ticket_ids: List[int], 
    verbose: bool = False,
    show_summary: bool = True
) -> Dict[int, Dict[int, Dict[str, Any]]]:
    """
    Analyze time spent across multiple tickets.
    
    Args:
        session_token: Valid GLPI session token
        ticket_ids: List of ticket IDs to analyze
        verbose: Whether to print detailed history for each ticket
        show_summary: Whether to show aggregate summary
    
    Returns:
        Dictionary mapping ticket_id to group analysis results
    """
    all_tickets_results = {}
    group_totals = defaultdict(lambda: {
        'total_duration': datetime.timedelta(),
        'ticket_count': 0,
        'assignment_count': 0,
        'group_info': None
    })
    
    print(f"\n{'=' * 90}")
    print(f"Analyzing {len(ticket_ids)} tickets...")
    print(f"{'=' * 90}")
    
    for ticket_id in ticket_ids:
        print(f"\n>>> Processing Ticket #{ticket_id}")
        
        results = analyze_all_groups_time(session_token, ticket_id, verbose=verbose)
        
        if results:
            all_tickets_results[ticket_id] = results
            
            # Aggregate totals across tickets
            for group_id, data in results.items():
                group_totals[group_id]['total_duration'] += data['total_duration']
                group_totals[group_id]['ticket_count'] += 1
                group_totals[group_id]['assignment_count'] += data['assignment_count']
                if group_totals[group_id]['group_info'] is None:
                    group_totals[group_id]['group_info'] = data['group_info']
        else:
            print(f"No group assignment data found for ticket #{ticket_id}")
    
    # Display aggregate summary
    if show_summary and group_totals:
        print("\n" + "=" * 100)
        print(f"AGGREGATE SUMMARY - All Groups Across {len(ticket_ids)} Tickets")
        print("=" * 100)
        print(f"{'Group ID':<12} {'Group Name':<25} {'Tickets':<10} {'Assignments':<15} {'Total Time':<20}")
        print("-" * 100)
        
        for group_id in sorted(group_totals.keys()):
            data = group_totals[group_id]
            group_info = data['group_info']
            ticket_count = data['ticket_count']
            assignment_count = data['assignment_count']
            total_time = data['total_duration']
            
            print(f"{group_id:<12} {group_info.name:<25} {ticket_count:<10} {assignment_count:<15} {str(total_time):<20}")
        
        print("=" * 100)
    
    return all_tickets_results


def analyze_ticket_range(
    session_token: str, 
    start_id: int, 
    end_id: int, 
    verbose: bool = False,
    show_summary: bool = True,
    skip_errors: bool = True
) -> Dict[int, Dict[int, Dict[str, Any]]]:
    """
    Analyze time spent across a range of ticket IDs.
    
    Args:
        session_token: Valid GLPI session token
        start_id: Starting ticket ID (inclusive)
        end_id: Ending ticket ID (inclusive)
        verbose: Whether to print detailed history for each ticket
        show_summary: Whether to show aggregate summary
        skip_errors: Whether to continue on errors or stop
    
    Returns:
        Dictionary mapping ticket_id to group analysis results
    """
    ticket_ids = list(range(start_id, end_id + 1))
    print(f"Analyzing ticket range: {start_id} to {end_id} ({len(ticket_ids)} tickets)")
    
    return analyze_multiple_tickets(session_token, ticket_ids, verbose, show_summary)


def export_results_to_dict(results: Dict[int, Dict[int, Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Export analysis results to a structured dictionary (for JSON export, etc.).
    
    Args:
        results: Analysis results from analyze_multiple_tickets
    
    Returns:
        Structured dictionary with serializable data
    """
    export_data = {
        'generated_at': datetime.datetime.now().isoformat(),
        'tickets': {}
    }
    
    for ticket_id, groups in results.items():
        export_data['tickets'][ticket_id] = {}
        
        for group_id, data in groups.items():
            group_info = data['group_info']
            export_data['tickets'][ticket_id][group_id] = {
                'group_name': group_info.name,
                'total_duration_seconds': data['total_duration'].total_seconds(),
                'total_duration_str': str(data['total_duration']),
                'assignment_count': data['assignment_count'],
                'periods': [
                    {
                        'assigned': p['assigned'].isoformat(),
                        'unassigned': p['unassigned'].isoformat(),
                        'duration_seconds': p['duration'].total_seconds(),
                        'still_assigned': p.get('still_assigned', False)
                    }
                    for p in data['periods']
                ]
            }
    
    return export_data


def main():
    """Main execution function."""
    try:
        with GLPISession(GLPI_URL, USER, PASSWORD, APP_TOKEN) as session_token:
            print(f"Authenticated. Session: {session_token}\n")
            
            # Example 1: Analyze a single ticket
            print("\n" + "=" * 90)
            print("EXAMPLE 1: Single Ticket Analysis")
            print("=" * 90)
            check_group_assignment(session_token, TICKET_ID, TARGET_GROUP_ID, OBSERVER_TYPE)
            analyze_all_groups_time(session_token, TICKET_ID, verbose=True)
            
            # Example 2: Analyze multiple specific tickets
            print("\n\n" + "=" * 90)
            print("EXAMPLE 2: Multiple Tickets Analysis")
            print("=" * 90)
            ticket_list = [1, 2, 3]
            analyze_multiple_tickets(session_token, ticket_list, verbose=True, show_summary=True)
            
            # Example 3: Analyze a range of tickets
            print("\n\n" + "=" * 90)
            print("EXAMPLE 3: Ticket Range Analysis")
            print("=" * 90)
            analyze_ticket_range(session_token, start_id=1, end_id=5, verbose=False, show_summary=True)
            
            # Example 4: Export results
            results = analyze_multiple_tickets(session_token, [1, 2, 3], verbose=False, show_summary=False)
            export_data = export_results_to_dict(results)
            import json
            with open('ticket_analysis.json', 'w') as f:
                json.dump(export_data, f, indent=2)
            print("Results exported to ticket_analysis.json")
            
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()