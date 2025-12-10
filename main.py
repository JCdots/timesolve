import os
import datetime
from typing import Optional, List, Dict, Any
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


def is_group_currently_assigned(groups: List[Dict[str, Any]], group_id: int, type_id: int) -> bool:
    """Check if a specific group is assigned with a specific type."""
    return any(
        int(g.get('groups_id', 0)) == group_id and int(g.get('type', 0)) == type_id
        for g in groups
    )


def is_group_action(log: Dict[str, Any], group_id: int, action: int) -> bool:
    """Check if log entry is a group action for the target group."""
    group_identifier = f"({group_id})"
    
    if log.get('itemtype_link') != 'Group' or int(log.get('linked_action', 0)) != action:
        return False
    
    value_field = 'new_value' if action == GROUP_ASSIGNED_ACTION else 'old_value'
    return group_identifier in log.get(value_field, '')


def calculate_time_spent(logs: List[Dict[str, Any]], group_id: int) -> datetime.timedelta:
    """
    Calculate total time spent by analyzing assignment/unassignment logs.
    
    Returns:
        Total duration the group was assigned.
    """
    # Sort logs chronologically (oldest first)
    sorted_logs = sorted(logs, key=lambda x: x['date_mod'])
    
    total_duration = datetime.timedelta()
    last_assign_date = None
    is_assigned = False
    
    print("\n--- Assignment History ---")
    
    for log in sorted_logs:
        date_mod = parse_glpi_date(log.get('date_mod'))
        
        # Handle assignment
        if is_group_action(log, group_id, GROUP_ASSIGNED_ACTION):
            if not is_assigned:
                last_assign_date = date_mod
                is_assigned = True
                print(f"[{format_datetime(date_mod)}] Assigned")
        
        # Handle unassignment
        elif is_group_action(log, group_id, GROUP_UNASSIGNED_ACTION):
            if is_assigned and last_assign_date:
                duration = date_mod - last_assign_date
                total_duration += duration
                is_assigned = False
                last_assign_date = None
                print(f"[{format_datetime(date_mod)}] Unassigned (Duration: {duration})")
    
    # Handle currently assigned case
    if is_assigned and last_assign_date:
        now = datetime.datetime.now().replace(microsecond=0)
        duration = now - last_assign_date
        total_duration += duration
        print(f"[{format_datetime(now)}] Still Assigned (Current stint: {duration})")
    
    return total_duration


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


def analyze_ticket_time(session_token: str, ticket_id: int, group_id: int) -> None:
    """Analyze and display time spent on a ticket by a group."""
    logs, error = get_ticket_subitems(
        GLPI_URL, session_token, ticket_id, "Log", app_token=APP_TOKEN, range_param='0-999'
    )
    
    if error:
        print(f"Error fetching logs: {error}")
        return
    
    total_duration = calculate_time_spent(logs, group_id)
    print(f"\nTotal Time Spent: {total_duration}")


def main():
    """Main execution function."""
    try:
        with GLPISession(GLPI_URL, USER, PASSWORD, APP_TOKEN) as session_token:
            print(f"Authenticated. Session: {session_token}\n")
            
            # Check current assignment status
            check_group_assignment(session_token, TICKET_ID, TARGET_GROUP_ID, OBSERVER_TYPE)
            
            # Analyze time spent
            analyze_ticket_time(session_token, TICKET_ID, TARGET_GROUP_ID)
            
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()