import os
import sqlite3
import datetime
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from glpi_methods import authenticate_glpi, get_ticket_subitems, kill_session
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# Configuration
# ============================================================================
GLPI_URL = os.getenv("DEFAULT_GLPI_URL")
APP_TOKEN = os.getenv("DEFAULT_APP_TOKEN")
USER = os.getenv("DEFAULT_USER")
PASSWORD = os.getenv("DEFAULT_PASS")

DB_FILENAME = "glpi_timesolve.db"
GROUP_ASSIGNED_ACTION = 15
GROUP_UNASSIGNED_ACTION = 16
GLPI_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class GroupInfo:
    id: int
    name: str = "Unknown"

@dataclass
class AssignmentPeriod:
    assigned: datetime.datetime
    unassigned: datetime.datetime
    duration: datetime.timedelta
    still_assigned: bool = False

# ============================================================================
# Database Management
# ============================================================================

class DatabaseManager:
    """Handles storage of analysis results into SQLite."""
    
    def __init__(self, db_name=DB_FILENAME):
        self.db_name = db_name
        self.conn = None
        self.init_db()

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def init_db(self):
        """Create necessary tables if they don't exist."""
        self.connect()
        cursor = self.conn.cursor()
        
        # Table to track which tickets we've processed
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_tickets (
                ticket_id INTEGER PRIMARY KEY,
                processed_at TIMESTAMP,
                status TEXT,
                error_message TEXT
            )
        ''')

        # Table to store the summary time for each group per ticket
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_durations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                group_id INTEGER,
                group_name TEXT,
                total_seconds REAL,
                assignment_count INTEGER,
                FOREIGN KEY(ticket_id) REFERENCES processed_tickets(ticket_id)
            )
        ''')

        # Table to store detailed periods (start/end times)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS assignment_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                group_id INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds REAL,
                still_assigned BOOLEAN,
                FOREIGN KEY(ticket_id) REFERENCES processed_tickets(ticket_id)
            )
        ''')
        
        self.conn.commit()

    def is_ticket_processed(self, ticket_id: int) -> bool:
        """Check if a ticket has already been successfully processed."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM processed_tickets WHERE ticket_id = ? AND status = 'SUCCESS'", (ticket_id,))
        return cursor.fetchone() is not None

    def save_results(self, ticket_id: int, results: Dict[int, Dict[str, Any]]):
        """Save analysis results for a single ticket."""
        cursor = self.conn.cursor()
        now = datetime.datetime.now().isoformat()

        try:
            # 1. Record successful processing
            cursor.execute('''
                INSERT OR REPLACE INTO processed_tickets (ticket_id, processed_at, status, error_message)
                VALUES (?, ?, 'SUCCESS', NULL)
            ''', (ticket_id, now))

            # 2. Clear old data for this ticket (to allow re-runs)
            cursor.execute("DELETE FROM group_durations WHERE ticket_id = ?", (ticket_id,))
            cursor.execute("DELETE FROM assignment_periods WHERE ticket_id = ?", (ticket_id,))

            # 3. Insert new data
            for group_id, data in results.items():
                group_info = data['group_info']
                
                # Insert Summary
                cursor.execute('''
                    INSERT INTO group_durations (ticket_id, group_id, group_name, total_seconds, assignment_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (ticket_id, group_id, group_info.name, data['total_duration'].total_seconds(), data['assignment_count']))

                # Insert Details
                for period in data['periods']:
                    cursor.execute('''
                        INSERT INTO assignment_periods (ticket_id, group_id, start_time, end_time, duration_seconds, still_assigned)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        ticket_id, 
                        group_id, 
                        period.assigned.isoformat(), 
                        period.unassigned.isoformat(), 
                        period.duration.total_seconds(), 
                        period.still_assigned
                    ))
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            print(f"DB Error saving ticket {ticket_id}: {e}")
            raise

    def log_error(self, ticket_id: int, error_msg: str):
        """Log a failed ticket processing attempt."""
        cursor = self.conn.cursor()
        now = datetime.datetime.now().isoformat()
        cursor.execute('''
            INSERT OR REPLACE INTO processed_tickets (ticket_id, processed_at, status, error_message)
            VALUES (?, ?, 'ERROR', ?)
        ''', (ticket_id, now, str(error_msg)))
        self.conn.commit()

    def get_max_processed_id(self) -> int:
        """Get the highest ticket ID processed so far."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(ticket_id) FROM processed_tickets")
        result = cursor.fetchone()[0]
        return result if result else 0

# ============================================================================
# Core Logic
# ============================================================================

class GLPISession:
    def __init__(self, url, user, password, app_token=None):
        self.url, self.user, self.password, self.app_token = url, user, password, app_token
        self.session_token = None
    def __enter__(self):
        self.session_token, err = authenticate_glpi(self.url, login=self.user, password=self.password, app_token=self.app_token)
        if not self.session_token: raise ConnectionError(f"Auth failed: {err}")
        return self.session_token
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session_token: kill_session(self.url, self.session_token, app_token=self.app_token)

def parse_glpi_date(date_str):
    return datetime.datetime.strptime(date_str, GLPI_DATE_FORMAT)

def extract_group_info(value: str) -> Optional[GroupInfo]:
    if not value: return None
    try:
        start, end = value.rfind('('), value.rfind(')')
        if start < end and start != -1:
            return GroupInfo(id=int(value[start+1:end]), name=value[:start].strip())
    except: pass
    return None

def calculate_ticket_times(logs: List[Dict]) -> Dict[int, Dict]:
    """Process logs in memory to calculate durations."""
    # Sort once
    logs.sort(key=lambda x: x['date_mod'])
    
    group_states = {} # {group_id: {'start': datetime, 'periods': [], 'info': GroupInfo}}
    
    for log in logs:
        if log.get('itemtype_link') != 'Group': continue
        
        action = int(log.get('linked_action', 0))
        
        if action == GROUP_ASSIGNED_ACTION:
            info = extract_group_info(log.get('new_value', ''))
            if info:
                if info.id not in group_states:
                    group_states[info.id] = {'start': None, 'periods': [], 'info': info}
                
                # Only start if not already started
                if group_states[info.id]['start'] is None:
                    group_states[info.id]['start'] = parse_glpi_date(log['date_mod'])

        elif action == GROUP_UNASSIGNED_ACTION:
            info = extract_group_info(log.get('old_value', ''))
            if info and info.id in group_states and group_states[info.id]['start']:
                start = group_states[info.id]['start']
                end = parse_glpi_date(log['date_mod'])
                group_states[info.id]['periods'].append(AssignmentPeriod(start, end, end - start))
                group_states[info.id]['start'] = None

    # Close open sessions
    now = datetime.datetime.now().replace(microsecond=0)
    results = {}
    
    for gid, state in group_states.items():
        if state['start']:
            duration = now - state['start']
            state['periods'].append(AssignmentPeriod(state['start'], now, duration, still_assigned=True))
        
        total_time = sum((p.duration for p in state['periods']), datetime.timedelta())
        
        if state['periods']: # Only save if there was actual activity
            results[gid] = {
                'group_info': state['info'],
                'total_duration': total_time,
                'periods': state['periods'],
                'assignment_count': len(state['periods'])
            }
            
    return results

def get_current_assignments(logs: List[Dict]) -> Dict[int, Dict[str, Any]]:
    """
    Helper: Get only groups currently assigned to the ticket.
    Calculates duration since the MOST RECENT assignment event.
    Returns: {group_id: {'group_info': GroupInfo, 'duration': timedelta, 'assigned_at': datetime}}
    """
    logs.sort(key=lambda x: x['date_mod'])
    active_groups = {}

    for log in logs:
        if log.get('itemtype_link') != 'Group': continue
        action = int(log.get('linked_action', 0))

        if action == GROUP_ASSIGNED_ACTION:
            info = extract_group_info(log.get('new_value', ''))
            if info:
                # Always update to the latest assignment time
                active_groups[info.id] = {'start': parse_glpi_date(log['date_mod']), 'info': info}
        
        elif action == GROUP_UNASSIGNED_ACTION:
            info = extract_group_info(log.get('old_value', ''))
            if info and info.id in active_groups:
                del active_groups[info.id]

    now = datetime.datetime.now().replace(microsecond=0)
    results = {}
    
    for gid, data in active_groups.items():
        results[gid] = {
            'group_info': data['info'],
            'duration': now - data['start'],
            'assigned_at': data['start']
        }
            
    return results

# ============================================================================
# Main Process
# ============================================================================

def sync_database(start_id: int = 1, end_id: int = 10000, batch_size: int = 50):
    """
    Main routine to scrape the database.
    Resumes from last success, handles errors, saves to SQLite.
    """
    db = DatabaseManager()
    
    # Smart Resume: Check where we left off
    last_id = db.get_max_processed_id()
    if last_id >= start_id:
        print(f"Found existing data. Resuming from Ticket #{last_id + 1}")
        start_id = last_id + 1

    print(f"Starting sync for range {start_id} to {end_id}...")

    try:
        with GLPISession(GLPI_URL, USER, PASSWORD, APP_TOKEN) as session:
            
            for current_id in range(start_id, end_id + 1):
                # Optional: Skip if already done (double check)
                if db.is_ticket_processed(current_id):
                    continue

                print(f"Processing Ticket #{current_id}...", end="\r")
                
                # 1. Fetch Logs
                logs, error = get_ticket_subitems(
                    GLPI_URL, session, current_id, "Log", 
                    app_token=APP_TOKEN, range_param='0-999'
                )

                if error:
                    # If 404 or similar, it might just not exist, log and continue
                    db.log_error(current_id, error)
                    continue

                # 2. Calculate
                if logs:
                    results = calculate_ticket_times(logs)
                    # 3. Save
                    db.save_results(current_id, results)
                else:
                    # Ticket exists but has no logs or empty response
                    db.save_results(current_id, {})

                # Rate limiting / Niceness
                if current_id % batch_size == 0:
                    print(f"Processed up to Ticket #{current_id} - Committing batch.")
                    # SQLite commits on every save_results in this implementation, 
                    # but this print helps user know progress.

    except KeyboardInterrupt:
        print("\nStopping sync... (Progress saved)")
    except Exception as e:
        print(f"\nCritical Error: {e}")
    finally:
        db.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    # Set your desired range here. 
    # You can set end_id very high; the script logs errors for missing tickets.
    sync_database(start_id=19000, end_id=21200)