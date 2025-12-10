import os
import requests
import base64
from urllib.parse import urlencode
from typing import Optional, Union, List, Dict, Any, Tuple

from dotenv import load_dotenv
load_dotenv()

DEFAULT_USER = os.getenv("DEFAULT_USER")
DEFAULT_PASS = os.getenv("DEFAULT_PASS")
DEFAULT_GLPI_URL = os.getenv("DEFAULT_GLPI_URL")
DEFAULT_APP_TOKEN = os.getenv("DEFAULT_APP_TOKEN")


def get_proxies():
        """Get proxy configuration with authentication"""
        proxy_url = f'http://{DEFAULT_USER}:{DEFAULT_PASS}@192.168.1.1:3128'
        if proxy_url:
            return {
                'http': proxy_url
            }
        else:
            return None

def authenticate_glpi(
    glpi_url: str,
    login: Optional[str] = None,
    password: Optional[str] = None,
    app_token: Optional[str] = None,
    user_token: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Authenticate against GLPI API and return session_token and error message.
    
    Args:
        glpi_url: Base URL of GLPI (e.g., 'http://your-glpi/apirest.php').
        login: GLPI username.
        password: GLPI password.
        app_token: Optional App-Token for API client.
    
    Returns:
        Tuple of (session_token, error_message). session_token is None on failure.
    """
    url = f"{glpi_url}/initSession"
    
    headers = {
        'Content-Type': 'application/json'
    }

    # Basic Auth
    if user_token:
        headers['Authorization'] = f"user_token {user_token}"
    else:
        auth_str = base64.b64encode(f"{login}:{password}".encode()).decode()
        headers['Authorization'] = f"Basic {auth_str}"
    
    if app_token:
        headers['App-Token'] = app_token
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        return data.get('session_token'), None
    
    except requests.exceptions.RequestException as e:
        return None, f"Request failed: {str(e)}"
    except ValueError as e:
        return None, f"Invalid response: {str(e)}"
    except KeyError:
        return None, "Invalid GLPI response format"

def kill_session(
    glpi_url: str,
    session_token: str,
    app_token: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Terminate a GLPI API session.
    
    Args:
        glpi_url: Base URL of GLPI (e.g., 'http://your-glpi/apirest.php').
        session_token: Valid session token from authentication.
        app_token: Optional App-Token for API client.
    
    Returns:
        Tuple of (success, error_message). success is False on failure.
    """
    url = f"{glpi_url}/killSession"
    
    headers = {
        'Content-Type': 'application/json',
        'Session-Token': session_token,
    }

    if app_token:
        headers['App-Token'] = app_token
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return True, None
    
    except requests.exceptions.RequestException as e:
        return False, f"Request failed: {str(e)}"

def get_full_tickets_data(
    glpi_url: str,
    session_token: str,
    search_data: Optional[Dict[str, Any]] = None,
    ticket_ids: Optional[Union[int, List[int]]] = None,
    expand_dropdowns: bool = True,
    app_token: Optional[str] = None
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Retrieve full details for tickets from GLPI search results or specified IDs.
    
    Args:
        glpi_url: Base URL of GLPI (e.g., 'http://your-glpi/apirest.php').
        session_token: Valid session token from authentication.
        search_data: Optional Dict from search results (e.g., from get_tickets_from_saved_search).
                     If provided, extracts ticket IDs from 'data' rows using key '2'.
        ticket_ids: Optional single int or List[int] of ticket IDs to fetch.
                    If provided, uses these directly (converts single int to list).
        expand_dropdowns: Whether to expand dropdown fields (default: True).
        app_token: Optional App-Token for API client.
    
    Returns:
        Tuple of (tickets_list, error_message). tickets_list is None on failure.
        Each item in tickets_list is the full ticket dict from GLPI.
    
    Notes:
        - Provide either search_data or ticket_ids, but not both (prioritizes ticket_ids if both given).
        - If neither is provided, returns empty list (no error).
    """
    headers = {
        'Content-Type': 'application/json',
        'Session-Token': session_token,
        'X-GLPI-Sanitized-Content': 'false',
    }

    if app_token:
        headers['App-Token'] = app_token
    
    # Determine ticket_ids
    if ticket_ids is not None:
        if isinstance(ticket_ids, int):
            ticket_ids_list = [ticket_ids]
        else:
            ticket_ids_list = list(ticket_ids)
    elif search_data is not None:
        data_rows = search_data.get('data', [])
        ticket_ids_list = []
        for row in data_rows:
            tid = row.get('2')  # Search option 2 is typically the ID
            if tid:
                ticket_ids_list.append(int(tid))
    else:
        return [], None  # No source provided, return empty
    
    if not ticket_ids_list:
        return None, "No ticket IDs found in provided data"
    
    # Build query string for additional params
    additional_params = {
        'expand_dropdowns': str(expand_dropdowns).lower()
    }
    full_query_str = urlencode(additional_params)
    
    tickets = []
    for tid in ticket_ids_list:
        ticket_url = f"{glpi_url}/Ticket/{tid}?{full_query_str}"
        try:
            resp = requests.get(ticket_url, headers=headers)
            resp.raise_for_status()
            ticket_data = resp.json()
            tickets.append(ticket_data)
        except requests.exceptions.RequestException as e:
            # Log per-ticket error but continue; could collect errors if needed
            print(f"Failed to fetch ticket {tid}: {str(e)}")
            continue
        except ValueError as e:
            print(f"Invalid response for ticket {tid}: {str(e)}")
            continue
    
    if not tickets:
        return None, "Failed to fetch any ticket details"
    
    return tickets, None

def get_ticket_subitems(
    glpi_url: str,
    session_token: str,
    ticket_id: int,
    sub_itemtype: str,
    expand_dropdowns: bool = True,
    app_token: Optional[str] = None,
    range_param: str = '0-49'
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Retrieve sub-items (e.g., ITILFollowup, ITILSolution, Item_Ticket) for a specific ticket.
    
    Args:
        glpi_url: Base URL of GLPI (e.g., 'http://your-glpi/apirest.php').
        session_token: Valid session token from authentication.
        ticket_id: ID of the ticket.
        sub_itemtype: The sub-item type (e.g., 'ITILFollowup', 'ITILSolution', 'Item_Ticket').
        app_token: Optional App-Token for API client.
        range_param: Pagination range (default: '0-49').
    
    Returns:
        Tuple of (subitems_list, error_message). subitems_list is None on failure.
    """
    headers = {
        'Content-Type': 'application/json',
        'Session-Token': session_token,
        'X-GLPI-Sanitized-Content': 'false',
    }

    if app_token:
        headers['App-Token'] = app_token
    
    url = f"{glpi_url}/Ticket/{ticket_id}/{sub_itemtype}"
    
    params = {'range': range_param} if range_param else {}
    if expand_dropdowns:
        params['expand_dropdowns'] = str(expand_dropdowns).lower()
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Assuming the response is a list of dicts directly; adjust if wrapped (e.g., data.get('data'))
        if isinstance(data, list):
            return data, None
        elif isinstance(data, dict) and 'data' in data:
            return data['data'], None
        else:
            return None, "Unexpected response format for sub-items"
    
    except requests.exceptions.RequestException as e:
        return None, f"Request failed: {str(e)}"
    except ValueError as e:
        return None, f"Invalid response: {str(e)}"
