import os
import sys
import tempfile
import subprocess
import nx_arangodb as nxadb
from dotenv import load_dotenv
from pydantic import ValidationError
from arango.exceptions import ServerConnectionError

from msm_digraph import MSMDiGraph, Metadata, Snippet, Category

# ANSI Color codes
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    
    # Foreground colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # Background colors
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

def print_header(text: str):
    """Print a styled header"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text.center(60)}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}\n")

def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")

def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")

def print_info(text: str):
    """Print info message"""
    print(f"{Colors.BLUE}ℹ {text}{Colors.RESET}")

def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")

def input_prompt(text: str, color=Colors.MAGENTA) -> str:
    """Styled input prompt"""
    return input(f"{color}{Colors.BOLD}➤ {text}{Colors.RESET} ")

def open_vim_editor(initial_content: str = "", extension: str = "txt") -> str:
    """Open vim editor and return the content"""
    with tempfile.NamedTemporaryFile(mode='w+', suffix=f'.{extension}', delete=False) as tf:
        tf.write(initial_content)
        tf.flush()
        temp_path = tf.name
    
    try:
        # Open vim
        subprocess.call(['vim', temp_path])
        
        # Read the content back
        with open(temp_path, 'r') as f:
            content = f.read()
        
        return content
    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

def open_snippet_in_vim_and_update(graph: MSMDiGraph, snippet: Snippet) -> bool:
    """
    Open snippet in vim and update if content changed.
    Returns True if content was modified, False otherwise.
    """
    print_info("Opening vim editor...")
    original_content = snippet.content
    new_content = open_vim_editor(snippet.content, snippet.extension)
    
    # Check if content changed
    if new_content != original_content:
        try:
            graph.update_snippet_content(snippet.name, new_content)
            print_success(f"Snippet '{snippet.name}' updated successfully!")
            return True
        except Exception as e:
            print_error(f"Error updating snippet: {e}")
            return False
    else:
        print_info("No changes detected")
        return False

def setup_database_connection() -> nxadb.DiGraph:
    load_dotenv()

    db_host = os.getenv("DATABASE_HOST", "http://127.0.0.1:8529")
    db_user = os.getenv("DATABASE_USERNAME")
    db_password = os.getenv("DATABASE_PASSWORD", "")
    db_name = os.getenv("DATABASE_NAME")

    if not all([db_host, db_user, db_name]):
        print_error("Environment variables not found")
        print_info("Make sure .env file is in main directory")
        print_info("Execute this script from the main directory (ex: python3 src/main.py)")
        sys.exit(1)

    os.environ["ARANGODB_HOST"] = db_host
    os.environ["ARANGODB_USERNAME"] = db_user
    os.environ["ARANGODB_PASSWORD"] = db_password
    os.environ["ARANGODB_DB_NAME"] = db_name

    print_info(f"Connecting to {db_host} (Database: {db_name}) as user: {db_user}...")

    try:
        adb_graph = nxadb.DiGraph(name=db_name)
        print_success(f"Connected to graph '{db_name}'")
        print_info(f"Current state: {adb_graph.number_of_nodes()} nodes, {adb_graph.number_of_edges()} edges")
        return adb_graph
       
    except ServerConnectionError as e:
        print_error("CONNECTION ERROR")
        print_error(f"Unable to connect to ArangoDB at: {db_host}")
        print_info("Make sure ArangoDB is running and .env credentials are correct")
        print_error(f"Error details: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error during connection: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def _prompt_category() -> Category:
    """Prompt user to select a category"""
    while True:
        cat_options = [c.value for c in Category]
        print(f"\n{Colors.BOLD}Available categories:{Colors.RESET}")
        for i, cat in enumerate(cat_options, 1):
            print(f"  {Colors.YELLOW}{i}.{Colors.RESET} {cat}")
        
        cat_str = input_prompt(f"Select category ({'/'.join(cat_options)})").lower().strip()
        try:
            return Category(cat_str)
        except ValueError:
            print_error(f"'{cat_str}' is not a valid category. Try again")

def _prompt_metadata_list(category: Category) -> list[Metadata]:
    """Prompt user to enter a list of metadata names and return Metadata objects"""
    metadata_names_str = input_prompt("List metadata names (comma-separated, e.g., algorithm, list)")
    metadata_names_list = [n.strip() for n in metadata_names_str.split(",") if n.strip()]

    if not metadata_names_list:
        raise ValueError("At least one metadata name is required.")

    return [Metadata(name=n, category=category) for n in metadata_names_list]

def _display_snippets_summary(snippets_list: list[tuple[Snippet, list[Metadata]]]):
    """Display a summary of snippets with their metadata"""
    if not snippets_list:
        print_warning("No snippets found.")
        return False
    
    print(f"{Colors.BOLD}Found {len(snippets_list)} snippet(s):{Colors.RESET}\n")
    
    for i, (snippet, metadata_list) in enumerate(snippets_list, 1):
        category = metadata_list[0].category.value if metadata_list else "N/A"
        
        print(f"{Colors.YELLOW}{i}.{Colors.RESET} {Colors.BOLD}{snippet.name}{Colors.RESET}")
        print(f"   {Colors.CYAN}Created:{Colors.RESET}    {snippet.created_at}")
        print(f"   {Colors.CYAN}Category:{Colors.RESET}   {category}")
        print(f"   {Colors.CYAN}Metadata:{Colors.RESET}   ", end="")
        
        if not metadata_list:
            print(f"{Colors.DIM}(None){Colors.RESET}")
        else:
            metadata_names = ", ".join([m.name for m in metadata_list])
            print(metadata_names)
        print()
    
    return True

def _interact_with_snippet_list(graph: MSMDiGraph):
    """Allow user to view/edit snippets from the list"""
    while True:
        view_choice = input_prompt("Enter snippet name to view, or 'n' to return to menu", Colors.GREEN).strip()
        
        if view_choice.lower() in ['n', 'no']:
            return
        
        if view_choice:
            try:
                snippet, metadata_list = graph.get_snippet(view_choice)
                
                print(f"\n{Colors.BOLD}{Colors.GREEN}Snippet: {snippet.name}{Colors.RESET}")
                open_snippet_in_vim_and_update(graph, snippet)
                print()
            except (KeyError, ValueError, ValidationError) as e:
                print_error(f"Error retrieving snippet '{view_choice}': {e}\n")

def _display_and_interact_with_snippets(
    graph: MSMDiGraph, 
    snippets_list: list[tuple[Snippet, list[Metadata]]], 
    title: str
):
    """Display snippets and allow user to interact with them"""
    print_header(title)
    
    if _display_snippets_summary(snippets_list):
        _interact_with_snippet_list(graph)

def _handle_add_free_metadata(graph: MSMDiGraph):
    """Add metadata without parent"""
    print_header("Add Metadata (without parent)")
    try:
        name = input_prompt("Metadata name").strip()
        if not name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return

        category = _prompt_category()

        metadata_obj = Metadata(name=name, category=category) 
        key = graph.insert_freemetadata(metadata_obj)
        print_success(f"Metadata created with key: {Colors.BOLD}{key}{Colors.RESET}")
    
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error during creation: {e}")

def _handle_add_metadata(graph: MSMDiGraph):
    """Add metadata with parent"""
    print_header("Add Metadata (with parent)")
    try:
        child_name = input_prompt("Name of new metadata (Child)").strip()
        parent_name = input_prompt("Name of parent metadata (must exist)").strip()
        
        if not child_name or not parent_name:
            print_warning("Operation cancelled. Names cannot be empty.")
            return 
        
        category = _prompt_category()

        child_metadata = Metadata(name=child_name, category=category)
        parent_metadata = Metadata(name=parent_name, category=category)

        graph.insert_metadata(child_metadata, parent_metadata, category)
        print_success(f"Metadata '{Colors.BOLD}{child_name}{Colors.RESET}' created as child of '{Colors.BOLD}{parent_name}{Colors.RESET}'")
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error during creation: {e}")
        print_info("Make sure parent metadata already exists")

def _handle_add_snippet(graph: MSMDiGraph):
    """Add snippet"""
    print_header("Add Snippet")
    try:
        name = input_prompt("Snippet name (e.g., my_code.py)").strip()
        extension = input_prompt("Extension (e.g., py, js, txt, mlw)").strip()
        
        if not all([name, extension]):
            print_warning("Operation cancelled. Name and extension cannot be empty.")
            return
        
        # Open vim for content editing
        print_info("Opening vim editor for snippet content...")
        print_info("(Save and quit vim with :wq when done)")
        input_prompt("Press Enter to open vim", Colors.GREEN)
        
        content = open_vim_editor("", extension)
        
        if not content.strip():
            print_warning("Operation cancelled. Content cannot be empty.")
            return
        
        category = _prompt_category()
        
        snippet_obj = Snippet(name=name, content=content, extension=extension)

        print(f"\n{Colors.BOLD}Now insert metadata to link{Colors.RESET} {Colors.DIM}(must already exist){Colors.RESET}")
        
        try:
            metadata_list = _prompt_metadata_list(category)
        except ValueError as e:
            print_warning(f"Operation cancelled. {e}")
            return

        graph.insert_snippet(snippet_obj, metadata_list, category)
        
        print_success(f"Snippet '{Colors.BOLD}{snippet_obj.name}{Colors.RESET}' inserted and linked to {len(metadata_list)} metadata")
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error during creation: {e}")
        print_info("Make sure you inserted at least one metadata and all metadata already exist")

def _handle_get_snippet(graph: MSMDiGraph):
    """Get snippet"""
    print_header("Get Snippet")
    try:
        name = input_prompt("Snippet name (e.g., my_code.py)").strip()
        if not name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return

        snippet, metadata_list = graph.get_snippet(name)

        # Loop to allow viewing/editing multiple times
        while True:
            print(f"\n{Colors.BOLD}{Colors.GREEN}Snippet Found{Colors.RESET}")
            print(f"{Colors.CYAN}Name:{Colors.RESET}       {snippet.name}")
            print(f"{Colors.CYAN}Extension:{Colors.RESET}  {snippet.extension}")
            print(f"{Colors.CYAN}Created:{Colors.RESET}    {snippet.created_at}")
            
            print(f"\n{Colors.CYAN}Linked Metadata:{Colors.RESET}")
            if not metadata_list:
                print(f"  {Colors.DIM}(None){Colors.RESET}")
            else:
                for meta in metadata_list:
                    print(f"  {Colors.YELLOW}•{Colors.RESET} {meta.name} {Colors.DIM}({meta.category.value}){Colors.RESET}")

            # Ask if user wants to open in vim
            print()
            open_vim = input_prompt("Open snippet in vim? (y/n)", Colors.GREEN).lower().strip()
            
            if open_vim == 'y':
                open_snippet_in_vim_and_update(graph, snippet)
                # Refresh snippet data in case it was modified
                snippet, metadata_list = graph.get_snippet(name)
            else:
                return

    except (KeyError, ValueError, ValidationError) as e:
        print_error(f"Error retrieving snippet: {e}")

def _handle_get_all_snippets(graph: MSMDiGraph):
    """Get all snippets"""
    try:
        all_snippets = graph.get_all_snippets()
        _display_and_interact_with_snippets(graph, all_snippets, "All Snippets")
    except Exception as e:
        print_error(f"Error retrieving snippets: {e}")

def _handle_get_snippets_union(graph: MSMDiGraph):
    """Get snippets containing at least one of the specified metadata (UNION)"""
    print_header("Get Snippets by Metadata (UNION)")
    print_info("Get all snippets that contain AT LEAST ONE of the specified metadata")
    
    try:
        category = _prompt_category()
        
        print(f"\n{Colors.BOLD}Enter metadata names:{Colors.RESET}")
        try:
            metadata_list = _prompt_metadata_list(category)
        except ValueError as e:
            print_warning(f"Operation cancelled. {e}")
            return
        
        snippets = graph.get_snippets_union(metadata_list)
        
        metadata_names = ", ".join([m.name for m in metadata_list])
        _display_and_interact_with_snippets(
            graph, 
            snippets, 
            f"Snippets with ANY of: {metadata_names}"
        )
        
    except Exception as e:
        print_error(f"Error retrieving snippets: {e}")

def _handle_get_snippets_intersection(graph: MSMDiGraph):
    """Get snippets containing all of the specified metadata (INTERSECTION)"""
    print_header("Get Snippets by Metadata (INTERSECTION)")
    print_info("Get all snippets that contain ALL of the specified metadata")
    
    try:
        category = _prompt_category()
        
        print(f"\n{Colors.BOLD}Enter metadata names:{Colors.RESET}")
        try:
            metadata_list = _prompt_metadata_list(category)
        except ValueError as e:
            print_warning(f"Operation cancelled. {e}")
            return
        
        snippets = graph.get_snippets_intersection(metadata_list)
        
        metadata_names = ", ".join([m.name for m in metadata_list])
        _display_and_interact_with_snippets(
            graph, 
            snippets, 
            f"Snippets with ALL of: {metadata_names}"
        )
        
    except Exception as e:
        print_error(f"Error retrieving snippets: {e}")

def _handle_exit(graph: MSMDiGraph):
    """Exit the program"""
    print_success("Goodbye! 👋")
    sys.exit(0)

MENU_ITEMS = [
    {
        "name": "Add a free metadata",
        "handler": _handle_add_free_metadata,
        "icon": "🏷️"
    },
    {
        "name": "Add a metadata with existing parent",
        "handler": _handle_add_metadata,
        "icon": "🔗"
    },
    {
        "name": "Add a snippet",
        "handler": _handle_add_snippet,
        "icon": "📄"
    },
    {
        "name": "Get a snippet by name",
        "handler": _handle_get_snippet,
        "icon": "🔍"
    },
    {
        "name": "View all snippets",
        "handler": _handle_get_all_snippets,
        "icon": "📚"
    },
    {
        "name": "Get snippets by metadata (UNION - at least one)",
        "handler": _handle_get_snippets_union,
        "icon": "∪"
    },
    {
        "name": "Get snippets by metadata (INTERSECTION - all)",
        "handler": _handle_get_snippets_intersection,
        "icon": "∩"
    },
    {
        "name": "Exit",
        "handler": _handle_exit,
        "icon": "🚪"
    }
]

def print_menu():
    """Print dynamic menu"""
    print_header("MSM - Metadata Snippet Manager")
    print(f"{Colors.BOLD}Choose an operation:{Colors.RESET}\n")
    
    for i, item in enumerate(MENU_ITEMS, 1):
        icon = item.get("icon", "•")
        print(f"  {Colors.YELLOW}{i}.{Colors.RESET} {icon}  {item['name']}")
    print()

def main_loop():
    adb_graph = setup_database_connection()
    msm_graph = MSMDiGraph(adb_graph)

    while True:
        print_menu()
        choice = input_prompt(f"Choice (1-{len(MENU_ITEMS)})").strip()

        try:
            choice_num = int(choice)
            if 1 <= choice_num <= len(MENU_ITEMS):
                handler = MENU_ITEMS[choice_num - 1]["handler"]
                handler(msm_graph)
            else:
                print_error(f"'{choice}' is not a valid choice. Please choose 1-{len(MENU_ITEMS)}")
        except ValueError:
            print_error(f"'{choice}' is not a valid number. Please enter a number between 1 and {len(MENU_ITEMS)}")

if __name__ == "__main__":
    main_loop()