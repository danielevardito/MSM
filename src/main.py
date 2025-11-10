import os
import sys
import tempfile
import subprocess
import nx_arangodb as nxadb
from dotenv import load_dotenv
from pydantic import ValidationError
from arango.exceptions import ServerConnectionError
import networkx as nx

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

def _print_tree_recursive(tree: nx.DiGraph, node: str, graph: MSMDiGraph, visited: set, prefix: str = "", is_last: bool = True):
    """Recursively print metadata tree structure"""
    if node in visited:
        return
    visited.add(node)
    
    # Parse metadata to get name and category
    try:
        metadata = graph._parse_metadata(node)
        node_display = f"{metadata.name} {Colors.DIM}({metadata.category.value}){Colors.RESET}"
    except:
        node_display = node
    
    # Print current node
    connector = "└── " if is_last else "├── "
    print(f"{prefix}{connector}{Colors.GREEN}{node_display}{Colors.RESET}")
    
    # Get children (successors in the tree)
    children = list(tree.successors(node))
    
    # Print children
    for i, child in enumerate(children):
        is_last_child = (i == len(children) - 1)
        extension = "    " if is_last else "│   "
        _print_tree_recursive(tree, child, graph, visited, prefix + extension, is_last_child)

def _display_metadata_tree(tree: nx.DiGraph, root_key: str, graph: MSMDiGraph):
    """Display metadata tree in a nice hierarchical format"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}Metadata Tree Structure:{Colors.RESET}\n")
    
    # Parse root metadata
    try:
        root_metadata = graph._parse_metadata(root_key)
        root_display = f"{root_metadata.name} {Colors.DIM}({root_metadata.category.value}){Colors.RESET}"
    except:
        root_display = root_key
    
    print(f"{Colors.BOLD}{Colors.YELLOW}🌳 {root_display}{Colors.RESET} {Colors.DIM}(ROOT){Colors.RESET}")
    
    # Get direct children of root
    children = list(tree.successors(root_key))
    visited = {root_key}
    
    for i, child in enumerate(children):
        is_last = (i == len(children) - 1)
        _print_tree_recursive(tree, child, graph, visited, "", is_last)
    
    # Print statistics
    print(f"\n{Colors.CYAN}Tree Statistics:{Colors.RESET}")
    print(f"  {Colors.CYAN}Total nodes:{Colors.RESET}  {tree.number_of_nodes()}")
    print(f"  {Colors.CYAN}Total edges:{Colors.RESET}  {tree.number_of_edges()}")
    print(f"  {Colors.CYAN}Tree depth:{Colors.RESET}  {nx.dag_longest_path_length(tree) if tree.number_of_nodes() > 0 else 0}")

# --- Menu Handlers ---

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

def _handle_add_metadata_tree(graph: MSMDiGraph):
    """Add a metadata tree using BFS"""
    print_header("Add Metadata Tree (BFS)")
    try:
        category = _prompt_category()
        root_name = input_prompt("Root metadata name").strip()
        if not root_name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return

        try:
            root_metadata = Metadata(name=root_name, category=category)
            root_key = graph.insert_freemetadata(root_metadata)
            print_success(f"Root metadata '{root_name}' created.")
        except (KeyError, ValueError, ValidationError) as e:
            print_error(f"Error creating root: {e}")
            print_info("Maybe the root metadata already exists? Try viewing the tree.")
            return
        
        queue = [root_metadata] # Use list as a queue (FIFO)
        
        while queue:
            parent_metadata = queue.pop(0) # Get next parent
            
            children_str = input_prompt(
                f"Enter children for '{Colors.BOLD}{parent_metadata.name}{Colors.RESET}' (comma-separated, or Enter to skip)",
                Colors.GREEN
            ).strip()

            if not children_str:
                continue

            child_names_raw = [n.strip() for n in children_str.split(",") if n.strip()]
            if not child_names_raw:
                continue
            
            # Use a retry loop for the current parent's children
            while True: 
                child_names = [n.strip() for n in children_str.split(",") if n.strip()]
                
                # De-duplicate names entered by user for this batch
                unique_child_names = list(dict.fromkeys(child_names)) # Preserves order
                if len(unique_child_names) != len(child_names):
                    print_warning(f"Duplicate names found in list, using unique: {', '.join(unique_child_names)}")
                    child_names = unique_child_names

                if not child_names:
                    break # No valid names entered after parsing

                child_metadata_list = [Metadata(name=n, category=category) for n in child_names]
                
                # Check for existing metadata
                existing_children = []
                for child_m in child_metadata_list:
                    if graph.is_metadata(graph._format_metdata(child_m)):
                        existing_children.append(child_m.name)
                
                if existing_children:
                    print_error(f"Error: The following metadata already exist: {Colors.BOLD}{', '.join(existing_children)}{Colors.RESET}")
                    print_info("None of the children for this parent were added.")
                    
                    retry_children_str = input_prompt(
                        f"Re-enter children for '{parent_metadata.name}' (or press Enter to skip this parent)"
                    ).strip()
                    
                    if not retry_children_str:
                        break # Break from retry loop, move to next parent in queue
                    else:
                        children_str = retry_children_str
                        continue # Continue to top of retry loop
                
                # All checks passed, insert them
                inserted_children_metadata = []
                try:
                    for child_m in child_metadata_list:
                        graph.insert_metadata(child_m, parent_metadata, category)
                        inserted_children_metadata.append(child_m)
                    
                    if inserted_children_metadata:
                        inserted_names = [m.name for m in inserted_children_metadata]
                        print_success(f"Added children for '{parent_metadata.name}': {', '.join(inserted_names)}")
                        queue.extend(inserted_children_metadata) # Add new children to queue for BFS
                    
                    break # Success, break from retry loop

                except (ValidationError, KeyError, ValueError) as e:
                    # This should be caught by the pre-check, but as a failsafe:
                    print_error(f"Error during batch insertion: {e}")
                    print_info("Aborting insertion for this parent's children.")
                    break # Break from retry loop

        print_info(f"\nMetadata tree insertion complete for root '{root_name}'.")
        print_info("Displaying the new tree...")
        
        # Display the tree
        tree = graph.get_metadata_tree(root_key)
        _display_metadata_tree(tree, root_key, graph)
    
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error adding metadata tree: {e}")
    except Exception as e:
        print_error(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

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

def _handle_get_all_roots(graph: MSMDiGraph):
    """Get all root metadata (metadata without parents)"""
    print_header("All Root Metadata")
    
    try:
        roots = graph.get_all_roots()
        
        if not roots:
            print_warning("No root metadata found.")
            return
        
        print(f"{Colors.BOLD}Found {len(roots)} root metadata:{Colors.RESET}\n")
        
        # Group roots by category
        roots_by_category = {}
        for root_key in roots:
            try:
                metadata = graph._parse_metadata(root_key)
                cat = metadata.category.value
                if cat not in roots_by_category:
                    roots_by_category[cat] = []
                roots_by_category[cat].append(metadata)
            except:
                pass
        
        # Display roots grouped by category
        for category, metadata_list in sorted(roots_by_category.items()):
            print(f"{Colors.BOLD}{Colors.CYAN}{category.upper()}:{Colors.RESET}")
            for metadata in metadata_list:
                print(f"  {Colors.YELLOW}•{Colors.RESET} {Colors.GREEN}{metadata.name}{Colors.RESET}")
            print()
        
        # Offer to view a specific tree
        print()
        view_tree = input_prompt("View tree for a specific root? (y/n)", Colors.GREEN).lower().strip()
        
        if view_tree == 'y':
            root_name = input_prompt("Enter root metadata name").strip()
            if root_name:
                category = _prompt_category()
                metadata = Metadata(name=root_name, category=category)
                metadata_key = f"{metadata.name}-{metadata.category.value}"
                
                if graph.is_metadata(metadata_key):
                    tree = graph.get_metadata_tree(metadata_key)
                    _display_metadata_tree(tree, metadata_key, graph)
                else:
                    print_error(f"Metadata '{root_name}' not found or is not a root")
    
    except Exception as e:
        print_error(f"Error retrieving root metadata: {e}")
        import traceback
        traceback.print_exc()

def _handle_get_metadata_tree(graph: MSMDiGraph):
    """Get and display metadata tree starting from a specific metadata"""
    print_header("View Metadata Tree")
    
    try:
        name = input_prompt("Metadata name (root of tree)").strip()
        if not name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return
        
        category = _prompt_category()
        
        metadata = Metadata(name=name, category=category)
        metadata_key = f"{metadata.name}-{metadata.category.value}"
        
        if not graph.is_metadata(metadata_key):
            print_error(f"Metadata '{name}' with category '{category.value}' not found.")
            return
        
        print_info("Building metadata tree...")
        tree = graph.get_metadata_tree(metadata_key)
        
        _display_metadata_tree(tree, metadata_key, graph)
        
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error retrieving metadata tree: {e}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

def _handle_get_metadata_forest(graph: MSMDiGraph):
    """Get and display the entire metadata forest"""
    print_header("View Metadata Forest (All Trees)")
    
    try:
        print_info("Building metadata forest...")
        forest = graph.get_whole_metadata_forest()
        
        if forest.number_of_nodes() == 0:
            print_warning("The metadata forest is empty. No metadata found.")
            return

        # Find all roots within the generated forest graph
        # These are nodes with an in-degree of 0 IN THE FOREST
        roots_in_forest = [node for node in forest.nodes() if forest.in_degree(node) == 0]

        if not roots_in_forest:
            print_error("Forest has nodes but no roots (cycle detected?). Cannot display.")
            return
        
        print_success(f"Found {forest.number_of_nodes()} total metadata nodes across {len(roots_in_forest)} tree(s).")

        # Display each tree starting from its root
        for i, root_key in enumerate(roots_in_forest):
            # Reuse the existing tree display function
            _display_metadata_tree(forest, root_key, graph)
            if i < len(roots_in_forest) - 1:
                print(f"\n{Colors.DIM}{'-'*60}{Colors.RESET}\n") # Add a separator

    except Exception as e:
        print_error(f"Error retrieving metadata forest: {e}")
        import traceback
        traceback.print_exc()

def _handle_delete_snippet(graph: MSMDiGraph):
    """Delete a snippet"""
    print_header("Delete Snippet")
    print_warning("This action cannot be undone!")
    
    try:
        name = input_prompt("Snippet name to delete (e.g., my_code.py)").strip()
        if not name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return
        
        # First, try to get the snippet to show what will be deleted
        try:
            snippet, metadata_list = graph.get_snippet(name)
            
            print(f"\n{Colors.BOLD}{Colors.YELLOW}Snippet to delete:{Colors.RESET}")
            print(f"{Colors.CYAN}Name:{Colors.RESET}       {snippet.name}")
            print(f"{Colors.CYAN}Extension:{Colors.RESET}  {snippet.extension}")
            print(f"{Colors.CYAN}Created:{Colors.RESET}    {snippet.created_at}")
            
            print(f"\n{Colors.CYAN}Linked Metadata:{Colors.RESET}")
            if metadata_list:
                for meta in metadata_list:
                    print(f"  {Colors.YELLOW}•{Colors.RESET} {meta.name} {Colors.DIM}({meta.category.value}){Colors.RESET}")
            else:
                print(f"  {Colors.DIM}(None){Colors.RESET}")
            
            # Confirmation prompt
            print()
            confirm = input_prompt(
                f"Are you sure you want to delete '{Colors.BOLD}{name}{Colors.RESET}'? (yes/no)", 
                Colors.RED
            ).lower().strip()
            
            if confirm == 'yes':
                graph.delete_snippet(name)
                print_success(f"Snippet '{Colors.BOLD}{name}{Colors.RESET}' deleted successfully!")
            else:
                print_info("Deletion cancelled.")
                
        except (KeyError, ValueError) as e:
            print_error(f"Snippet '{name}' not found: {e}")
            
    except Exception as e:
        print_error(f"Error during deletion: {e}")

def _handle_delete_metadata(graph: MSMDiGraph):
    """Delete metadata and all snippets that only have this metadata"""
    print_header("Delete Metadata")
    print_warning("This action cannot be undone!")
    print_warning("Snippets that have ONLY this metadata will also be deleted!")
    
    try:
        name = input_prompt("Metadata name to delete").strip()
        if not name:
            print_warning("Operation cancelled. Name cannot be empty.")
            return
        
        category = _prompt_category()
        
        metadata = Metadata(name=name, category=category)
        m_key = f"{metadata.name}-{metadata.category.value}"
        
        # Check if metadata exists
        if not graph.is_metadata(m_key):
            print_error(f"Metadata '{name}' with category '{category.value}' not found.")
            return
        
        # Get snippets that will be affected
        snippets = graph._get_snippets_from_metadata(metadata)
        
        print(f"\n{Colors.BOLD}{Colors.YELLOW}Metadata to delete:{Colors.RESET}")
        print(f"{Colors.CYAN}Name:{Colors.RESET}       {metadata.name}")
        print(f"{Colors.CYAN}Category:{Colors.RESET}   {metadata.category.value}")

        snippets_to_delete = []
        snippets_to_keep = []

        if snippets:
            print(f"\n{Colors.CYAN}Linked to {len(snippets)} snippet(s):{Colors.RESET}")
                        
            for snippet_key in snippets:
                try:
                    snippet, _ = graph.get_snippet(snippet_key)
                    # Check if this snippet has only this metadata
                    num_metadata = graph._snippet_metadata_outdegree(snippet_key)
                    if num_metadata == 1:
                        snippets_to_delete.append(snippet.name)
                    else:
                        snippets_to_keep.append(snippet.name)
                except:
                    pass
            
            if snippets_to_delete:
                print(f"\n  {Colors.RED}Will be DELETED (only have this metadata):{Colors.RESET}")
                for sn in snippets_to_delete:
                    print(f"    {Colors.RED}✗{Colors.RESET} {sn}")
            
            if snippets_to_keep:
                print(f"\n  {Colors.GREEN}Will be KEPT (have other metadata too):{Colors.RESET}")
                for sn in snippets_to_keep:
                    print(f"    {Colors.GREEN}✓{Colors.RESET} {sn}")
        else:
            print(f"\n{Colors.DIM}(No snippets linked to this metadata){Colors.RESET}")
        
        # Confirmation prompt
        print()
        confirm = input_prompt(
            f"Are you sure you want to delete metadata '{Colors.BOLD}{name}{Colors.RESET}'? (yes/no)", 
            Colors.RED
        ).lower().strip()
        
        if confirm == 'yes':
            graph.delete_metadata(metadata)
            print_success(f"Metadata '{Colors.BOLD}{name}{Colors.RESET}' deleted successfully!")
            if snippets_to_delete:
                print_success(f"Also deleted {len(snippets_to_delete)} snippet(s) that only had this metadata")
        else:
            print_info("Deletion cancelled.")
            
    except (ValidationError, KeyError, ValueError) as e:
        print_error(f"Error during deletion: {e}")

def _handle_exit(graph: MSMDiGraph):
    """Exit the program"""
    print_success("Goodbye! 👋")
    sys.exit(0)

# --- Menu Definition ---

MENU_ITEMS = [
    # --- Add Operations ---
    {
        "name": "Add a snippet",
        "handler": _handle_add_snippet,
        "icon": "📄"
    },
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
        "name": "Add metadata tree (BFS)",
        "handler": _handle_add_metadata_tree,
        "icon": "🌲"
    },
    
    # --- Snippet Query Operations ---
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
    
    # --- Metadata Query Operations ---
    {
        "name": "View all root metadata",
        "handler": _handle_get_all_roots,
        "icon": "🌱"
    },
    {
        "name": "View metadata tree (from one root)",
        "handler": _handle_get_metadata_tree,
        "icon": "🌳"
    },
    {
        "name": "View metadata forest (All trees)",
        "handler": _handle_get_metadata_forest,
        "icon": "🏞️"
    },

    # --- Delete Operations ---
    {
        "name": "Delete a snippet",
        "handler": _handle_delete_snippet,
        "icon": "🗑️"
    },
    {
        "name": "Delete a metadata",
        "handler": _handle_delete_metadata,
        "icon": "🗑️"
    },

    # --- System ---
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
    
    # Grouping for better readability
    groups = {
        "Create": (0, 4),
        "Query Snippets": (4, 8),
        "Query Metadata": (8, 11),
        "Manage": (11, 13),
        "System": (13, 14)
    }
    
    current_item = 1
    for group_name, (start, end) in groups.items():
        print(f"{Colors.BOLD}{Colors.CYAN}--- {group_name} ---{Colors.RESET}")
        for i in range(start, end):
            item = MENU_ITEMS[i]
            icon = item.get("icon", "•")
            print(f"  {Colors.YELLOW}{current_item}.{Colors.RESET} {icon}  {item['name']}")
            current_item += 1
        print() # Add space between groups

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
        
        # Add a small pause for readability
        if choice.strip() != str(len(MENU_ITEMS)): # Don't pause on exit
            input_prompt("\nPress Enter to continue...", Colors.DIM)

if __name__ == "__main__":
    main_loop()