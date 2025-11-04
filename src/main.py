import os
import sys
import nx_arangodb as nxadb
from dotenv import load_dotenv
from pydantic import ValidationError
from arango.exceptions import ServerConnectionError

from msm_digraph import MSMDiGraph, Metadata, Snippet, Category

def setup_database_connection() -> nxadb.DiGraph:
    load_dotenv()

    db_host = os.getenv("DATABASE_HOST", "http://127.0.0.1:8529")
    db_user = os.getenv("DATABASE_USERNAME")
    db_password = os.getenv("DATABASE_PASSWORD", "")
    db_name = os.getenv("DATABASE_NAME")

    if not all([db_host, db_user, db_name]):
      print("--- Error: env variables not found ---", file=sys.stderr)
      print("Be sure .env file is in main directory", file=sys.stderr)
      print("Execute this script from the main directory (ex: python3 src/main.py)", file=sys.stderr)
      sys.exit(1)

    os.environ["ARANGODB_HOST"] = db_host
    os.environ["ARANGODB_USERNAME"] = db_user
    os.environ["ARANGODB_PASSWORD"] = db_password
    os.environ["ARANGODB_DB_NAME"] = db_name

    print(f"Trying to connect to {db_host}, (Database: {db_name}) as user: {db_user}...")

    try:
       adb_graph = nxadb.DiGraph(name=db_name)

       print(f"Connection to graph '{db_name}' done.")
       print(f"Current state: {adb_graph.number_of_nodes()} nodes, {adb_graph.number_of_edges()} edges")
       
       return adb_graph
       
    except ServerConnectionError as e:
       print("\n--- CONNECTION ERROR ---", file=sys.stderr)
       print(f"Impossible to connect to ArangoDB at: {db_host}", file=sys.stderr)
       print("Make sure ArangoDB is executing and .env credentials are correct", file=sys.stderr)
       print(f"Error details: {e}", file=sys.stderr)
       sys.exit(1)
    except Exception as e:
       print(f"Error during connection: {e}", file=sys.stderr)
       import traceback
       traceback.print_exc()
       sys.exit(1)

def _prompt_category() -> Category:
    while True:
        cat_options = [c.value for c in Category]
        cat_str = input(f"Insert category ({'/'.join(cat_options)}): ").lower().strip()
        try:
            return Category(cat_str)
        except ValueError:
            print(f"Error: '{cat_str}' not a valid category. Try again")


def _handle_add_free_metadata(graph: MSMDiGraph):
    print("\n--- 1. Add Metadata (without parent) ---")
    try:
        name = input("Metadata name: ").strip()
        if not name:
            print("Operation cancelled. Name can't be empty.")
            return

        category = _prompt_category()

        metadata_obj = Metadata(name=name, category=category) 
        key = graph.insert_freemetadata(metadata_obj)
        print(f"\nSuccess! Metadata created with key: {key}")
    
    except (ValidationError, KeyError, ValueError) as e:
       print(f"\nError during creation: {e}")

def _handle_add_metadata(graph: MSMDiGraph):
    print("\n--- 2. Add metadata (with parent) ---")
    try:
        child_name = input("Name of new metadata (Child): ").strip()
        parent_name = input("Name of metadata parent (it must exist): ").strip()
        
        if not child_name or not parent_name:
           print("Operation cancelled. Name can't be empty.")
           return 
        
        category = _prompt_category()

        child_metadata = Metadata(name=child_name, category=category)
        parent_metadata = Metadata(name=parent_name, category=category)

        graph.insert_metadata(child_metadata, parent_metadata, category)
        print(f"\nSuccess! Metadata '{child_name}' created as child of '{parent_name}'.")
    except (ValidationError, KeyError, ValueError) as e:
        print(f"\nError during creation: {e}")
        print("Make sure metadata 'parent' already exists")

def _handle_add_snippet(graph: MSMDiGraph):
    print("\n--- 3. Add Snippet ---")
    try:
        name = input("Snippet name (e.g., my_code.py): ").strip()
        content = input("Snippet content: ").strip()
        extension = input("Extension (e.g., py, js, txt, mlw): ").strip()
        category = _prompt_category()

        if not all([name, content, extension]):
           print("Operation cancelled. All fields mustn't be empty.")
           return 
        
        snippet_obj = Snippet(name=name, content=content, extension=extension)

        print("\nNow insert metadata to link (they have to be already present).")
        metadata_names_str = input("List metadata names (separated by commas, e.g., algorithm, list): ")
        metadata_names_list = [n.strip() for n in metadata_names_str.split(",") if n.strip()]

        metadata_list = [Metadata(name=n, category=category) for n in metadata_names_list]
        graph.insert_snippet(snippet_obj, metadata_list, category)
        
        print(f"\nSuccess! Snippet '{snippet_obj.name}' inserted and linked to {len(metadata_list)} metadata.")
    except (ValidationError, KeyError, ValueError) as e:
        print(f"\nError during creation: {e}")
        print("Make sure you inserted at least one metadata and all metadata inserted already exist.")
        
def print_menu():
   """Print menu"""
   print("\n--- MSM - Metadata Snippet Manager ---")
   print("Choose an operation:")
   print("  1. Add a free metadata")
   print("  2. Add a metadata with existing parent")
   print("  3. Add a snippet")
   print("  4. Exit")

def main_loop():
 adb_graph = setup_database_connection()
 msm_graph = MSMDiGraph(adb_graph)

 while True:
    print_menu()
    choice = input("Choice (1-4): ").strip()

    if choice == '1':
       _handle_add_free_metadata(msm_graph)
    elif choice == '2':
       _handle_add_metadata(msm_graph)
    elif choice == '3':
       _handle_add_snippet(msm_graph)
    elif choice == '4':
       print("Bye")
       break
    else: 
       print(f"'{choice}' not a valid choice. Try again")

if __name__ == "__main__":
    main_loop()