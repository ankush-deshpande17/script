#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import json
from datetime import datetime
import requests
import shutil

# ANSI color codes for terminal
YELLOW = "\033[93m"
RESET = "\033[0m"

# Script information
SCRIPT_VERSION = "v1.0.0"  # Update this with each release
GITHUB_REPO = "ankush-deshpande17/script"
GITHUB_BRANCH = "main"
VERSION_FILE = "latest_version.txt"

# Updated ASCII Banner
BANNER = """
==========================================================================================

                   ██████  ██████  ██████                                               
                   ██      ██  ██  ██                                                   
                   ██████  ██████  ██████                                               
                       ██  ██  ██      ██                                               
                   ██████  ██  ██  ██████                                               

         SAS Viya 4 Troubleshooting and Validation Script                                

==========================================================================================
"""

# Updated Index with new step
INDEX = """
==========================================================================================
          S T E P S   O F   T H E   A U T O M A T I O N   P R O C E S S
==========================================================================================
  1. [🔑] Get Namespace from User Input
  2. [📋] List All Pods in the Namespace
  3. [✅] SAS Readiness Check
  4. [💻] List Nodes and Their Utilization (Overview)
  5. [📊] Node Resource Utilization (Reserved Resources)
  6. [🚨] Check Pods for Errors
  7. [📈] Pod Resource Utilization (Actual vs Limits)
==========================================================================================
"""

# HTML components
HTML_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SAS Viya 4 Troubleshooting Report - {namespace}</title>
    <style>
        body {{font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; color: #333;}}
        h1 {{text-align: center; color: #2c3e50; font-size: 2.5em; margin-bottom: 20px;}}
        h2 {{color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 5px; margin-top: 40px;}}
        table {{width: 100%; border-collapse: collapse; margin: 20px 0; box-shadow: 0 2px 5px rgba(0,0,0,0.1); background-color: #fff;}}
        th, td {{padding: 12px 15px; text-align: left; border: 1px solid #ddd;}}
        th {{background-color: #3498db; color: white; font-weight: bold;}}
        tr:nth-child(even) {{background-color: #f9f9f9;}}
        tr.highlight {{background-color: #f1c40f; color: #333;}}
        td.high-usage {{background-color: #e74c3c; color: white;}}
        pre {{background-color: #ecf0f1; padding: 15px; border-radius: 5px; white-space: pre-wrap; margin: 10px 0;}}
        .container {{max-width: 1200px; margin: 0 auto;}}
        .timestamp {{text-align: center; color: #7f8c8d; font-size: 0.9em; margin-top: 10px;}}
    </style>
</head>
<body>
    <div class="container">
        <h1>SAS Viya 4 Troubleshooting Report - {namespace}</h1>
        <div class="timestamp">Generated on: {timestamp}</div>
"""

HTML_FOOT = """
    </div>
</body>
</html>
"""

def run_command(command):
    """Execute a shell command and return its output."""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command '{command}': {e}")
        return None

def print_table(headers, rows):
    """Print a table with properly aligned columns."""
    if not rows:
        print("No data to display in table.")
        return
    
    col_widths = [max(len(str(header)), max(len(str(row[i])) for row in rows)) for i, header in enumerate(headers)]
    horizontal_border = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_row = "|" + "|".join(f" {h:<{w}} " for h, w in zip(headers, col_widths)) + "|"
    
    print(horizontal_border)
    print(header_row)
    print(horizontal_border)
    
    for row in rows:
        formatted_row = "|" + "|".join(f" {str(item):<{w}} " for item, w in zip(row, col_widths)) + "|"
        print(formatted_row)
    
    print(horizontal_border)

def check_for_updates():
    """Check for a newer version of the script on GitHub and update if requested."""
    repo_url = f"https://github.com/{GITHUB_REPO}"
    version_file_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{VERSION_FILE}"
    script_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/viya4_environment_troubleshooting_v1.py"

    try:
        version_response = requests.get(version_file_url, timeout=5)
        version_response.raise_for_status()
        latest_version = version_response.text.strip()

        current_version = SCRIPT_VERSION.lstrip('v')

        latest_ver_num = int(''.join(latest_version.split('.')))
        current_ver_num = int(''.join(current_version.split('.')))

        if latest_ver_num > current_ver_num:
            print(f"{YELLOW}WARNING: A new version is available! (v{latest_version} vs current {SCRIPT_VERSION}){RESET}")
            print(f"Download the latest version from: {repo_url}")
            update = input(f"{YELLOW}Do you want to update the script now? (y/n): {RESET}").strip().lower()

            if update in ('y', 'yes'):
                script_response = requests.get(script_url, timeout=5)
                script_response.raise_for_status()
                latest_script = script_response.text

                script_path = os.path.realpath(__file__)

                if not os.access(script_path, os.W_OK):
                    print(f"{YELLOW}Update requires write permissions to {script_path}.{RESET}")
                    print(f"{YELLOW}Attempting to update with sudo...{RESET}")
                    temp_path = "/tmp/viya4_environment_troubleshooting_v1.py.tmp"
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        f.write(latest_script)
                    try:
                        subprocess.run(['sudo', 'mv', temp_path, script_path], check=True)
                        subprocess.run(['sudo', 'chmod', '+x', script_path], check=True)
                        print("INFO: Script updated successfully. Please re-run the script.")
                        sys.exit(0)
                    except subprocess.CalledProcessError as e:
                        print(f"ERROR: Update failed: {e}")
                        print(f"INFO: Please update manually from {repo_url}")
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                else:
                    temp_path = script_path + ".tmp"
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        f.write(latest_script)
                    try:
                        shutil.move(temp_path, script_path)
                        os.chmod(script_path, 0o755)
                        print("INFO: Script updated successfully. Restarting...")
                        os.execv(sys.executable, [sys.executable] + sys.argv)
                    except Exception as e:
                        print(f"ERROR: Failed to update script: {e}")
                        print(f"INFO: Please update manually from {repo_url}")
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
        else:
            print(f"Script is up to date (version {SCRIPT_VERSION}).")
    except requests.RequestException as e:
        print(f"Failed to check for updates: {e}")
        print(f"INFO: You can manually check the latest version at {repo_url}")
    except Exception as e:
        print(f"Unexpected error during update check: {e}")

# Remaining functions (unchanged, omitted for brevity)
def get_namespace(): ...
def list_pods(namespace, html_data): ...
def sas_readiness_check(namespace, html_data): ...
def list_nodes_and_utilization(namespace, html_data): ...
def parse_resource_value(value, is_cpu=False): ...
def node_resource_utilization(namespace, html_data): ...
def check_pods_for_errors(namespace, html_data): ...
def pod_resource_utilization(namespace, html_data): ...
def generate_html(namespace, html_data): ...

def main():
    print("Checking for script updates...")
    check_for_updates()
    
    print(BANNER)
    print(INDEX)
    
    print("1. [🔑] Get Namespace from User Input")
    namespace = get_namespace()
    print(f"Using namespace: {namespace}")
    
    html_data = {}
    list_pods(namespace, html_data)
    sas_readiness_check(namespace, html_data)
    list_nodes_and_utilization(namespace, html_data)
    node_resource_utilization(namespace, html_data)
    check_pods_for_errors(namespace, html_data)
    pod_resource_utilization(namespace, html_data)
    generate_html(namespace, html_data)
    
    print("\nTroubleshooting complete!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript terminated by user.")
        sys.exit(0)
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)
