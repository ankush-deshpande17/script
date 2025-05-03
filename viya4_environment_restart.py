#!/usr/bin/env python3

import os
import sys
import subprocess
import getpass
from datetime import datetime
import re
import time
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Version and GitHub settings
SCRIPT_VERSION = "v1.0.0"  # Current version
GITHUB_REPO = "ankush-deshpande17/script"
GITHUB_BRANCH = "main"
VERSION_FILE = "restart_version.txt"

def check_for_updates():
    """Check for script updates from GitHub"""
    version_file_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{VERSION_FILE}"
    try:
        response = requests.get(version_file_url, timeout=5)
        response.raise_for_status()
        latest_version = response.text.strip()
        current_version = SCRIPT_VERSION.lstrip('v')
        latest_ver_num = int(''.join(latest_version.split('.')))
        current_ver_num = int(''.join(current_version.split('.')))
        return latest_ver_num > current_ver_num, latest_version
    except Exception as e:
        logger.warning(f"Could not check for updates: {e}")
        return False, None

def update_script():
    """Update the script from GitHub"""
    script_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/viya4_environment_restart.py"
    script_path = os.path.realpath(__file__)
    try:
        response = requests.get(script_url, timeout=5)
        response.raise_for_status()
        latest_script = response.text
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(latest_script)
        os.chmod(script_path, 0o755)
        return True
    except Exception as e:
        logger.error(f"Failed to update script: {e}")
        return False

def print_step_header(step_num, step_name, icon):
    """Print formatted step header with index, name, and icon"""
    print(f"\n{'=' * 50}")
    print(f"Step {step_num}: {icon} {step_name}")
    print(f"{'=' * 50}")

def parse_ci(ci):
    """Step 0: Parse Configuration Item to extract NS, AKSN, and RGN"""
    print_step_header(0, "Parse Configuration Item", "📋")
    
    match = re.match(r'^([A-Z]{3})_.*_VIYA4_([A-Z]+)$', ci)
    if not match:
        print(f"❌ Error: Invalid Configuration Item format: {ci}")
        sys.exit(1)
    
    tla, env = match.groups()
    ns = f"{tla.lower()}{env.lower()}"
    aksn = f"{tla.lower()}-{env.lower()}"
    rgn = aksn
    
    env_vars = {
        'NS': ns,
        'AKSN': aksn,
        'RGN': rgn
    }
    
    print("✅ Environment Variables Initialized:")
    for key, value in env_vars.items():
        print(f"  {key}: {value}")
    
    return env_vars

def setup_zabbix_maintenance(ci, ticket, duration):
    """Step 1: Setup Zabbix Maintenance"""
    print_step_header(1, "Setup Zabbix Maintenance", "🛠️")
    
    zabbix_script = "/home/anzdes/viya-upgrade-scripts/zabbixClient-v2.0"
    
    if not os.path.isfile(zabbix_script):
        print(f"❌ Error: Zabbix script not found at {zabbix_script}")
        sys.exit(1)
    if not os.access(zabbix_script, os.X_OK):
        print(f"❌ Error: Zabbix script at {zabbix_script} is not executable")
        sys.exit(1)
    
    if not duration.endswith('h'):
        duration = f"{duration}h"
        print(f"⚠️ Adjusted duration to: {duration}")
    
    cmd = [
        zabbix_script,
        "create",
        f"--configurationitem={ci}",
        f"--ticketnumber={ticket}",
        f"--duration={duration}"
    ]
    
    print(f"🔍 Executing Zabbix maintenance command: {' '.join(cmd)}")
    
    vsp_user = input("👤 Enter VSP userid: ")
    vsp_pass = getpass.getpass("🔑 Enter VSP Password: ")
    
    log_file = f"/tmp/zabbix_{ticket}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    print(f"📝 Logging Zabbix command output to: {log_file}")
    
    timeout_seconds = 60
    
    try:
        print("🚀 Starting Zabbix command execution...")
        with open(log_file, 'w') as log:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            process.stdin.write(f"{vsp_user}\n")
            process.stdin.write(f"{vsp_pass}\n")
            process.stdin.flush()
            
            start_time = time.time()
            stdout_lines = []
            stderr_lines = []
            
            while process.poll() is None:
                if time.time() - start_time > timeout_seconds:
                    process.terminate()
                    print(f"❌ Error: Zabbix command timed out after {timeout_seconds} seconds")
                    sys.exit(1)
                
                stdout_line = process.stdout.readline()
                if stdout_line:
                    print(stdout_line.strip())
                    log.write(stdout_line)
                    stdout_lines.append(stdout_line)
                
                stderr_line = process.stderr.readline()
                if stderr_line:
                    print(stderr_line.strip())
                    log.write(stderr_line)
                    stderr_lines.append(stderr_line)
                
                time.sleep(0.1)
            
            stdout, stderr = process.communicate()
            if stdout:
                print(stdout.strip())
                log.write(stdout)
                stdout_lines.append(stdout)
            if stderr:
                print(stderr.strip())
                log.write(stderr)
                stderr_lines.append(stderr)
            
            combined_output = (''.join(stdout_lines) + ''.join(stderr_lines)).lower()
            if process.returncode != 0:
                if "maintenance" in combined_output and "already exists" in combined_output:
                    print(f"⚠️ Warning: Maintenance for ticket {ticket} already exists. Skipping Zabbix maintenance setup.")
                    return
                else:
                    print(f"❌ Error in Zabbix maintenance: {''.join(stderr_lines) or ''.join(stdout_lines)}")
                    sys.exit(1)
        
        print("✅ Zabbix maintenance setup completed")
    
    except Exception as e:
        print(f"❌ Failed to setup Zabbix maintenance: {str(e)}")
        sys.exit(1)

def list_running_pods(ns):
    """Step 2: List Currently Running Pods"""
    print_step_header(2, "List Running Pods", "📜")
    
    cmd = ["kubectl", "get", "pods", "-n", ns]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("\n📋 Currently running pods:")
        print(result.stdout)
        if result.returncode != 0:
            print(f"❌ Error listing pods: {result.stderr}")
            sys.exit(1)
        print("✅ Pod listing completed")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to list pods: {e}")
        sys.exit(1)

def collect_container_log(ns, pod_name, container_name, log_file, is_init=False):
    """Helper function to collect logs for a single container"""
    log_cmd = ["kubectl", "logs", "-n", ns, pod_name, "-c", container_name]
    container_type = "init container" if is_init else "container"
    try:
        with open(log_file, 'w') as f:
            subprocess.run(log_cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        return (container_name, True, None)
    except subprocess.CalledProcessError as e:
        return (container_name, False, f"Failed to collect logs for {container_type} {container_name}: {e.stderr}")

def create_backup_logs(ns, ticket):
    """Step 3: Create Backup of Logs"""
    print_step_header(3, "Create Backup of Logs", "💾")
    
    # Start timing
    start_time = time.time()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    user = getpass.getuser()
    backup_dir = f"/home/{user}/viya4_environment_restart/tla/{ns}/{ticket}/{timestamp}"
    
    print(f"📁 Creating backup directory: {backup_dir}")
    
    try:
        os.makedirs(backup_dir, exist_ok=True)
        if not os.access(backup_dir, os.W_OK):
            print(f"❌ Error: Backup directory {backup_dir} is not writable")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error creating backup directory: {str(e)}")
        sys.exit(1)
    
    cmd = ["kubectl", "get", "pods", "-n", ns, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error listing pods: {result.stderr}")
            sys.exit(1)
        
        pods_data = json.loads(result.stdout)
        pods = pods_data.get("items", [])
        running_pods = [pod for pod in pods if pod["status"].get("phase") == "Running"]
        print(f"📥 Collecting logs for {len(running_pods)} running pods (out of {len(pods)} total pods)...")
        
        for pod in running_pods:
            pod_name = pod["metadata"]["name"]
            container_statuses = pod["status"].get("containerStatuses", [])
            init_container_statuses = pod["status"].get("initContainerStatuses", [])
            
            pod_dir = os.path.join(backup_dir, pod_name)
            os.makedirs(pod_dir, exist_ok=True)
            
            containers = [container["name"] for container in pod["spec"].get("containers", [])]
            init_containers = [container["name"] for container in pod["spec"].get("initContainers", [])]
            
            log_tasks = []
            init_results = []
            regular_results = []
            
            for container_name in containers:
                container_state = None
                for status in container_statuses:
                    if status["name"] == container_name:
                        container_state = status.get("state", {})
                        break
                
                if not container_state:
                    print(f"⚠️ Skipping container {container_name} in pod {pod_name}: No state information")
                    regular_results.append((container_name, False, "No state information"))
                    continue
                
                state_folder = None
                if "running" in container_state:
                    state_folder = "Running/logs"
                elif "terminated" in container_state:
                    reason = container_state["terminated"].get("reason", "Unknown")
                    if reason == "Completed":
                        state_folder = "Terminated/logs"
                    else:
                        state_folder = "Error/logs"
                elif "waiting" in container_state:
                    print(f"⚠️ Skipping container {container_name} in pod {pod_name}: Waiting state")
                    regular_results.append((container_name, False, "Waiting state"))
                    continue
                else:
                    print(f"⚠️ Skipping container {container_name} in pod {pod_name}: Unsupported state {container_state}")
                    regular_results.append((container_name, False, f"Unsupported state {container_state}"))
                    continue
                
                log_dir = os.path.join(pod_dir, state_folder)
                os.makedirs(log_dir, exist_ok=True)
                
                log_file = os.path.join(log_dir, f"{container_name}.log")
                log_tasks.append((ns, pod_name, container_name, log_file, False))
            
            for container_name in init_containers:
                container_state = None
                for status in init_container_statuses:
                    if status["name"] == container_name:
                        container_state = status.get("state", {})
                        break
                
                if not container_state:
                    print(f"⚠️ Skipping init container {container_name} in pod {pod_name}: No state information")
                    init_results.append((container_name, False, "No state information"))
                    continue
                
                state_folder = None
                if "running" in container_state:
                    state_folder = "Running/logs"
                elif "terminated" in container_state:
                    reason = container_state["terminated"].get("reason", "Unknown")
                    if reason == "Completed":
                        state_folder = "Terminated/logs"
                    else:
                        state_folder = "Error/logs"
                elif "waiting" in container_state:
                    print(f"⚠️ Skipping init container {container_name} in pod {pod_name}: Waiting state")
                    init_results.append((container_name, False, "Waiting state"))
                    continue
                else:
                    state_folder = "Terminated/logs"
                    print(f"⚠️ Warning: Init container {container_name} in pod {pod_name} has unclear state, defaulting to Terminated/logs")
                
                log_dir = os.path.join(pod_dir, state_folder)
                os.makedirs(log_dir, exist_ok=True)
                
                log_file = os.path.join(log_dir, f"{container_name}.log")
                log_tasks.append((ns, pod_name, container_name, log_file, True))
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_container = {
                    executor.submit(collect_container_log, *task): task[2] for task in log_tasks
                }
                for future in as_completed(future_to_container):
                    container_name = future_to_container[future]
                    is_init = any(task[2] == container_name and task[4] for task in log_tasks)
                    result = future.result()
                    if is_init:
                        init_results.append(result)
                    else:
                        regular_results.append(result)
            
            print(f"\nBacking up log for Deployment Pod {pod_name}")
            print("=" * 18)
            if init_containers:
                print("Init Containers:")
                for container_name, success, error in sorted(init_results, key=lambda x: x[0]):
                    status = "✅" if success else "❌"
                    print(f"- {container_name}: {status}")
                    if error and not success:
                        print(f"  Error: {error}")
            if containers:
                print("Containers:")
                for container_name, success, error in sorted(regular_results, key=lambda x: x[0]):
                    status = "✅" if success else "❌"
                    print(f"- {container_name}: {status}")
                    if error and not success:
                        print(f"  Error: {error}")
        
        tar_file = f"{backup_dir}/logs_backup_{timestamp}.tar.gz"
        tar_cmd = ["tar", "-czf", tar_file, "-C", backup_dir, "."]
        subprocess.run(tar_cmd)
        
        # Calculate and display time taken
        end_time = time.time()
        duration_seconds = end_time - start_time
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        print(f"\n✅ Logs backed up to: {tar_file}")
        print(f"⏱️ Time taken to backup logs: {minutes} minutes, {seconds} seconds")
        if duration_seconds > 300:
            print(f"⚠️ Warning: Backup took longer than 5 minutes. Consider increasing max_workers or optimizing system resources.")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to create backup: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse pod JSON data: {e}")
        sys.exit(1)

def main():
    # Check for updates
    has_update, latest_version = check_for_updates()
    if has_update:
        logger.info(f"New version {latest_version} is available. Current version: {SCRIPT_VERSION}")
        print(f"New version {latest_version} is available. Current version: {SCRIPT_VERSION}")
        user_choice = input("Would you like to update? (Yes/No): ").strip().lower()
        logger.info(f"User chose to {'update' if user_choice == 'yes' else 'skip update'}")
        if user_choice == 'yes':
            if update_script():
                logger.info("Script updated successfully. Proceeding with execution.")
                print("✅ Script updated successfully. Proceeding with execution.")
            else:
                logger.error("Failed to update script. Proceeding with current version.")
                print("❌ Failed to update script. Proceeding with current version.")
        else:
            logger.info("Skipping update. Proceeding with current version.")
            print("Skipping update. Proceeding with current version.")
    else:
        logger.info(f"Script is up-to-date. Running version: {SCRIPT_VERSION}")

    if len(sys.argv) != 3:
        print("❌ Usage: ./viya4_environment_restart.py <ConfigurationItem> <TicketNumber>")
        sys.exit(1)
    
    ci = sys.argv[1]
    ticket = sys.argv[2]
    
    print(f"\n🚀 Starting SAS Viya 4 Environment Restart Automation")
    print(f"Configuration Item: {ci}")
    print(f"Ticket Number: {ticket}\n")
    
    env_vars = parse_ci(ci)
    for key, value in env_vars.items():
        os.environ[key] = value
    
    duration = input("⏱️ Enter maintenance duration (e.g., 2h): ")
    setup_zabbix_maintenance(ci, ticket, duration)
    
    list_running_pods(env_vars['NS'])
    
    create_backup_logs(env_vars['NS'], ticket)
    
    print(f"\n🎉 Automation completed successfully!")

if __name__ == "__main__":
    main()