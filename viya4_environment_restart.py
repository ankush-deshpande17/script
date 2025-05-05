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
import shutil

# Set up logging with file output for DEBUG and console output for INFO
log_file = f"/tmp/viya4_restart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),  # Write all logs to file
        logging.StreamHandler(sys.stdout)  # Console output
    ]
)

# Create a console handler with INFO level to suppress DEBUG in terminal
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Replace default StreamHandler with the new one
logger = logging.getLogger(__name__)
for handler in logger.handlers[:]:  # Copy to avoid modifying while iterating
    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
        logger.removeHandler(handler)
logger.addHandler(console_handler)

# Version and GitHub settings
SCRIPT_VERSION = "v1.5.2"
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

def print_index_page():
    """Print an index page with all steps and their descriptions"""
    steps = [
        (0, "Parse Configuration Item", "Parse the Configuration Item to extract namespace, AKS name, and region"),
        (1, "Setup Zabbix Maintenance", "Create a Zabbix maintenance window for the specified duration"),
        (2, "List Running Pods", "List all currently running pods in the namespace"),
        (3, "Create Backup of Logs", "Backup logs for all running pods"),
        (4, "Backup Consul Raft.db File", "Backup the Consul raft.db file for all consul server pods"),
        (5, "Stop SAS Environment", "Stop the SAS environment by submitting a stop job"),
        (6, "Deletion of Jobs", "Delete specified jobs matching predefined patterns"),
        (7, "Start Viya Environment", "Start the SAS Viya environment by submitting a start job"),
        (8, "Verifying Consul Server Pods", "Verify that all consul server pods are healthy"),
        (9, "Monitoring Pods Post Restart", "Dynamically monitor pod statuses after restart")
    ]
    
    print("\n" + "=" * 60)
    print(f"{'SAS Viya 4 Environment Restart Automation - Index':^60}")
    print("=" * 60)
    print(f"{'Step':<8}{'Name':<30}{'Description':<50}")
    print("-" * 60)
    for step_num, step_name, description in steps:
        print(f"{step_num:<8}{step_name:<30}{description:<50}")
    print("=" * 60 + "\n")

def countdown(seconds, message):
    """Display a countdown timer that updates in place"""
    for i in range(seconds, -1, -1):
        print(f"\r{message}: {i}  ", end="", flush=True)
        sys.stdout.flush()
        time.sleep(1)
    print()

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
        'RGN': rgn,
        'TLA': tla  # Add TLA for Zabbix maintenance check
    }
    
    print("✅ Environment Variables Initialized:")
    for key, value in env_vars.items():
        print(f"  {key}: {value}")
    
    return env_vars

def check_zabbix_maintenance(tla, ci, vsp_user, vsp_pass):
    """Check if the environment is already under Zabbix maintenance"""
    zabbix_script = "/home/anzdes/viya-upgrade-scripts/zabbixClient-v2.0"
    
    if not os.path.isfile(zabbix_script):
        logger.warning(f"Zabbix script not found at {zabbix_script}. Skipping maintenance check.")
        return False, []
    if not os.access(zabbix_script, os.X_OK):
        logger.warning(f"Zabbix script at {zabbix_script} is not executable. Skipping maintenance check.")
        return False, []
    
    cmd = [zabbix_script, "list", f"--tla={tla}"]
    log_file = f"/tmp/zabbix_list_{tla}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logger.debug(f"Executing Zabbix list command: {' '.join(cmd)}")
    
    try:
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
            
            stdout_lines = []
            stderr_lines = []
            timeout_seconds = 60
            start_time = time.time()
            
            while process.poll() is None:
                if time.time() - start_time > timeout_seconds:
                    process.terminate()
                    logger.error(f"Zabbix list command timed out after {timeout_seconds} seconds")
                    return False, []
                
                stdout_line = process.stdout.readline()
                if stdout_line:
                    log.write(stdout_line)
                    stdout_lines.append(stdout_line)
                
                stderr_line = process.stderr.readline()
                if stderr_line:
                    log.write(stderr_line)
                    stderr_lines.append(stderr_line)
                
                time.sleep(0.1)
            
            stdout, stderr = process.communicate()
            if stdout:
                log.write(stdout)
                stdout_lines.append(stdout)
            if stderr:
                log.write(stderr)
                stderr_lines.append(stderr)
            
            if process.returncode != 0:
                logger.error(f"Zabbix list command failed: {''.join(stderr_lines) or ''.join(stdout_lines)}")
                return False, []
        
        # Parse the output
        output = ''.join(stdout_lines)
        
        lines = output.splitlines()
        maintenance_records = []
        filtered_records = []
        in_table = False
        header = None
        
        # Construct target host group name
        env_map = {
            'PROD': 'PROD',
            'DEV': 'DEV',
            # Add other environments as needed
        }
        match = re.match(r'^([A-Z]{3})_(.+)_VIYA4_([A-Z]+)$', ci)
        if not match:
            logger.error(f"Invalid CI format for constructing host group: {ci}")
            return False, []
        
        tla, middle, env = match.groups()
        env_display = env_map.get(env, env)
        target_host_group = f"{tla} (VML VIYA4 {env_display})"
        logger.debug(f"Target host group: {target_host_group}")
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if 'Host Group ID |Host Group Name' in line:
                in_table = True
                header = [h.strip() for h in line.split('|')]
                logger.debug(f"Table header: {header}")
                continue
            if in_table and '|' in line:
                try:
                    fields = [f.strip() for f in line.split('|')]
                    if len(fields) < len(header):
                        logger.warning(f"Skipping malformed table row: {line}")
                        continue
                    record = dict(zip(header, fields))
                    if 'Host Group Name' not in record:
                        logger.warning(f"Skipping row missing Host Group Name: {line}")
                        continue
                    # Filter for target host group
                    if record['Host Group Name'] == target_host_group:
                        filtered_records.append(line)
                        if record.get('IsInMaintenance', '').lower() == 'true':
                            maintenance_records.append({
                                'MaintenanceID': record.get('MaintenanceID', 'N/A'),
                                'MaintenanceName': record.get('MaintenanceName', 'N/A')
                            })
                except Exception as e:
                    logger.warning(f"Failed to parse table row: {line}, Error: {e}")
                    continue
        
        # Log filtered records instead of full output
        if filtered_records:
            logger.debug(f"Filtered Zabbix output for {target_host_group}:\n{'Host Group ID |Host Group Name          |IsInMaintenance |MaintenanceID |MaintenanceName'}\n" + '\n'.join(filtered_records))
        else:
            logger.debug(f"No records found for host group {target_host_group}")
        
        is_in_maintenance = bool(maintenance_records)
        if is_in_maintenance:
            logger.info(f"Environment {target_host_group} is under maintenance: {maintenance_records}")
        else:
            logger.info(f"No maintenance found for environment {target_host_group}")
        
        return is_in_maintenance, maintenance_records
    
    except Exception as e:
        logger.error(f"Failed to check Zabbix maintenance: {str(e)}")
        return False, []

def setup_zabbix_maintenance(ci, ticket, duration):
    """Step 1: Setup Zabbix Maintenance"""
    print_step_header(1, "Setup Zabbix Maintenance", "🛠️")
    
    zabbix_script = "/home/anzdes/viya-upgrade-scripts/zabbixClient-v2.0"
    
    # Extract TLA from CI
    tla_match = re.match(r'^([A-Z]{3})_', ci)
    if not tla_match:
        print(f"❌ Error: Cannot extract TLA from Configuration Item: {ci}")
        sys.exit(1)
    tla = tla_match.group(1)
    
    # Prompt for credentials
    vsp_user = input("👤 Enter VSP userid: ")
    vsp_pass = getpass.getpass("🔑 Enter VSP Password: ")
    
    # Check for existing maintenance
    print(f"🔍 Checking for existing Zabbix maintenance for TLA: {tla}")
    is_in_maintenance, maintenance_records = check_zabbix_maintenance(tla, ci, vsp_user, vsp_pass)
    
    if is_in_maintenance:
        print(f"\n⚠️ Environment for {ci} is already under maintenance. Skipping maintenance creation.")
        print("📋 Active Maintenance Details:")
        for record in maintenance_records:
            print(f"- Maintenance ID: {record['MaintenanceID']}, Name: {record['MaintenanceName']}")
        return
    
    # Proceed with maintenance creation if no existing maintenance
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
    
    print(f"\n🔍 Executing Zabbix maintenance command: {' '.join(cmd)}")
    
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

def backup_consul_raft(ns, ticket):
    """Step 4: Backup Consul Raft.db File"""
    print_step_header(4, "Backup Consul Raft.db File", "💾")
    
    start_time = time.time()
    
    source_file = "/consul/data/raft/raft.db"
    dest_file = f"/consul/data/raft/raft.db_{ticket}"
    
    cmd = ["kubectl", "get", "pods", "-n", ns, "-l", "app=sas-consul-server", "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        pods_data = json.loads(result.stdout)
        consul_pods = [pod["metadata"]["name"] for pod in pods_data.get("items", []) if pod["status"].get("phase") == "Running"]
        if not consul_pods:
            print(f"❌ Error: No running sas-consul-server pods found in namespace {ns}")
            sys.exit(1)
        print(f"📋 Found {len(consul_pods)} running sas-consul-server pods: {', '.join(consul_pods)}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to list sas-consul-server pods: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse pod JSON data: {e}")
        sys.exit(1)
    
    def backup_raft_for_pod(pod_name):
        """Helper function to backup raft.db for a single pod"""
        check_cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "test", "-f", source_file]
        try:
            subprocess.run(check_cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            return (pod_name, False, f"Source file {source_file} does not exist in pod {pod_name}: {e.stderr}")
        
        backup_cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "cp", source_file, dest_file]
        try:
            result = subprocess.run(backup_cmd, capture_output=True, text=True, check=True)
            print(f"✅ Backed up {source_file} to {dest_file} in pod {pod_name}")
            return (pod_name, True, None)
        except subprocess.CalledProcessError as e:
            return (pod_name, False, f"Failed to backup {source_file} to {dest_file} in pod {pod_name}: {e.stderr}")
    
    backup_results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_pod = {executor.submit(backup_raft_for_pod, pod): pod for pod in consul_pods}
        for future in as_completed(future_to_pod):
            pod_name = future_to_pod[future]
            result = future.result()
            backup_results.append(result)
    
    print(f"\n📋 Consul Raft.db Backup Summary:")
    print("=" * 18)
    for pod_name, success, error in sorted(backup_results, key=lambda x: x[0]):
        status = "✅" if success else "❌"
        print(f"- {pod_name}: {status}")
        if error and not success:
            print(f"  Error: {error}")
    
    failed_backups = [r for r in backup_results if not r[1]]
    if failed_backups:
        print(f"❌ Error: Failed to backup raft.db for {len(failed_backups)} pod(s). See errors above.")
        sys.exit(1)
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ Consul Raft.db backups completed for all {len(consul_pods)} pods")
    print(f"⏱️ Time taken to backup raft.db files: {minutes} minutes, {seconds} seconds")

def stop_sas_environment(ns, ticket):
    """Step 5: Stop SAS Environment"""
    print_step_header(5, "Stop SAS Environment", "🛑")
    
    start_time = time.time()
    
    timestamp = str(int(time.time()))
    job_name = f"sas-stop-all-{timestamp}"
    cmd = ["kubectl", "create", "job", job_name, "--from", "cronjobs/sas-stop-all", "-n", ns]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✅ Submitted stop job: {job_name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to submit stop job: {e.stderr}")
        sys.exit(1)
    
    countdown(10, "⏳ Waiting for stop job pod to be created")
    
    cmd = ["kubectl", "get", "pods", "-n", ns, "-l", f"job-name={job_name}", "-o", "json"]
    max_attempts = 30
    attempt = 0
    pod_name = None
    while attempt < max_attempts:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            pods = pods_data.get("items", [])
            if pods:
                pod = pods[0]
                pod_name = pod["metadata"]["name"]
                pod_phase = pod["status"].get("phase")
                print(f"📋 Found stop job pod: {pod_name} (Phase: {pod_phase})")
                break
            else:
                attempt += 1
                time.sleep(2)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            attempt += 1
            time.sleep(2)
    
    if not pod_name:
        print(f"❌ Error: Stop job pod not found after {max_attempts * 2} seconds")
        sys.exit(1)
    
    print(f"\n⏳ Waiting for pod {pod_name} to complete")
    while True:
        try:
            cmd = ["kubectl", "get", "pod", "-n", ns, pod_name, "-o", "json"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pod_data = json.loads(result.stdout)
            pod_phase = pod_data["status"].get("phase")
            if pod_phase == "Succeeded":
                print(f"✅ Stop job pod {pod_name} completed successfully")
                cmd = ["kubectl", "get", "pod", "-n", ns, pod_name]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                pod_status = result.stdout.strip().split('\n')[-1]
                print(f"📋 Final pod state: {pod_status}")
                break
            elif pod_phase == "Failed":
                print(f"❌ Error: Stop job pod {pod_name} failed")
                sys.exit(1)
            time.sleep(5)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Error checking pod status: {e}")
            sys.exit(1)
    
    countdown(60, "⏳ Waiting for pods to terminate gracefully")
    
    exclude_pods = set()
    try:
        cmd = ["kubectl", "get", "pods", "-n", ns, "-l", "app=prometheus-pushgateway", "-o", "json"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        pods_data = json.loads(result.stdout)
        exclude_pods = {pod["metadata"]["name"] for pod in pods_data.get("items", [])}
        if exclude_pods:
            print(f"📋 Excluding prometheus-pushgateway pods from deletion: {', '.join(exclude_pods)}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Warning: Failed to list prometheus-pushgateway pods: {e.stderr}")
    
    cmd = ["kubectl", "get", "pods", "-n", ns, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        pods_data = json.loads(result.stdout)
        stuck_pods = []
        for pod in pods_data.get("items", []):
            pod_name = pod["metadata"]["name"]
            pod_phase = pod["status"].get("phase")
            if pod_name not in exclude_pods and (pod_phase == "Running" or pod.get("metadata", {}).get("deletionTimestamp")):
                stuck_pods.append(pod_name)
        
        if stuck_pods:
            print(f"📋 Found {len(stuck_pods)} stuck pods: {', '.join(stuck_pods)}")
            for pod_name in stuck_pods:
                cmd = ["kubectl", "delete", "pod", "-n", ns, pod_name, "--force", "--grace-period=0"]
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                    print(f"✅ Forcefully deleted stuck pod: {pod_name}")
                except subprocess.CalledProcessError as e:
                    print(f"❌ Failed to delete pod {pod_name}: {e.stderr}")
        else:
            print("✅ No stuck pods found")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"❌ Failed to check for stuck pods: {e}")
        sys.exit(1)
    
    print("\n📋 Listing pods after stop operation:")
    list_running_pods(ns)
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ SAS environment stop completed")
    print(f"⏱️ Time taken to stop SAS environment: {minutes} minutes, {seconds} seconds")

def delete_jobs(ns, ticket):
    """Step 6: Deletion of Jobs"""
    print_step_header(6, "Deletion of Jobs", "🗑️")
    
    start_time = time.time()
    
    job_patterns = [
        "sas-backup-purge-job",
        "sas-update-checker",
        "sas-import-data-loader",
        "sas-deployment-operator-autoupdate"
    ]
    
    cmd = ["kubectl", "get", "jobs", "-n", ns, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        jobs_data = json.loads(result.stdout)
        jobs = jobs_data.get("items", [])
        jobs_to_delete = [
            job["metadata"]["name"] for job in jobs
            if any(pattern in job["metadata"]["name"] for pattern in job_patterns)
        ]
        
        if not jobs_to_delete:
            print("✅ No matching jobs found to delete")
        else:
            print(f"📋 Found {len(jobs_to_delete)} jobs to delete: {', '.join(jobs_to_delete)}")
            for job_name in jobs_to_delete:
                cmd = ["kubectl", "delete", "job", "-n", ns, job_name]
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                    print(f"✅ Deleted job: {job_name}")
                except subprocess.CalledProcessError as e:
                    print(f"❌ Failed to delete job {job_name}: {e.stderr}")
        
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"❌ Failed to list jobs: {e}")
        sys.exit(1)
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ Job deletion completed")
    print(f"⏱️ Time taken to delete jobs: {minutes} minutes, {seconds} seconds")

def start_viya_environment(ns, ticket):
    """Step 7: Start Viya Environment"""
    print_step_header(7, "Start Viya Environment", "🚀")
    
    start_time = time.time()
    
    timestamp = str(int(time.time()))
    job_name = f"sas-start-all-{timestamp}"
    cmd = ["kubectl", "create", "job", job_name, "--from", "cronjobs/sas-start-all", "-n", ns]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✅ Submitted start job: {job_name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to submit start job: {e.stderr}")
        sys.exit(1)
    
    countdown(10, "⏳ Waiting for start job pod to be created")
    
    cmd = ["kubectl", "get", "pods", "-n", ns, "-l", f"job-name={job_name}", "-o", "json"]
    max_attempts = 30
    attempt = 0
    pod_name = None
    while attempt < max_attempts:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            pods = pods_data.get("items", [])
            if pods:
                pod = pods[0]
                pod_name = pod["metadata"]["name"]
                pod_phase = pod["status"].get("phase")
                print(f"📋 Found start job pod: {pod_name} (Phase: {pod_phase})")
                break
            else:
                attempt += 1
                time.sleep(2)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            attempt += 1
            time.sleep(2)
    
    if not pod_name:
        print(f"❌ Error: Start job pod not found after {max_attempts * 2} seconds")
        sys.exit(1)
    
    print(f"\n⏳ Waiting for pod {pod_name} to complete")
    while True:
        try:
            cmd = ["kubectl", "get", "pod", "-n", ns, pod_name, "-o", "json"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pod_data = json.loads(result.stdout)
            pod_phase = pod_data["status"].get("phase")
            if pod_phase == "Succeeded":
                print(f"✅ Start job pod {pod_name} completed successfully")
                cmd = ["kubectl", "get", "pod", "-n", ns, pod_name]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                pod_status = result.stdout.strip().split('\n')[-1]
                print(f"📋 Final pod state: {pod_status}")
                break
            elif pod_phase == "Failed":
                print(f"❌ Error: Start job pod {pod_name} failed")
                sys.exit(1)
            time.sleep(5)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Error checking pod status: {e}")
            sys.exit(1)
    
    countdown(60, "⏳ Waiting for pods to start")
    
    print("\n📋 Listing pods after start operation:")
    list_running_pods(ns)
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ SAS Viya environment start completed")
    print(f"⏱️ Time taken to start SAS Viya environment: {minutes} minutes, {seconds} seconds")

def verify_consul_pods(ns, ticket):
    """Step 8: Verifying Consul Server Pods"""
    print_step_header(8, "Verifying Consul Server Pods", "🔍")
    
    start_time = time.time()
    initial_statuses = []  # Store initial pod statuses for comparison
    remediation_performed = False  # Track if remediation was needed
    
    def check_consul_health():
        """Check if all sas-consul-server pods are 1/1 Running"""
        cmd = ["kubectl", "get", "pods", "-n", ns, "-l", "app=sas-consul-server", "-o", "json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            consul_pods = pods_data.get("items", [])
            if not consul_pods:
                print(f"⚠️ Warning: No sas-consul-server pods found in namespace {ns}. Continuing.")
                return False, []
            
            print("\n📋 Consul server pod status:")
            all_healthy = True
            pod_statuses = []
            for pod in consul_pods:
                pod_name = pod["metadata"]["name"]
                phase = pod["status"].get("phase", "Unknown")
                container_statuses = pod["status"].get("containerStatuses", [])
                
                total_containers = len(container_statuses)
                ready_containers = sum(1 for cs in container_statuses if cs.get("ready", False))
                ready_status = f"{ready_containers}/{total_containers}"
                
                status = phase
                for cs in container_statuses:
                    state = cs.get("state", {})
                    if "waiting" in state and state["waiting"].get("reason") == "CrashLoopBackOff":
                        status = "CrashLoopBackOff"
                        break
                    elif "terminated" in state and state["terminated"].get("exitCode", 0) != 0:
                        status = "Error"
                        break
                
                pod_statuses.append((pod_name, ready_status, status))
                if ready_status != f"{total_containers}/{total_containers}" or status not in ["Running"]:
                    all_healthy = False
            
            for pod_name, ready, status in sorted(pod_statuses, key=lambda x: x[0]):
                status_icon = "✅" if ready == "1/1" and status == "Running" else "❌"
                print(f"- {pod_name}: {ready} {status} {status_icon}")
            
            return all_healthy, pod_statuses
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Failed to check consul pod status: {e}. Continuing.")
            logger.error(f"Failed to check consul pod status: {e}")
            return False, []

    def remediate_consul(initial_statuses):
        """Scale down consul, delete raft.db from PVCs, and scale up"""
        print("\n📉 Scaling down sas-consul-server to 0 replicas")
        cmd = ["kubectl", "-n", ns, "scale", "sts/sas-consul-server", "--replicas=0"]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("✅ Scaled down sas-consul-server")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to scale down sas-consul-server: {e.stderr}. Continuing.")
            logger.error(f"Failed to scale down sas-consul-server: {e.stderr}")

        countdown(10, "⏳ Waiting for consul pods to terminate")
        
        cmd = ["kubectl", "get", "pods", "-n", ns, "-l", "app=sas-consul-server", "-o", "json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            lingering_pods = [pod["metadata"]["name"] for pod in pods_data.get("items", [])]
            if lingering_pods:
                print(f"📋 Found {len(lingering_pods)} lingering consul pods: {', '.join(lingering_pods)}")
                for pod_name in lingering_pods:
                    cmd = ["kubectl", "delete", "pod", "-n", ns, pod_name, "--force", "--grace-period=0"]
                    try:
                        subprocess.run(cmd, capture_output=True, text=True, check=True)
                        print(f"✅ Forcefully deleted pod: {pod_name}")
                    except subprocess.CalledProcessError as e:
                        print(f"❌ Failed to delete pod {pod_name}: {e.stderr}. Continuing.")
                        logger.error(f"Failed to delete pod {pod_name}: {e.stderr}")
            else:
                print("✅ No lingering consul pods found")
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Failed to check for lingering pods: {e}. Continuing.")
            logger.error(f"Failed to check for lingering pods: {e}")
        
        pvc_names = [
            "sas-viya-consul-data-volume-sas-consul-server-0",
            "sas-viya-consul-data-volume-sas-consul-server-1",
            "sas-viya-consul-data-volume-sas-consul-server-2"
        ]
        
        pvc_errors = []
        for pvc_name in pvc_names:
            print(f"\n📀 Processing PVC: {pvc_name}")
            pod_name = f"temp-consul-pvc-{pvc_name.split('-')[-1]}-{int(time.time())}"
            pod_manifest = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod_name, "namespace": ns},
                "spec": {
                    "containers": [
                        {
                            "name": "busybox",
                            "image": "busybox:latest",
                            "command": ["sleep", "3600"],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/consul/data"}
                            ]
                        }
                    ],
                    "volumes": [
                        {
                            "name": "data",
                            "persistentVolumeClaim": {"claimName": pvc_name}
                        }
                    ]
                }
            }
            
            manifest_file = f"/tmp/{pod_name}.yaml"
            with open(manifest_file, "w") as f:
                json.dump(pod_manifest, f)
            
            cmd = ["kubectl", "apply", "-f", manifest_file, "-n", ns]
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Created temporary pod: {pod_name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to create pod for PVC {pvc_name}: {e.stderr}. Continuing.")
                logger.error(f"Failed to create pod for PVC {pvc_name}: {e.stderr}")
                pvc_errors.append(f"Failed to create pod for PVC {pvc_name}: {e.stderr}")
                os.remove(manifest_file)
                continue
            
            max_attempts = 30
            attempt = 0
            while attempt < max_attempts:
                cmd = ["kubectl", "get", "pod", "-n", ns, pod_name, "-o", "json"]
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    pod_data = json.loads(result.stdout)
                    if pod_data["status"].get("phase") == "Running":
                        print(f"✅ Pod {pod_name} is Running")
                        break
                    time.sleep(2)
                    attempt += 1
                except (subprocess.CalledProcessError, json.JSONDecodeError):
                    time.sleep(2)
                    attempt += 1
            
            if attempt >= max_attempts:
                print(f"❌ Error: Pod {pod_name} did not reach Running state. Continuing.")
                logger.error(f"Pod {pod_name} did not reach Running state")
                pvc_errors.append(f"Pod {pod_name} did not reach Running state")
                cmd = ["kubectl", "delete", "pod", "-n", ns, pod_name, "--force", "--grace-period=0"]
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                    print(f"✅ Deleted temporary pod: {pod_name}")
                except subprocess.CalledProcessError as e:
                    print(f"❌ Failed to delete pod {pod_name}: {e.stderr}. Continuing.")
                    logger.error(f"Failed to delete pod {pod_name}: {e.stderr}")
                os.remove(manifest_file)
                continue
            
            # List raft.db before deletion
            print(f"\n📋 Listing raft.db before deletion in PVC {pvc_name}")
            cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "ls", "-l", "/consul/data/raft/raft.db"]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"📄 {result.stdout.strip()}")
            except subprocess.CalledProcessError as e:
                print(f"⚠️ Warning: Could not list raft.db: {e.stderr}. Continuing.")
                logger.warning(f"Could not list raft.db in PVC {pvc_name}: {e.stderr}")
            
            # Delete raft.db
            cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "rm", "-f", "/consul/data/raft/raft.db"]
            max_retries = 3
            retry_attempt = 0
            while retry_attempt < max_retries:
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                    print(f"✅ Deleted raft.db from PVC {pvc_name}")
                    break
                except subprocess.CalledProcessError as e:
                    retry_attempt += 1
                    if retry_attempt < max_retries:
                        print(f"❌ Failed to delete raft.db from PVC {pvc_name} (Attempt {retry_attempt}/{max_retries}): {e.stderr}. Retrying in 5 seconds.")
                        logger.error(f"Failed to delete raft.db from PVC {pvc_name} (Attempt {retry_attempt}/{max_retries}): {e.stderr}")
                        time.sleep(5)
                    else:
                        print(f"❌ Failed to delete raft.db from PVC {pvc_name} after {max_retries} attempts: {e.stderr}. Continuing.")
                        logger.error(f"Failed to delete raft.db from PVC {pvc_name} after {max_retries} attempts: {e.stderr}")
                        pvc_errors.append(f"Failed to delete raft.db from PVC {pvc_name}: {e.stderr}")
            
            # List directory after deletion
            print(f"\n📋 Listing /consul/data/raft/ after deletion in PVC {pvc_name}")
            cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "ls", "-l", "/consul/data/raft/"]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"📄 {result.stdout.strip() or 'Directory is empty'}")
            except subprocess.CalledProcessError as e:
                print(f"⚠️ Warning: Could not list directory: {e.stderr}. Continuing.")
                logger.warning(f"Could not list directory in PVC {pvc_name}: {e.stderr}")
            
            # Delete temporary pod
            cmd = ["kubectl", "delete", "pod", "-n", ns, pod_name, "--force", "--grace-period=0"]
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Deleted temporary pod: {pod_name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to delete pod {pod_name}: {e.stderr}. Continuing.")
                logger.error(f"Failed to delete pod {pod_name}: {e.stderr}")
                pvc_errors.append(f"Failed to delete pod {pod_name}: {e.stderr}")
            
            os.remove(manifest_file)
        
        print("\n📈 Scaling up sas-consul-server to 3 replicas")
        cmd = ["kubectl", "-n", ns, "scale", "sts/sas-consul-server", "--replicas=3"]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("✅ Scaled up sas-consul-server")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to scale up sas-consul-server: {e.stderr}. Continuing.")
            logger.error(f"Failed to scale up sas-consul-server: {e.stderr}")
            pvc_errors.append(f"Failed to scale up sas-consul-server: {e.stderr}")
        
        countdown(120, "⏳ Waiting for consul pods to start")
        
        if pvc_errors:
            print(f"\n⚠️ Encountered {len(pvc_errors)} errors during PVC remediation:")
            for error in pvc_errors:
                print(f"- {error}")
    
    max_retries = 1
    attempt = 0
    all_healthy = False
    while attempt <= max_retries:
        all_healthy, current_statuses = check_consul_health()
        if attempt == 0:
            initial_statuses = current_statuses  # Store initial statuses
        if all_healthy:
            print("\n✅ All sas-consul-server pods are healthy (1/1 Running)")
            # Print comparison table only if remediation was performed
            if remediation_performed and initial_statuses and current_statuses:
                print("\n📊 Consul Pod Status Comparison (Pre- and Post-PVC Operations):")
                terminal_width = min(shutil.get_terminal_size().columns, 80)
                separator = "-" * terminal_width
                print(separator)
                print(f"{'Pod Name':<25}{'Pre-PVC Ready':<15}{'Pre-PVC Status':<20}{'Post-PVC Ready':<15}{'Post-PVC Status':<20}")
                print(separator)
                # Merge statuses by pod name
                for curr in sorted(current_statuses, key=lambda x: x[0]):
                    pod_name, post_ready, post_status = curr
                    pre_ready = pre_status = "N/A"
                    for init in initial_statuses:
                        if init[0] == pod_name:
                            pre_ready, pre_status = init[1], init[2]
                            break
                    print(f"{pod_name:<25}{pre_ready:<15}{pre_status:<20}{post_ready:<15}{post_status:<20}")
                print(separator)
            break
        else:
            print(f"\n⚠️ Consul pods are not healthy (Attempt {attempt + 1}/{max_retries + 1})")
            remediation_performed = True  # Mark that remediation was attempted
            remediate_consul(initial_statuses)
            attempt += 1
            if attempt <= max_retries:
                print("\n🔄 Retrying consul health check after remediation")
    
    if not all_healthy:
        print("\n⚠️ Warning: Consul pods are still not healthy after remediation attempts. Continuing to next step.")
        logger.warning("Consul pods are not healthy after remediation attempts")
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ Consul server pod verification completed (with possible errors)")
    print(f"⏱️ Time taken to verify consul pods: {minutes} minutes, {seconds} seconds")

def monitor_pods(namespace, ticket):
    """Step 9: Monitor pod statuses with a single table updated in place using ANSI codes"""
    print_step_header(9, "Monitoring Pods Post Restart", "📊")
    
    # Set up logging with file output for DEBUG and console output for INFO
    log_file = f"/tmp/pod_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Configure root logger to WARNING to prevent interference
    logging.getLogger('').setLevel(logging.WARNING)
    logging.getLogger('').handlers.clear()

    # Create named logger for the script
    logger = logging.getLogger('pod_monitor')
    logger.setLevel(logging.DEBUG)  # Capture all levels
    logger.handlers.clear()  # Clear any existing handlers

    # File handler for DEBUG and above
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # Console handler for WARNING and above to suppress INFO logs
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)  # Only show WARNING and above in console
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

    start_time = time.time()
    iteration = 0
    table_lines = 10  # Header, subtitle, separator, timestamp, instruction, separator, headers, data, separator, empty line
    
    # Log terminal and environment details (to file only)
    term_type = os.environ.get("TERM", "unknown")
    in_tmux = os.environ.get("TMUX", "no") != "no"
    in_screen = os.environ.get("STY", "no") != "no"
    is_tty = sys.stdout.isatty()
    logger.debug(f"Terminal type: {term_type}, TMUX: {in_tmux}, SCREEN: {in_screen}, TTY: {is_tty}")
    
    # Check if appending is forced or ANSI is unsupported
    force_append = os.environ.get("FORCE_APPEND", "0") == "1"
    use_ansi = not force_append and term_type != "dumb" and is_tty
    
    # Print separator with minimal padding to separate from prior script output
    terminal_width = min(shutil.get_terminal_size().columns, 80)
    print(f"\n{'=' * terminal_width}")
    
    try:
        while True:
            iteration += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_pods = running = pending = crashloop = error = completed = 0
            
            try:
                cmd = ["kubectl", "get", "pods", "-n", namespace, "-o", "json"]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
                pod_data = json.loads(result.stdout)
                pods = pod_data.get("items", [])
                
                total_pods = len(pods)
                for pod in pods:
                    phase = pod["status"].get("phase", "Unknown")
                    container_statuses = pod["status"].get("containerStatuses", [])
                    
                    if phase == "Pending":
                        pending += 1
                        continue
                    elif phase == "Succeeded":
                        completed += 1
                        continue
                    
                    total_containers = len(container_statuses)
                    ready_containers = sum(1 for cs in container_statuses if cs.get("ready", False))
                    ready_status = f"{ready_containers}/{total_containers}"
                    
                    has_crashloop = False
                    has_error = False
                    for cs in container_statuses:
                        state = cs.get("state", {})
                        if "waiting" in state and state["waiting"].get("reason") == "CrashLoopBackOff":
                            has_crashloop = True
                            break
                        elif "terminated" in state and state["terminated"].get("exitCode", 0) != 0:
                            has_error = True
                            break
                    
                    if has_crashloop:
                        crashloop += 1
                    elif has_error:
                        error += 1
                    elif phase == "Running" and ready_status == f"{total_containers}/{total_containers}":
                        running += 1
                    else:
                        error += 1
                
                logger.debug(f"Iteration {iteration}: Total={total_pods}, Running={running}, Pending={pending}, CrashLoopBackOff={crashloop}, Error={error}, Completed={completed}")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to fetch pod statuses: {e.stderr}")
                print(f"❌ Error fetching pod statuses at {timestamp}: {e.stderr}")
                total_pods = running = pending = crashloop = error = completed = 0
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse pod JSON data: {e}")
                print(f"❌ Error parsing pod data at {timestamp}: {e}")
                total_pods = running = pending = crashloop = error = completed = 0
            except subprocess.TimeoutExpired as e:
                logger.error(f"kubectl command timed out: {e}")
                print(f"❌ kubectl command timed out at {timestamp}")
                total_pods = running = pending = crashloop = error = completed = 0
            
            # Build table with Pending column
            separator = "-" * terminal_width
            table = [
                f"{'=' * terminal_width}",
                f"Pod Monitoring (Namespace: {namespace}, Ticket: {ticket})",
                f"{'=' * terminal_width}",
                f"Timestamp: {timestamp} | Iteration: {iteration}",
                f"Press Ctrl+C to stop monitoring",
                separator,
                f"{'Sr. No.':<10}{'Total Pods':<12}{'Running':<10}{'Pending':<10}{'CrashLoopBackOff':<18}{'Error':<10}{'Completed':<10}",
                separator,
                f"{iteration:<10}{total_pods:<12}{running:<10}{pending:<10}{crashloop:<18}{error:<10}{completed:<10}",
                separator
            ]
            
            # Ensure table has exactly table_lines lines
            while len(table) < table_lines:
                table.append("")
            table = table[:table_lines]
            
            # Print table
            if use_ansi:
                try:
                    if iteration == 1:
                        # Print table for the first time
                        for line in table:
                            print(line)
                    else:
                        # Move cursor up to the start of the table and update
                        print(f"\033[{table_lines}A", end="")
                        for line in table:
                            print(f"\033[K{line}")  # Clear line and print new content
                    sys.stdout.flush()
                except Exception as e:
                    logger.warning(f"ANSI rendering failed: {e}. Switching to append mode.")
                    print(f"\n⚠️ ANSI rendering failed: {e}. Switching to append mode.")
                    use_ansi = False
            
            if not use_ansi:
                print("\n")
                for line in table:
                    try:
                        print(f"{line:<{terminal_width}}")
                        sys.stdout.flush()
                    except Exception as e:
                        logger.error(f"Terminal write error: {e}")
                        print(f"❌ Terminal write error: {e}")
            
            # Exit if no Pending pods
            if pending == 0:
                print("\n✅ No Pending pods remaining. Exiting monitoring.")
                break
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        separator = "-" * terminal_width
        table = [
            f"{'=' * terminal_width}",
            f"Pod Monitoring (Namespace: {namespace}, Ticket: {ticket})",
            f"{'=' * terminal_width}",
            f"Timestamp: {timestamp} | Iteration: {iteration}",
            f"Monitoring stopped by user",
            separator,
            f"{'Sr. No.':<10}{'Total Pods':<12}{'Running':<10}{'Pending':<10}{'CrashLoopBackOff':<18}{'Error':<10}{'Completed':<10}",
            separator,
            f"{iteration:<10}{total_pods:<12}{running:<10}{pending:<10}{crashloop:<18}{error:<10}{completed:<10}",
            separator
        ]
        print("\n")
        for line in table:
            print(f"{line:<{terminal_width}}")
        print("\n✅ Monitoring stopped by user.")
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ Pod monitoring completed")
    print(f"⏱️ Time taken to monitor pods: {minutes} minutes, {seconds} seconds")

def main():
    has_update, latest_version = check_for_updates()
    if has_update:
        logger.info(f"New version {latest_version} is available. Current version: {SCRIPT_VERSION}")
        print(f"New version {latest_version} is available. Current version:> {SCRIPT_VERSION}")
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
        print(f"Script is up-to-date. Running version: {SCRIPT_VERSION}")

    if len(sys.argv) != 3:
        print("❌ Usage: ./viya4_environment_restart.py <ConfigurationItem> <TicketNumber>")
        sys.exit(1)
    
    ci = sys.argv[1]
    ticket = sys.argv[2]
    
    print(f"\n🚀 Starting SAS Viya 4 Environment Restart Automation")
    print(f"Configuration Item: {ci}")
    print(f"Ticket Number: {ticket}")
    print(f"📝 Logs are written to: {log_file}\n")
    
    # Print index page
    print_index_page()
    
    env_vars = parse_ci(ci)
    for key, value in env_vars.items():
        os.environ[key] = value
    
    duration = input("⏱️ Enter maintenance duration (e.g., 2h): ")
    setup_zabbix_maintenance(ci, ticket, duration)
    
    list_running_pods(env_vars['NS'])
    
    create_backup_logs(env_vars['NS'], ticket)
    
    backup_consul_raft(env_vars['NS'], ticket)
    
    stop_sas_environment(env_vars['NS'], ticket)
    
    delete_jobs(env_vars['NS'], ticket)
    
    start_viya_environment(env_vars['NS'], ticket)
    
    verify_consul_pods(env_vars['NS'], ticket)
    
    monitor_pods(env_vars['NS'], ticket)
    
    print(f"\n🎉 Automation completed successfully!")

if __name__ == "__main__":
    main()