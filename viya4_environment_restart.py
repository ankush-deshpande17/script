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
SCRIPT_VERSION = "v1.5.0"  # Updated for Step 8
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

def countdown(seconds, message):
    """Display a countdown timer that updates in place"""
    for i in range(seconds, -1, -1):
        # Use \r to return to start of line, pad with spaces to clear previous output
        print(f"\r{message}: {i}  ", end="", flush=True)
        sys.stdout.flush()
        time.sleep(1)
    # Move to next line after countdown
    print()

# [Unchanged functions: parse_ci, setup_zabbix_maintenance, list_running_pods, collect_container_log, create_backup_logs, backup_consul_raft, stop_sas_environment, delete_jobs, start_viya_environment]
# For brevity, these are not repeated here but are included in the full script.

def verify_consul_pods(ns, ticket):
    """
    Step 8: Verifying Consul Server Pod
    Checks the health of sas-consul-server pods. If not all pods are 1/1 Running,
    scales down the statefulset, deletes raft.db from each PVC, and scales back up.
    Retries once if pods remain unhealthy.
    """
    print_step_header(8, "Verifying Consul Server Pod", "🔍")
    
    # Start timing
    start_time = time.time()
    
    def check_consul_health():
        """Check if all sas-consul-server pods are 1/1 Running"""
        cmd = ["kubectl", "get", "pods", "-n", ns, "-l", "app=sas-consul-server", "-o", "json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            pods_data = json.loads(result.stdout)
            consul_pods = pods_data.get("items", [])
            if not consul_pods:
                print(f"❌ Error: No sas-consul-server pods found in namespace {ns}")
                return False, []
            
            print("\n📋 Consul server pod status:")
            all_healthy = True
            pod_statuses = []
            for pod in consul_pods:
                pod_name = pod["metadata"]["name"]
                ready = pod["status"].get("ready", "0/0")
                status = pod["status"].get("phase", "Unknown")
                for container in pod["status"].get("containerStatuses", []):
                    state = container.get("state", {})
                    if "waiting" in state and state["waiting"].get("reason") == "CrashLoopBackOff":
                        status = "CrashLoopBackOff"
                    elif "terminated" in state and state["terminated"].get("exitCode", 0) != 0:
                        status = "Error"
                pod_statuses.append((pod_name, ready, status))
                if ready != "1/1" or status != "Running":
                    all_healthy = False
            
            for pod_name, ready, status in sorted(pod_statuses, key=lambda x: x[0]):
                status_icon = "✅" if ready == "1/1" and status == "Running" else "❌"
                print(f"- {pod_name}: {ready} {status} {status_icon}")
            
            return all_healthy, consul_pods
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Failed to check consul pod status: {e}")
            sys.exit(1)
    
    def remediate_consul():
        """Scale down consul, delete raft.db from PVCs, and scale up"""
        # Step 1: Scale down consul
        print("\n📉 Scaling down sas-consul-server to 0 replicas")
        cmd = ["kubectl", "-n", ns, "scale", "sts/sas-consul-server", "--replicas=0"]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("✅ Scaled down sas-consul-server")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to scale down sas-consul-server: {e.stderr}")
            sys.exit(1)
        
        # Wait for termination
        countdown(10, "⏳ Waiting for consul pods to terminate")
        
        # Check for lingering pods and forcefully delete
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
                        print(f"❌ Failed to delete pod {pod_name}: {e.stderr}")
            else:
                print("✅ No lingering consul pods found")
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"❌ Failed to check for lingering pods: {e}")
            sys.exit(1)
        
        # Step 2: Delete raft.db from each PVC
        pvc_names = [
            "sas-viya-consul-data-volume-sas-consul-server-0",
            "sas-viya-consul-data-volume-sas-consul-server-1",
            "sas-viya-consul-data-volume-sas-consul-server-2"
        ]
        
        for pvc_name in pvc_names:
            print(f"\n📀 Processing PVC: {pvc_name}")
            # Create temporary pod manifest
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
            
            # Write manifest to temporary file
            manifest_file = f"/tmp/{pod_name}.yaml"
            with open(manifest_file, "w") as f:
                json.dump(pod_manifest, f)
            
            # Create pod
            cmd = ["kubectl", "apply", "-f", manifest_file, "-n", ns]
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Created temporary pod: {pod_name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to create pod for PVC {pvc_name}: {e.stderr}")
                sys.exit(1)
            
            # Wait for pod to be Running
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
                print(f"❌ Error: Pod {pod_name} did not reach Running state")
                sys.exit(1)
            
            # Delete raft.db
            cmd = ["kubectl", "exec", "-n", ns, pod_name, "--", "rm", "-f", "/consul/data/raft/raft.db"]
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Deleted raft.db from PVC {pvc_name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to delete raft.db from PVC {pvc_name}: {e.stderr}")
                sys.exit(1)
            
            # Delete temporary pod
            cmd = ["kubectl", "delete", "pod", "-n", ns, pod_name, "--force", "--grace-period=0"]
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Deleted temporary pod: {pod_name}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to delete pod {pod_name}: {e.stderr}")
                sys.exit(1)
            
            # Clean up manifest file
            os.remove(manifest_file)
        
        # Step 3: Scale up consul
        print("\n📈 Scaling up sas-consul-server to 3 replicas")
        cmd = ["kubectl", "-n", ns, "scale", "sts/sas-consul-server", "--replicas=3"]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("✅ Scaled up sas-consul-server")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to scale up sas-consul-server: {e.stderr}")
            sys.exit(1)
        
        # Wait for pods to come up
        countdown(120, "⏳ Waiting for consul pods to start")
    
    # Main verification loop (with retry)
    max_retries = 1
    attempt = 0
    while attempt <= max_retries:
        all_healthy, consul_pods = check_consul_health()
        if all_healthy:
            print("\n✅ All sas-consul-server pods are healthy (1/1 Running)")
            break
        else:
            print(f"\n⚠️ Consul pods are not healthy (Attempt {attempt + 1}/{max_retries + 1})")
            remediate_consul()
            attempt += 1
            if attempt <= max_retries:
                print("\n🔄 Retrying consul health check after remediation")
    
    if not all_healthy:
        print("\n❌ Error: Consul pods are still not healthy after remediation attempts")
        sys.exit(1)
    
    # Calculate and display time taken
    end_time = time.time()
    duration_seconds = end_time - start_time
    minutes = int(duration_seconds // 60)
    seconds = int(duration_seconds % 60)
    print(f"\n✅ Consul server pod verification completed")
    print(f"⏱️ Time taken to verify consul pods: {minutes} minutes, {seconds} seconds")

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
    
    backup_consul_raft(env_vars['NS'], ticket)
    
    stop_sas_environment(env_vars['NS'], ticket)
    
    delete_jobs(env_vars['NS'], ticket)
    
    start_viya_environment(env_vars['NS'], ticket)
    
    verify_consul_pods(env_vars['NS'], ticket)
    
    print(f"\n🎉 Automation completed successfully!")

if __name__ == "__main__":
    main()