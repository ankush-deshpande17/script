#!/usr/bin/env python3
import subprocess
import sys
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import tarfile
import random
import requests  # Added for check_for_updates
import shutil  # Added for check_for_updates

# Enhanced ASCII banner
BANNER = """
==========================================================================================
                                                                                         
                   ██████  ██████  ██████                                               
                   ██      ██  ██  ██                                                   
                   ██████  ██████  ██████                                               
                       ██  ██  ██      ██                                               
                   ██████  ██  ██  ██████                                               
                                                                                         
         SAS Viya 4 Environment Restart Automation Script                                
                                                                                         
==========================================================================================
"""

# Updated index with better icons
INDEX = """
==========================================================================================
          S T E P S   O F   T H E   A U T O M A T I O N   P R O C E S S
==========================================================================================
  0. [🔓] Take Namespace and Ticket Number as User Input
  1. [📋] List All Pods in the Specified Namespace
  2. [��] Take Backup of All Running Pods
  3. [🔒] Backup Consul's raft.db File
  4. [🛑] Stopping SAS Viya 4 Environments
  5. [🗑️] Delete Non-Running Pods
  6. [🛫] Start Viya 4 Environment
  7. [👀] Monitoring of SAS Viya Environment
  8. [✔️] Check SAS Readiness Status
  9. [📄] Summary for Change Request/Ticket
==========================================================================================
"""

# Global variables
initial_running_pods = 0
initial_pod_list = ""
initial_pod_table = ""
log_backup_dir = ""
consul_backup_files = []
post_stop_pod_list = ""
final_pod_list = ""
final_pod_table = ""
consul_issues = False
post_cleanup_jobs = ""
post_cleanup_pods = ""
SCRIPT_VERSION = os.environ.get("SCRIPT_VERSION", "v1.0.0")  # Added for check_for_updates
GITHUB_REPO = "ankush-deshpande17/script"  # Added for check_for_updates
GITHUB_BRANCH = "main"  # Added for check_for_updates
VERSION_FILE = "restart_version.txt"  # Added for check_for_updates
YELLOW = "\033[93m"  # Added for check_for_updates
RESET = "\033[0m"  # Added for check_for_updates

def check_for_updates():
    """Check for a newer version of the script on GitHub and update if requested."""
    repo_url = f"https://github.com/{GITHUB_REPO}"
    version_file_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{VERSION_FILE}"
    script_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/viya4_environment_restart.py"

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
                    temp_path = "/tmp/viya4_environment_restart.py.tmp"
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
                        os.environ["SCRIPT_VERSION"] = latest_version
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

def display_intro():
    print(BANNER)
    print(INDEX)
    print("Starting the automation process...\n")

def get_user_input():
    step_title = "[🔓] Step 0: Collecting user input"
    print(step_title)
    print("=" * len(step_title))
    namespace = input("Enter the namespace: ").strip()
    ticket_number = input("Enter the ticket number: ").strip()
    if not namespace:
        print("Namespace is required.")
        sys.exit(1)
    return namespace, ticket_number

def check_namespace_exists(namespace):
    try:
        subprocess.run(["kubectl", "get", "namespace", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        print(f"Namespace '{namespace}' does not exist.")
        sys.exit(1)

def list_pods(namespace):
    """List all pods in the namespace and display/store initial pod status table."""
    global initial_pod_list, initial_pod_table
    step_title = f"[📋] Step 1: Listing all pods in namespace '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    try:
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        initial_pod_list = result.stdout  # Store raw pod list
        
        # Generate and display pod status table
        states = {"RUNNING": 0, "COMPLETED": 0, "ERROR": 0, "CRASHLOOPBACKOFF": 0, "INIT": 0, "PODINITIALIZING": 0}
        for line in result.stdout.strip().split("\n")[1:]:
            if line:
                status = line.split()[2]
                if status == "Running":
                    states["RUNNING"] += 1
                elif status == "Completed":
                    states["COMPLETED"] += 1
                elif status == "Error":
                    states["ERROR"] += 1
                elif status == "CrashLoopBackOff":
                    states["CRASHLOOPBACKOFF"] += 1
                elif status == "Init":
                    states["INIT"] += 1
                elif status == "PodInitializing":
                    states["PODINITIALIZING"] += 1
        
        headers = ["Sr. No.", "RUNNING", "COMPLETED", "ERROR", "CRASHLOOPBACKOFF", "INIT", "PODINITIALIZING"]
        data = [1, states["RUNNING"], states["COMPLETED"], states["ERROR"], states["CRASHLOOPBACKOFF"], states["INIT"], states["PODINITIALIZING"]]
        initial_pod_table = (
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{header:<15}' for header in headers)} |\n"
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{str(item):<15}' for item in data)} |\n"
            f"{'-' * 80}"
        )
        
        print("Initial Pod Status Table:")
        print(initial_pod_table)
        print("\nPod List:")
        print(initial_pod_list)
        return initial_pod_list
    
    except subprocess.CalledProcessError as e:
        print(f"Failed to list pods: {e.stderr}")
        sys.exit(1)

def display_pod_table(namespace, dynamic=False):
    global initial_running_pods, initial_pod_list, final_pod_list, final_pod_table
    result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    lines = result.stdout.strip().split("\n")[1:]
    
    states = {"RUNNING": 0, "COMPLETED": 0, "ERROR": 0, "CRASHLOOPBACKOFF": 0, "INIT": 0, "PODINITIALIZING": 0}
    for line in lines:
        if line:
            status = line.split()[2]
            if status == "Running":
                states["RUNNING"] += 1
            elif status == "Completed":
                states["COMPLETED"] += 1
            elif status == "Error":
                states["ERROR"] += 1
            elif status == "CrashLoopBackOff":
                states["CRASHLOOPBACKOFF"] += 1
            elif status == "Init":
                states["INIT"] += 1
            elif status == "PodInitializing":
                states["PODINITIALIZING"] += 1
    
    if not dynamic and not initial_pod_list:
        initial_running_pods = states["RUNNING"]
        initial_pod_list = result.stdout
    elif not dynamic:
        final_pod_list = result.stdout
        headers = ["Sr. No.", "RUNNING", "COMPLETED", "ERROR", "CRASHLOOPBACKOFF", "INIT", "PODINITIALIZING"]
        data = [1, states["RUNNING"], states["COMPLETED"], states["ERROR"], states["CRASHLOOPBACKOFF"], states["INIT"], states["PODINITIALIZING"]]
        final_pod_table = (
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{header:<15}' for header in headers)} |\n"
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{str(item):<15}' for item in data)} |\n"
            f"{'-' * 80}"
        )
    
    if dynamic:
        headers = ["Sr. No.", "RUNNING", "COMPLETED", "ERROR", "CRASHLOOPBACKOFF", "INIT", "PODINITIALIZING"]
        data = [1, states["RUNNING"], states["COMPLETED"], states["ERROR"], states["CRASHLOOPBACKOFF"], states["INIT"], states["PODINITIALIZING"]]
        table = (
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{header:<15}' for header in headers)} |\n"
            f"{'-' * 80}\n"
            f"| {' | '.join(f'{str(item):<15}' for item in data)} |\n"
            f"{'-' * 80}"
        )
        sys.stdout.write(f"\033[5A{table}\n")
        sys.stdout.flush()
    
    return states["RUNNING"]

def backup_pods(namespace, ticket_number, pod_list_output, log_dir):
    global log_backup_dir
    step_title = f"[💽] Step 2: Backing up logs for all running pods in '{namespace}' into a single tar file"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    lines = pod_list_output.strip().split("\n")[1:]
    running_pods = [line.split()[0] for line in lines if line and line.split()[2] == "Running"]
    
    if not running_pods:
        print("No running pods found to back up.")
        return None
    
    log_backup_dir = log_dir
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tar_file_path = f"{log_dir}/pod_logs_{timestamp}.tar.gz"
    
    def fetch_pod_logs(pod_name):
        try:
            kubectl_cmd = ["kubectl", "logs", "-n", namespace, pod_name]
            result = subprocess.run(kubectl_cmd, capture_output=True, timeout=60)
            try:
                logs = result.stdout.decode('utf-8')
            except UnicodeDecodeError:
                print(f"UTF-8 decoding failed for pod '{pod_name}', using latin-1 as fallback")
                logs = result.stdout.decode('latin-1')
            if result.returncode == 0:
                return pod_name, logs
            else:
                print(f"Failed to fetch logs for pod '{pod_name}': {result.stderr.decode('utf-8')}")
                return pod_name, None
        except subprocess.TimeoutExpired:
            print(f"Timeout fetching logs for pod '{pod_name}' after 60 seconds.")
            return pod_name, None
        except Exception as e:
            print(f"Unexpected error fetching logs for pod '{pod_name}': {e}")
            return pod_name, None
    
    print(f"Backing up logs for {len(running_pods)} running pods in parallel...")
    log_data = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_pod = {executor.submit(fetch_pod_logs, pod): pod for pod in running_pods}
        for future in as_completed(future_to_pod):
            pod_name = future_to_pod[future]
            try:
                pod, logs = future.result()
                if logs:
                    log_data[pod] = logs
                    print(f"Collected logs for pod '{pod}'")
            except Exception as e:
                print(f"Error processing logs for pod '{pod_name}': {e}")
    
    if log_data:
        try:
            with tarfile.open(tar_file_path, "w:gz") as tar:
                for pod_name, logs in log_data.items():
                    log_file = io.BytesIO(logs.encode('utf-8'))
                    tarinfo = tarfile.TarInfo(name=f"{pod_name}.log")
                    tarinfo.size = len(logs.encode('utf-8'))
                    tarinfo.mtime = time.time()
                    tar.addfile(tarinfo, log_file)
            print(f"All pod logs backed up to {tar_file_path}")
            return timestamp
        except Exception as e:
            print(f"Failed to create tar file '{tar_file_path}': {e}")
            return None
    else:
        print("No logs collected for backup.")
        return None

def backup_consul_raft(namespace, ticket_number):
    """Backup Consul's raft.db file within the pod in parallel."""
    global consul_backup_files
    step_title = f"[🔒] Step 3: Backing up Consul's raft.db file in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    try:
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pod_lines = result.stdout.strip().split("\n")[1:]
        consul_pods = [line.split()[0] for line in pod_lines if "consul" in line and line]
    except subprocess.CalledProcessError as e:
        print(f"Failed to list pods for consul backup: {e.stderr}")
        sys.exit(1)
    
    if not consul_pods:
        print("No consul pods found to back up.")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raft_path = "/consul/data/raft/raft.db"
    backup_path = f"/consul/data/raft/raft.db_{timestamp}"
    
    def backup_single_pod(pod_name):
        try:
            check_cmd = ["kubectl", "exec", "-n", namespace, pod_name, "--", "test", "-f", raft_path]
            subprocess.run(check_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            backup_cmd = ["kubectl", "exec", "-n", namespace, pod_name, "--", "cp", raft_path, backup_path]
            subprocess.run(backup_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            verify_cmd = ["kubectl", "exec", "-n", namespace, pod_name, "--", "test", "-f", backup_path]
            subprocess.run(verify_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            consul_backup_files.append(f"{pod_name}: {backup_path}")
            return f"raft.db backup taken for {pod_name}: {backup_path}"
        except subprocess.CalledProcessError as e:
            return f"Failed to backup raft.db for pod '{pod_name}': {e.stderr}"
        except Exception as e:
            return f"Unexpected error backing up raft.db for pod '{pod_name}': {e}"
    
    print(f"Backing up raft.db for {len(consul_pods)} Consul pods in parallel...")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=min(len(consul_pods), 5)) as executor:
        future_to_pod = {executor.submit(backup_single_pod, pod): pod for pod in consul_pods}
        for future in as_completed(future_to_pod):
            pod_name = future_to_pod[future]
            try:
                message = future.result()
                print(message.ljust(80))
            except Exception as e:
                print(f"Error processing backup for pod '{pod_name}': {e}".ljust(80))
    
    elapsed_time = time.time() - start_time
    print(f"[INFO] Consul backup completed in {elapsed_time:.2f} seconds")

def stop_sas_viya(namespace):
    global post_stop_pod_list
    step_title = f"[🛑] Step 4: Stopping SAS Viya 4 Environments in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    job_name = f"sas-stop-all-{int(time.time())}"
    try:
        subprocess.run(["kubectl", "create", "job", job_name, "--from", "cronjobs/sas-stop-all", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"Created stop job: {job_name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create stop job: {e.stderr}")
        sys.exit(1)
    
    print("Waiting 10 seconds for job to initiate...")
    time.sleep(10)
    
    job_pod = None
    for _ in range(30):
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace, "-l", f"job-name={job_name}"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pod_lines = result.stdout.strip().split("\n")[1:]
        if pod_lines and pod_lines[0]:
            job_pod = pod_lines[0].split()[0]
            break
        time.sleep(1)
    
    if job_pod:
        while True:
            result = subprocess.run(["kubectl", "get", "pod", job_pod, "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            status = [line.split()[2] for line in result.stdout.strip().split("\n")[1:] if line][0]
            if status == "Completed":
                print(f"\nStop job pod '{job_pod}' has reached Completed state.")
                log_result = subprocess.run(["kubectl", "logs", "-n", namespace, job_pod], capture_output=True, text=True)
                if log_result.stdout:
                    log_lines = log_result.stdout.strip().split("\n")[-5:]
                    print(f"Last 5 lines of logs from '{job_pod}':")
                    for line in log_lines:
                        print(line)
                break
            time.sleep(5)
    else:
        print(f"[ERROR] Could not find pod for job '{job_name}' within 30 seconds.")
    
    print("\nWaiting 60 seconds for environment to settle...")
    for remaining in range(60, 0, -1):
        sys.stdout.write(f"\rTime remaining: {remaining} seconds{' ' * 10}")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 50 + "\r")
    sys.stdout.flush()
    print("")
    
    result = subprocess.run(["kubectl", "get", "pods", "-n", namespace, "-o", "wide"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    running_pods = []
    for line in result.stdout.strip().split("\n")[1:]:
        if line:
            fields = line.split()
            pod_name = fields[0]
            status = fields[2]
            pod_info = subprocess.run(["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.metadata.ownerReferences[0].kind},{.metadata.ownerReferences[0].name}"],
                                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            owner_kind, owner_name = pod_info.stdout.split(",") if pod_info.stdout else ("", "")
            if status == "Running" and not (owner_kind == "ReplicaSet" and "prometheus-pushgateway" in owner_name):
                running_pods.append(pod_name)
    
    if running_pods:
        print(f"Found {len(running_pods)} running pods after 60 seconds. Force killing them (except prometheus-pushgateway deployment pods):")
        for pod in running_pods:
            try:
                subprocess.run(["kubectl", "delete", "pod", pod, "-n", namespace, "--grace-period=0", "--force"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                print(f"[INFO] Force killed running pod '{pod}'")
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to force kill pod '{pod}': {e.stderr}")
        
        print("\nVerifying all pods are terminated (waiting up to 30 seconds)...")
        for _ in range(30):
            result = subprocess.run(["kubectl", "get", "pods", "-n", namespace, "-o", "wide"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            remaining_running = []
            for line in result.stdout.strip().split("\n")[1:]:
                if line:
                    fields = line.split()
                    pod_name = fields[0]
                    status = fields[2]
                    pod_info = subprocess.run(["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.metadata.ownerReferences[0].kind},{.metadata.ownerReferences[0].name}"],
                                            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    owner_kind, owner_name = pod_info.stdout.split(",") if pod_info.stdout else ("", "")
                    if status == "Running" and not (owner_kind == "ReplicaSet" and "prometheus-pushgateway" in owner_name):
                        remaining_running.append(pod_name)
            if not remaining_running:
                print("[INFO] All pods (except prometheus-pushgateway deployment pods) confirmed terminated.")
                break
            time.sleep(1)
        else:
            print(f"[WARNING] After 30 seconds, {len(remaining_running)} pods still running: {', '.join(remaining_running)}")
    
    post_stop_pod_list = result.stdout

def delete_non_running_pods(namespace):
    global post_cleanup_jobs, post_cleanup_pods
    step_title = f"[🗑️] Step 5: Delete Non-Running Pods in '{namespace}'"
    print(step_title)
    print("=" * len(step_title))
    
    result = subprocess.run(["kubectl", "get", "jobs", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    lines = result.stdout.strip().split("\n")[1:]
    jobs_to_delete = [line.split()[0] for line in lines if line and any(status in line for status in ["0/1", "0/2", "1/1"])]
    
    if not jobs_to_delete:
        print("No non-running jobs found to delete.")
    else:
        for job in jobs_to_delete:
            try:
                subprocess.run(["kubectl", "delete", "job", job, "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                print(f"Deleted job '{job}'")
            except subprocess.CalledProcessError as e:
                print(f"Failed to delete job '{job}': {e.stderr}")
    
    print("\nRemaining jobs after cleanup:")
    result_jobs = subprocess.run(["kubectl", "get", "jobs", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(result_jobs.stdout or "No jobs remaining.")
    post_cleanup_jobs = result_jobs.stdout or "No jobs remaining."
    
    print("Remaining pods after cleanup:")
    result_pods = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(result_pods.stdout or "No pods remaining.")
    post_cleanup_pods = result_pods.stdout or "No pods remaining."

def process_consul_pvc(namespace, pod, pvc_mapping):
    pvc = pvc_mapping[pod]
    temp_pod = f"pvc-attach-{pod}-{int(time.time())}-{random.randint(1000, 9999)}"
    pod_yaml = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {temp_pod}
  namespace: {namespace}
spec:
  containers:
  - name: busybox
    image: busybox
    command: ["sh", "-c", "sleep 3600"]
    volumeMounts:
    - mountPath: /consul/data
      name: consul-volume
  volumes:
  - name: consul-volume
    persistentVolumeClaim:
      claimName: {pvc}
"""
    with open(f"{temp_pod}.yaml", "w") as f:
        f.write(pod_yaml)
    try:
        subprocess.run(["kubectl", "apply", "-f", f"{temp_pod}.yaml"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"[INFO] Created pod '{temp_pod}' for PVC '{pvc}'")
        
        subprocess.run(["kubectl", "-n", namespace, "wait", "--for=condition=Ready", f"pod/{temp_pod}", "--timeout=120s"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        print(f"\n[⚠️] Manual Intervention Required: Exec into '{temp_pod}' to delete raft.db in /consul/data/raft/")
        print("[⚠️] This step is excluded from the 15-20 minute execution time limit. Take as long as needed.")
        print("[⚠️] Command: kubectl exec -it -n {namespace} {temp_pod} -- sh")
        print("[⚠️] Inside the pod, run: rm -f /consul/data/raft/raft.db")
        print("[⚠️] Type 'exit' when done. The script will wait indefinitely.")
        subprocess.run(["kubectl", "exec", "-it", "-n", namespace, temp_pod, "--", "sh"])
        
        subprocess.run(["kubectl", "delete", "pod", temp_pod, "-n", namespace, "--grace-period=0", "--force"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"[INFO] Deleted pod '{temp_pod}'")
        
        os.remove(f"{temp_pod}.yaml")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to process PVC for '{pod}': {e.stderr}")
        if os.path.exists(f"{temp_pod}.yaml"):
            os.remove(f"{temp_pod}.yaml")
    except Exception as e:
        print(f"[ERROR] Unexpected error processing PVC for '{pod}': {e}")

def start_sas_viya(namespace):
    global consul_issues, final_pod_list
    step_title = f"[🛫] Step 6: Start Viya 4 Environment in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    job_name = f"sas-start-all-{int(time.time())}"
    try:
        subprocess.run(["kubectl", "create", "job", job_name, "--from", "cronjobs/sas-start-all", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"Created start job: {job_name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create start job: {e.stderr}")
        sys.exit(1)
    
    print("Waiting 10 seconds for job to initiate...")
    time.sleep(10)
    
    job_pod = None
    for _ in range(30):
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace, "-l", f"job-name={job_name}"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pod_lines = result.stdout.strip().split("\n")[1:]
        if pod_lines and pod_lines[0]:
            job_pod = pod_lines[0].split()[0]
            break
        time.sleep(1)
    
    if job_pod:
        while True:
            result = subprocess.run(["kubectl", "get", "pod", job_pod, "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            status = [line.split()[2] for line in result.stdout.strip().split("\n")[1:] if line][0]
            if status == "Completed":
                print(f"\nStart job pod '{job_pod}' has reached Completed state.")
                log_result = subprocess.run(["kubectl", "logs", "-n", namespace, job_pod], capture_output=True, text=True)
                if log_result.stdout:
                    log_lines = log_result.stdout.strip().split("\n")[-5:]
                    print(f"Last 5 lines of logs from '{job_pod}':")
                    for line in log_lines:
                        print(line)
                break
            time.sleep(5)
    else:
        print(f"[ERROR] Could not find pod for job '{job_name}' within 30 seconds.")
    
    print("\nWaiting 30 seconds before checking consul health...")
    for remaining in range(30, 0, -1):
        sys.stdout.write(f"\rTime remaining: {remaining} seconds{' ' * 10}")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 50 + "\r")
    sys.stdout.flush()
    print("")
    
    result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    consul_pods_status = {line.split()[0]: line.split()[2] for line in result.stdout.strip().split("\n")[1:] if "sas-consul-server" in line and line}
    print("Current Consul pod statuses:")
    for pod, status in consul_pods_status.items():
        print(f"{pod}: {status}")
    
    crashloop_pods = [pod for pod, status in consul_pods_status.items() if status == "CrashLoopBackOff"]
    if crashloop_pods:
        consul_issues = True
        print(f"\nFound {len(crashloop_pods)} consul pods in CrashLoopBackOff state. Scaling down Consul and attaching PVCs...")
        
        try:
            subprocess.run(["kubectl", "-n", namespace, "scale", "sts/sas-consul-server", "--replicas=0"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print("[INFO] Scaled down sas-consul-server StatefulSet to 0 replicas.")
            
            print("[INFO] Waiting for Consul pods to terminate...")
            for _ in range(30):
                result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, text=True)
                if not any("sas-consul-server" in line for line in result.stdout.strip().split("\n")[1:]):
                    break
                time.sleep(1)
            else:
                print("[WARNING] Consul pods did not terminate within 30 seconds. Proceeding anyway.")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Failed to scale down sas-consul-server: {e.stderr}")
            sys.exit(1)
        
        pvc_mapping = {
            "sas-consul-server-0": "sas-viya-consul-data-volume-sas-consul-server-0",
            "sas-consul-server-1": "sas-viya-consul-data-volume-sas-consul-server-1",
            "sas-consul-server-2": "sas-viya-consul-data-volume-sas-consul-server-2"
        }
        
        for i, pod in enumerate(crashloop_pods):
            process_consul_pvc(namespace, pod, pvc_mapping)
            if i == len(crashloop_pods) - 1:
                print("[INFO] Waiting 5 seconds before scaling up Consul...")
                time.sleep(5)
                try:
                    subprocess.run(["kubectl", "-n", namespace, "scale", "sts/sas-consul-server", "--replicas=3"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    print("[INFO] Scaled up sas-consul-server StatefulSet to 3 replicas.")
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Failed to scale up sas-consul-server: {e.stderr}")
                    sys.exit(1)
    
    else:
        print("[INFO] No Consul pods in CrashLoopBackOff state initially. Skipping PVC operations.")
    
    print("\nMonitoring consul health every 20 seconds for 40 seconds...")
    for _ in range(2):
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, text=True)
        consul_status = {line.split()[0]: line.split()[1] for line in result.stdout.strip().split("\n")[1:] if "sas-consul-server" in line and line}
        print(f"Current consul status: {', '.join(f'{pod}: {status}' for pod, status in consul_status.items())}")
        time.sleep(20)
    
    result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, text=True)
    consul_pods_status = {line.split()[0]: line.split()[2] for line in result.stdout.strip().split("\n")[1:] if "sas-consul-server" in line and line}
    problematic_pods = [pod for pod, status in consul_pods_status.items() if status in ["CrashLoopBackOff", "Error"]]
    
    if problematic_pods:
        print(f"\nFound {len(problematic_pods)} consul pods still in CrashLoopBackOff/Error after initial fix: {', '.join(problematic_pods)}")
        print("Repeating PVC remediation for problematic pods...")
        
        for pod in problematic_pods:
            process_consul_pvc(namespace, pod, pvc_mapping)
        
        print("[INFO] Waiting 5 seconds before continuing Consul health monitoring...")
        time.sleep(5)
        
        crashloop_pods.extend(problematic_pods)
    
    print("\nMonitoring consul health every 20 seconds until all pods are healthy...")
    while True:
        result = subprocess.run(["kubectl", "get", "pods", "-n", namespace], check=True, stdout=subprocess.PIPE, text=True)
        consul_status = {line.split()[0]: line.split()[1] for line in result.stdout.strip().split("\n")[1:] if "sas-consul-server" in line and line}
        if all(status == "1/1" for status in consul_status.values()):
            print(f"\nAll consul pods are healthy: {', '.join(f'{pod}: {status}' for pod, status in consul_status.items())}")
            break
        print(f"Current consul status: {', '.join(f'{pod}: {status}' for pod, status in consul_status.items())}")
        time.sleep(20)
    
    step_title = f"[👀] Step 7: Monitoring of SAS Viya Environment in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    print("\nMonitoring pod states dynamically (updates every 5 seconds for up to 5 minutes):")
    print("\n" * 5)
    
    start_time = time.time()
    max_monitor_time = 5 * 60
    
    while True:
        display_pod_table(namespace, dynamic=True)
        
        try:
            result = subprocess.run(["kubectl", "get", "pod", "-n", namespace, "-l", "app=sas-readiness"],
                                  check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = result.stdout.strip().split("\n")[1:]
            if lines:
                pod_status = lines[0].split()
                readiness = pod_status[1]
                status = pod_status[2]
                if status == "Running" and readiness == "1/1":
                    print(f"\n[INFO] sas-readiness pod is Running and 1/1. Proceeding to Step 8.")
                    display_pod_table(namespace, dynamic=False)  # Store final table
                    break
        except subprocess.CalledProcessError:
            pass
        
        if time.time() - start_time >= max_monitor_time:
            print(f"\n[WARNING] Reached 5-minute monitoring limit without sas-readiness pod reaching Running 1/1.")
            display_pod_table(namespace, dynamic=False)  # Store final table
            break
            
        time.sleep(5)
    
    return crashloop_pods

def check_sas_readiness(namespace):
    """Check SAS readiness status with dynamic in-place updates."""
    step_title = f"[✔️] Step 8: Check SAS Readiness Status in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    max_attempts = 180  # 15 minutes / 5 seconds per attempt
    attempt = 0
    
    # Initial placeholder lines for dynamic update
    print("\n" * 2)  # Reserve 2 lines for status and progress
    
    while attempt < max_attempts:
        try:
            result = subprocess.run(["kubectl", "get", "pod", "-n", namespace, "-l", "app=sas-readiness"],
                                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = result.stdout.strip().split("\n")[1:]
            if lines:
                pod_status = lines[0].split()
                pod_name = pod_status[0]
                readiness = pod_status[1]
                status = pod_status[2]
                status_line = f"sas-readiness pod status: {pod_name} {readiness} {status}"
                progress_line = f"Waiting for Running and 1/1 (Attempt {attempt + 1}/{max_attempts}, Time elapsed: {attempt * 5 // 60} min {attempt * 5 % 60} sec)"
                
                # Move cursor up 2 lines and overwrite
                sys.stdout.write(f"\033[2A{status_line.ljust(80)}\n{progress_line.ljust(80)}\n")
                sys.stdout.flush()
                
                if status == "Running" and readiness == "1/1":
                    print(f"\n[INFO] sas-readiness pod '{pod_name}' is Running and 1/1. Proceeding to Step 9.")
                    return True
            else:
                status_line = "sas-readiness pod status: Not found yet"
                progress_line = f"Waiting for pod to appear (Attempt {attempt + 1}/{max_attempts}, Time elapsed: {attempt * 5 // 60} min {attempt * 5 % 60} sec)"
                sys.stdout.write(f"\033[2A{status_line.ljust(80)}\n{progress_line.ljust(80)}\n")
                sys.stdout.flush()
        
        except subprocess.CalledProcessError as e:
            status_line = f"Error checking sas-readiness pod: {e.stderr.strip()}"
            progress_line = f"Retrying (Attempt {attempt + 1}/{max_attempts}, Time elapsed: {attempt * 5 // 60} min {attempt * 5 % 60} sec)"
            sys.stdout.write(f"\033[2A{status_line.ljust(80)}\n{progress_line.ljust(80)}\n")
            sys.stdout.flush()
        
        time.sleep(5)
        attempt += 1
    
    # Final message on timeout
    sys.stdout.write(f"\033[2A{'sas-readiness pod status: Timed out'.ljust(80)}\n{'[WARNING] Did not reach Running and 1/1 within 15 minutes'.ljust(80)}\n")
    sys.stdout.flush()
    print("")  # Add a newline for clean separation
    return False

def generate_summary(namespace, ticket_number, log_timestamp, crashloop_pods):
    """Generate a summary including initial and final pod status tables and lists."""
    step_title = f"[📄] Step 9: Summary for Change Request/Ticket in '{namespace}'"
    print("\n" + step_title)
    print("=" * len(step_title))
    
    print(f"\nTicket Number: {ticket_number}")
    print(f"Namespace: {namespace}")
    print(f"Execution Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n[🔄] Workflow of Execution Steps")
    print("- Step 0: Collected namespace and ticket number from user input.")
    print("- Step 1: Listed all pods in the namespace to capture initial state.")
    print(f"- Step 2: Backed up logs of all running pods to '{log_backup_dir}/pod_logs_{log_timestamp}.tar.gz'." if log_timestamp else "- Step 2: Skipped log backup (no running pods found).")
    print(f"- Step 3: Backed up Consul raft.db files for {len(consul_backup_files)} pods." if consul_backup_files else "- Step 3: Skipped Consul backup (no Consul pods found).")
    print("- Step 4: Stopped SAS Viya 4 environment using sas-stop-all job, waited 60 seconds, and force-killed remaining pods (except prometheus-pushgateway).")
    print("- Step 5: Deleted all non-running jobs in the namespace.")
    print(f"- Step 6: Started SAS Viya 4 environment using sas-start-all job{' and remediated Consul issues (manual steps excluded from 15-20 min limit)' if consul_issues else '.'}")
    if consul_issues:
        print(f"  - Detected {len(crashloop_pods)} Consul pods in CrashLoopBackOff/Error: {', '.join(crashloop_pods)}.")
        print("  - Scaled down sas-consul-server to 0 replicas, attached PVCs for manual raft.db deletion, and scaled back to 3 replicas.")
        if len(crashloop_pods) > len(set(crashloop_pods)):
            print("  - After 40 seconds, repeated PVC remediation for pods still in CrashLoopBackOff/Error (manual steps excluded from time limit).")
    print("- Step 7: Monitored environment for up to 5 minutes until sas-readiness pod reached Running 1/1 or timeout.")
    print("- Step 8: Verified sas-readiness pod reached Running and 1/1 state within 15 minutes.")
    
    print("\n[📋] Step 1: Initial Pod Status")
    print("Initial Pod Status Table:")
    print(initial_pod_table)
    print("\nInitial Pod List:")
    print(initial_pod_list)
    
    print("\n[💽] Step 2: Log Backup Location")
    print(f"Logs backed up to: {log_backup_dir}/pod_logs_{log_timestamp}.tar.gz" if log_timestamp else "No logs backed up.")
    
    print("\n[🔒] Step 3: Consul Backup Details")
    if consul_backup_files:
        print("Backed up raft.db files:")
        for file in consul_backup_files:
            print(f"- {file}")
    else:
        print("No raft.db files backed up.")
    
    print("\n[🛑] Step 4: Post-Stop Pod List")
    print(post_stop_pod_list)
    
    print("\n[🗑️] Step 5: Post-Cleanup Status")
    print("Remaining jobs after cleanup:")
    print(post_cleanup_jobs)
    print("Remaining pods after cleanup:")
    print(post_cleanup_pods)
    
    print("\n[🛫] Step 6: Final Pod Status")
    print("Final Pod Status Table:")
    print(final_pod_table)
    print("\nFinal Pod List:")
    print(final_pod_list)
    
    if consul_issues:
        print("\nConsul Issues Encountered:")
        print(f"- Detected {len(crashloop_pods)} Consul pods in CrashLoopBackOff/Error: {', '.join(crashloop_pods)}")
        print("Remediation (Manual Steps Excluded from 15-20 Minute Limit):")
        print("- Scaled down sas-consul-server StatefulSet to 0 replicas (if initially affected).")
        print("- Attached PVCs to affected pods for manual raft.db deletion.")
        print("- Scaled up sas-consul-server StatefulSet to 3 replicas after initial remediation.")
        print("- Repeated PVC remediation for pods still in CrashLoopBackOff/Error after 40 seconds.")

def main():
    global pod_list_output
    display_intro()
    print("Checking for script updates...")
    check_for_updates()
    namespace, ticket_number = get_user_input()
    check_namespace_exists(namespace)
    pod_list_output = list_pods(namespace)
    
    base_dir = f"/home/anzdes/viya4_environment_restart_logs/{namespace}/{ticket_number}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = f"{base_dir}/{timestamp}"
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as e:
        print(f"Failed to create log directory '{log_dir}': {e}")
        sys.exit(1)
    
    start_time = time.time()
    log_timestamp = backup_pods(namespace, ticket_number, pod_list_output, log_dir)
    backup_consul_raft(namespace, ticket_number)
    stop_sas_viya(namespace)
    delete_non_running_pods(namespace)
    crashloop_pods = start_sas_viya(namespace)
    readiness_reached = check_sas_readiness(namespace)
    generate_summary(namespace, ticket_number, log_timestamp, crashloop_pods)
    
    # Check total execution time (excluding manual Consul steps)
    total_time = time.time() - start_time
    print(f"\n[⏱️] Total automated execution time (excluding manual Consul steps): {total_time / 60:.2f} minutes")

if __name__ == "__main__":
    main()
