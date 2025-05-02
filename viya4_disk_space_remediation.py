#!/usr/bin/env python3

import subprocess
import sys
import os
import json
from datetime import datetime, timedelta, timezone
import re
import time
import matplotlib.pyplot as plt
from tabulate import tabulate

# Big ASCII Banner
BANNER = """
==========================================================================================
                                                                                         
                   ██████  ██████  ██████                                               
                   ██      ██  ██  ██                                                   
                   ██████  ██████  ██████                                               
                       ██  ██  ██      ██                                               
                   ██████  ██  ██  ██████                                               
                                                                                         
         SAS Viya 4 Disk Space Automation Script                                
                                                                                         
==========================================================================================
"""

# Index Page with Steps and Icons
INDEX = """
==========================================================================================
          S T E P S   O F   T H E   A U T O M A T I O N   P R O C E S S
==========================================================================================
  0. [🔑] Get Namespace and Resource Group from Arguments
  1. [✅] Check Prerequisites (kubectl, kubectl-pgo, az, and utilities)
  2. [📋] List All Postgres Clusters
  3. [🔍] Check Pods for Each Postgres Cluster
  4. [👑] Find the Leader in the Postgres Cluster
  5. [💽] Check Disk Space in /pgdata on Any Pod
  6. [📊] Execute psql Queries to List Top 10 Databases
  7. [🗄️] Validate Backups with kubectl and Show Status
  8. [📸] Check Azure NetApp Files Volume Snapshots and Metrics
  9. [📂] Check /sasviyabackup Directory Size
 10. [🗑️] Manage and Delete Old Backups (Retain Last Successful)
 11. [🔗] Attach PVC and Verify Data
==========================================================================================
"""

# Global variables
NAMESPACE = ""
RGN = ""
AKSN = ""
ANF_ACCOUNT = ""
ANF_POOL = "ultra"
POSTGRES_CLUSTERS = []
BACKUP_LIST = []
TOTAL_SIZE = ""
MOUNT_TOTAL_SIZE = ""
POD_NAME = ""
SCHEDULED_BACKUPS = []
STEP10_DELETIONS = {
    "count": 0, "space_reclaimed": 0.0, "before_size": "", "after_size": "",
    "mount_before": "", "mount_after": ""
}
STEP11_DELETIONS = {
    "count": 0, "space_reclaimed": 0.0, "before_size": "", "after_size": "",
    "mount_before": "", "mount_after": ""
}

# Conversion functions
def bytes_to_human_readable(size_bytes):
    """Convert bytes to GiB or TiB."""
    gib = size_bytes / (1024 ** 3)
    tib = size_bytes / (1024 ** 4)
    if tib >= 1:
        return f"{tib:.2f} TiB"
    return f"{gib:.2f} GiB"

def bytes_to_gib(size_bytes):
    """Convert bytes to GiB for graphing."""
    return size_bytes / (1024 ** 3)

def run_command(command, shell=False, capture_output=True):
    """Run a shell command and handle errors."""
    try:
        if capture_output:
            result = subprocess.run(command, shell=shell, check=True, text=True, capture_output=True)
            return result.stdout.strip()
        else:
            subprocess.run(command, shell=shell, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed: {' '.join(command if not shell else [command])}\nOutput: {e.stderr}")
        return None

def run_az_command(command):
    """Run an az command and return parsed output."""
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.stderr:
        print(f"[ERROR] {result.stderr}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        output = result.stdout.strip()
        if not output:
            return []
        return output.splitlines()

def parse_size(size_str):
    """Convert a size string (e.g., '45G', '1.0T') to gigabytes."""
    size_str = size_str.strip()
    if not size_str:
        return 0.0
    unit = size_str[-1].upper()
    value = float(size_str[:-1])
    if unit == 'T':
        return value * 1024  # Convert TiB to GiB
    elif unit == 'G':
        return value
    elif unit == 'M':
        return value / 1024
    elif unit == 'K':
        return value / (1024 * 1024)
    else:
        raise ValueError(f"Unknown size unit in '{size_str}'")

# Steps
def get_namespace_and_rgn():
    """Step 0: Get TLA and ENV from arguments, set namespace, RGN, AKSN, and ANF params with manual override option."""
    print("========================================")
    print("🔑 STEP 0: GET NAMESPACE AND RESOURCE GROUP FROM ARGUMENTS")
    print("========================================")
    
    global NAMESPACE, RGN, AKSN, ANF_ACCOUNT
    if len(sys.argv) != 3:
        print("[ERROR] Usage: ./script.py TLA ENV (e.g., ./script.py smf prod)")
        sys.exit(1)
    
    tla = sys.argv[1].lower()
    env = sys.argv[2].lower()
    
    default_namespace = f"{tla}{env}"
    default_rgn = f"{tla}-{env}"
    default_aksn = f"{tla}-{env}-aks"
    default_anf_account = f"{tla}-{env}"
    
    print(f"[INFO] Default Namespace: {default_namespace}")
    print(f"[INFO] Default Resource Group Name (RGN): {default_rgn}")
    print(f"[INFO] Default AKS Cluster Name (AKSN): {default_aksn}")
    print(f"[INFO] Default ANF Account Name: {default_anf_account}")
    print(f"[INFO] ANF Pool Name (fixed): {ANF_POOL}")
    
    override = input("\nDo you want to override the default values? (y/n, default is n): ").strip().lower()
    if override == 'y':
        NAMESPACE = input(f"Enter Namespace (default: {default_namespace}): ").strip() or default_namespace
        RGN = input(f"Enter Resource Group Name (RGN) (default: {default_rgn}): ").strip() or default_rgn
        AKSN = input(f"Enter AKS Cluster Name (AKSN) (default: {default_aksn}): ").strip() or default_aksn
        ANF_ACCOUNT = input(f"Enter ANF Account Name (default: {default_anf_account}): ").strip() or default_anf_account
    else:
        NAMESPACE, RGN, AKSN, ANF_ACCOUNT = default_namespace, default_rgn, default_aksn, default_anf_account
    
    print(f"\n[INFO] Final Namespace set to: {NAMESPACE}")
    print(f"[INFO] Final Resource Group Name (RGN) set to: {RGN}")
    print(f"[INFO] Final AKS Cluster Name (AKSN) set to: {AKSN}")
    print(f"[INFO] Final ANF Account Name set to: {ANF_ACCOUNT}")
    print(f"[INFO] ANF Pool Name set to: {ANF_POOL}")
    print("")

def check_prerequisites():
    """Step 1: Verify kubectl, kubectl-pgo, az, and utilities."""
    print("========================================")
    print("✅ STEP 1: CHECKING PREREQUISITES")
    print("========================================")

    tools = ["kubectl", "kubectl-pgo", "az"]
    missing_tools = [tool for tool in tools if not run_command(["which", tool])]
    
    if missing_tools:
        print(f"[ERROR] The following required tools are missing: {' '.join(missing_tools)}")
        print("[INFO] Please install them and try again.")
        sys.exit(1)
    
    print("[INFO] Prerequisites verified.")
    print("")

def list_postgres_clusters():
    """Step 2: Retrieve Postgres clusters in the namespace."""
    print("========================================")
    print("📋 STEP 2: LISTING ALL POSTGRES CLUSTERS")
    print("========================================")

    global POSTGRES_CLUSTERS
    output = run_command(["kubectl", "-n", NAMESPACE, "get", "postgrescluster", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"])
    if not output:
        print(f"[WARNING] No Postgres clusters found in namespace: {NAMESPACE}. Continuing with remaining steps.")
        POSTGRES_CLUSTERS = []  # Ensure POSTGRES_CLUSTERS is empty
        return
    
    POSTGRES_CLUSTERS = output.splitlines()
    print("[INFO] Found Postgres clusters:")
    for cluster in POSTGRES_CLUSTERS:
        print(f"    {cluster}")
    print("")

def check_cluster_pods(cluster):
    """Step 3: List pods for a Postgres cluster."""
    print("========================================")
    print(f"🔍 STEP 3: CHECKING PODS FOR CLUSTER: {cluster}")
    print("========================================")

    output = run_command(["kubectl", "-n", NAMESPACE, "get", "pods", "-l", "postgres-operator.crunchydata.com/role", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"])
    if not output:
        print(f"[WARNING] No pods found for cluster: {cluster} in namespace: {NAMESPACE}.")
        return False
    
    pods = [pod for pod in output.splitlines() if pod.startswith(cluster)]
    if not pods:
        print(f"[WARNING] No pods found for cluster {cluster} in namespace: {NAMESPACE}.")
        return False
    
    print(f"[INFO] Pods for cluster {cluster}:")
    for pod in pods:
        print(f"    {pod}")
    print("")
    return True

def find_cluster_leader(cluster):
    """Step 4: Identify leader pod for a Postgres cluster."""
    print("========================================")
    print(f"👑 STEP 4: FINDING LEADER FOR CLUSTER: {cluster}")
    print("========================================")

    output = run_command(["kubectl", "-n", NAMESPACE, "get", "pods", "-l", "postgres-operator.crunchydata.com/role=master", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"])
    if not output:
        print(f"[WARNING] No leader pod found for cluster: {cluster}")
        return None
    
    leader_pod = next((pod for pod in output.splitlines() if pod.startswith(cluster)), None)
    if not leader_pod:
        print(f"[WARNING] No leader pod found for cluster {cluster}")
        return None
    
    print(f"[INFO] Leader pod for cluster {cluster} is: {leader_pod}")
    print(f"[ACTION] Running patronictl list on leader pod: {leader_pod}")
    output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", leader_pod, "--", "patronictl", "list"])
    if output is None:
        print(f"[ERROR] Failed to run patronictl list on pod {leader_pod}")
    else:
        print(output)
    print("")
    return leader_pod

def check_pgdata_size(pod):
    """Step 5: Check disk usage in /pgdata."""
    print("========================================")
    print(f"💽 STEP 5: CHECKING DISK SPACE IN /PGDATA ON POD: {pod}")
    print("========================================")

    print("[ACTION] Checking disk space in /pgdata...")
    output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod, "--", "df", "-hT", "/pgdata"])
    if output is None:
        print(f"[ERROR] Failed to check disk space on pod {pod}")
    else:
        print(output)
    print("")

def run_psql_queries(pod):
    """Step 6: List top 10 databases by size."""
    print("========================================")
    print(f"📊 STEP 6: RUNNING PSQL QUERIES ON POD: {pod}")
    print("========================================")

    print("[ACTION] Listing top 10 databases by size:")
    query = "SELECT datname AS database_name, pg_size_pretty(pg_database_size(datname)) AS size FROM pg_database ORDER BY pg_database_size(datname) DESC LIMIT 10;"
    output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod, "--", "bash", "-c", f"PAGER=cat psql -d SharedServices --no-psqlrc --no-align --tuples-only -c \"{query}\""])
    if output is None:
        print(f"[ERROR] Failed to list databases on pod {pod}")
    else:
        print(output)
    print("")

def validate_backups():
    """Step 7: Check backup status with kubectl and display a table of backups."""
    print("========================================")
    print("🗄️ STEP 7: VALIDATING BACKUPS WITH STATUS")
    print("========================================")

    global SCHEDULED_BACKUPS
    output = run_command([
        "kubectl", "-n", NAMESPACE, "get", "jobs",
        "-l", "sas.com/backup-job-type=scheduled-backup",
        "-L", "sas.com/sas-backup-id,sas.com/backup-job-type,sas.com/sas-backup-job-status,sas.com/sas-backup-persistence-status"
    ])
    if output is None:
        print("[ERROR] Failed to retrieve backup status. Check kubectl permissions or cluster connectivity.")
        SCHEDULED_BACKUPS = []
        return
    
    if not output.strip():
        print("[WARNING] No backup jobs found in namespace.")
        SCHEDULED_BACKUPS = []
        return
    
    lines = output.splitlines()
    SCHEDULED_BACKUPS = []
    table_data = []
    header_skipped = False
    for line in lines:
        if not header_skipped:
            header_skipped = True
            continue
        parts = re.split(r'\s+', line.strip())
        if len(parts) < 8:
            continue
        try:
            name, status, completions, duration, age, backup_id, job_type, job_status, persistence_status = parts[:9] if len(parts) >= 9 else (parts[0], "Unknown", *parts[1:7])
            timestamp_str = backup_id.split('-')[0]
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    timestamp_str = backup_id.split('T')[0]
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            backup_entry = {
                "NAME": name,
                "STATUS": status,
                "COMPLETIONS": completions,
                "DURATION": duration,
                "AGE": age,
                "SAS-BACKUP-ID": backup_id,
                "TIMESTAMP": timestamp,
                "SAS-BACKUP-JOB-STATUS": job_status,
                "SAS-BACKUP-PERSISTENCE-STATUS": persistence_status
            }
            SCHEDULED_BACKUPS.append(backup_entry)
            table_data.append([
                name,
                backup_id,
                timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                job_status,
                persistence_status
            ])
        except ValueError:
            continue
    
    if not SCHEDULED_BACKUPS:
        print("[WARNING] No backups parsed from output.")
        return
    
    # Print the table
    headers = ["Name", "SAS-BACKUP-ID", "Timestamp", "Job Status", "Persistence Status"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Highlight the last successful backup
    successful_backups = [b for b in SCHEDULED_BACKUPS if b["SAS-BACKUP-JOB-STATUS"] == "Completed"]
    if successful_backups:
        last_successful = max(successful_backups, key=lambda x: x["TIMESTAMP"])
        print(f"\n[INFO] Last Successful Backup: {last_successful['SAS-BACKUP-ID']} at {last_successful['TIMESTAMP'].strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("\n[INFO] No successful backups found.")
    print("")

def check_azure_snapshots():
    """Step 8: Check Azure NetApp Files volume snapshots and metrics."""
    print("========================================")
    print("📸 STEP 8: CHECKING AZURE NETAPP FILES VOLUME SNAPSHOTS AND METRICS")
    print("========================================")

    subscription_id = run_command(["az", "account", "show", "--query", "id", "-o", "tsv"])
    if not subscription_id:
        print("[ERROR] Could not retrieve Azure subscription ID.")
        return
    
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.00000 +00:00")
    start_time = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S.00000 +00:00")

    volume_list_cmd = (
        f"az netappfiles volume list --resource-group {RGN} "
        f"--account-name {ANF_ACCOUNT} --pool-name {ANF_POOL} "
        f"--query '[].name' --output tsv"
    )
    volumes = run_az_command(volume_list_cmd)
    if not volumes:
        print(f"[ERROR] No volumes found or error occurred in resource group {RGN}.")
        return

    volume_data = []

    for full_volume_path in volumes:
        volume = full_volume_path.split('/')[-1]
        print(f"\n[ACTION] Processing Volume: {volume}")

        provisioned_size_cmd = (
            f"az netappfiles volume show --resource-group {RGN} "
            f"--account-name {ANF_ACCOUNT} --pool-name {ANF_POOL} "
            f"--name {volume} --query 'usageThreshold' --output tsv"
        )
        provisioned_size_raw = run_az_command(provisioned_size_cmd)
        print(f"[DEBUG] Raw provisioned size output for {volume}: {provisioned_size_raw}")

        if provisioned_size_raw:
            try:
                if isinstance(provisioned_size_raw, list):
                    provisioned_size_str = provisioned_size_raw[0] if provisioned_size_raw else "0"
                elif isinstance(provisioned_size_raw, str):
                    provisioned_size_str = provisioned_size_raw
                elif isinstance(provisioned_size_raw, int):
                    provisioned_size_str = str(provisioned_size_raw)
                else:
                    raise ValueError(f"Unexpected type for provisioned_size_raw: {type(provisioned_size_raw)}")
                provisioned_size = int(float(provisioned_size_str))
            except (ValueError, IndexError, TypeError) as e:
                print(f"[WARNING] Could not parse provisioned size for {volume}: {e}. Assuming 0.")
                provisioned_size = 0
        else:
            print(f"[WARNING] Could not retrieve provisioned size for {volume}. Assuming 0.")
            provisioned_size = 0

        resource_id = (
            f"/subscriptions/{subscription_id}/resourceGroups/{RGN}/"
            f"providers/Microsoft.NetApp/netAppAccounts/{ANF_ACCOUNT}/capacityPools/{ANF_POOL}/volumes/{volume}"
        )
        metrics_cmd = (
            f"az monitor metrics list --resource '{resource_id}' "
            f"--metric 'VolumeLogicalSize,VolumeSnapshotSize' "
            f"--start-time '{start_time}' --end-time '{end_time}' "
            f"--interval PT1H "
            f"--aggregation Average --query 'value[].timeseries[0].data[-1].average' --output tsv"
        )
        metrics = run_az_command(metrics_cmd)
        if metrics and len(metrics) >= 2:
            used_size = int(float(metrics[0]))
            snapshot_size = int(float(metrics[1]))
        else:
            used_size = snapshot_size = 0
            print(f"[WARNING] Could not retrieve metrics for {volume}.")

        filesystem_size = used_size - snapshot_size
        free_size = provisioned_size - used_size if provisioned_size >= used_size else 0

        print(f"Quota (Provisioned Size): {bytes_to_human_readable(provisioned_size)} ({provisioned_size} bytes)")
        print(f"File System Consumption: {bytes_to_human_readable(filesystem_size)} ({filesystem_size} bytes)")
        print(f"Snapshot Consumption: {bytes_to_human_readable(snapshot_size)} ({snapshot_size} bytes)")
        print(f"Free Space: {bytes_to_human_readable(free_size)} ({free_size} bytes)")

        if provisioned_size > 0:
            used_percentage = (used_size / provisioned_size) * 100
        else:
            used_percentage = 0

        volume_data.append({
            "name": volume,
            "free_size": bytes_to_gib(free_size),
            "filesystem_size": bytes_to_gib(filesystem_size),
            "snapshot_size": bytes_to_gib(snapshot_size),
            "total_used": bytes_to_gib(used_size),
            "provisioned_size": bytes_to_gib(provisioned_size),
            "used_percentage": used_percentage
        })

        snapshot_cmd = (
            f"az netappfiles snapshot list --resource-group {RGN} "
            f"--account-name {ANF_ACCOUNT} --pool-name {ANF_POOL} "
            f"--volume-name {volume} --query '[].name' --output tsv | awk -F'/' '{{print $4}}'"
        )
        snapshots = run_az_command(snapshot_cmd)
        if snapshots and snapshots[0]:
            print("Snapshots:")
            for snapshot in snapshots:
                print(f"- {snapshot}")
        else:
            print("Snapshots: None")

    if volume_data:
        num_volumes = len(volume_data)
        cols = min(4, max(1, num_volumes // 2 + num_volumes % 2))
        rows = (num_volumes + cols - 1) // cols

        base_width_per_chart = 3
        base_height_per_chart = 2.5
        fig_width = base_width_per_chart * cols
        fig_height = base_height_per_chart * rows + 1

        fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height), constrained_layout=True)
        if num_volumes == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for idx, vol in enumerate(volume_data):
            ax = axes[idx]
            free_size = vol["free_size"]
            filesystem_size = vol["filesystem_size"]
            snapshot_size = vol["snapshot_size"]
            total_used = vol["total_used"]
            used_percentage = vol["used_percentage"]

            sizes = [free_size, filesystem_size, snapshot_size]
            labels = [
                f"Free\n{free_size:.2f} GiB",
                f"File System\n{filesystem_size:.2f} GiB",
                f"Snapshot\n{snapshot_size:.2f} GiB"
            ]
            colors = ['#d3d3d3', '#1f77b4', '#ff7f0e']

            wedges, texts, autotexts = ax.pie(
                sizes, labels=labels, colors=colors, startangle=90,
                wedgeprops=dict(width=0.2, edgecolor='w'),
                autopct=lambda p: f'{p:.1f}%' if p > 5 else '', textprops={'fontsize': 8}
            )

            for text in texts:
                text.set_fontsize(8)

            ax.text(0, 0, f"{used_percentage:.1f}%\n{total_used:.2f} GiB Used", ha='center', va='center', fontsize=9)
            ax.set_title(f"Volume: {vol['name']}", fontsize=10, pad=5)

        for idx in range(len(volume_data), len(axes)):
            axes[idx].axis('off')

        plt.suptitle("Azure NetApp Files Consumption Breakdown by Volume", fontsize=12, y=1.02)
        plt.show()
    else:
        print("[INFO] No volumes to graph.")
    print("")

def check_sasviyabackup_size():
    """Step 9: Check /sasviyabackup directory size."""
    print("========================================")
    print("📂 STEP 9: CHECKING /SASVIYABACKUP DIRECTORY SIZE")
    print("========================================")

    global POD_NAME, TOTAL_SIZE, BACKUP_LIST, MOUNT_TOTAL_SIZE
    pod_output = run_command(["kubectl", "-n", NAMESPACE, "get", "pods", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"])
    if not pod_output:
        print(f"[WARNING] No pods found in namespace: {NAMESPACE}. Skipping /sasviyabackup checks.")
        BACKUP_LIST = []
        return
    
    POD_NAME = next((pod for pod in pod_output.splitlines() if "sas-cas-server-default-controller" in pod), None)
    if not POD_NAME:
        print(f"[WARNING] sas-cas-server-default-controller pod not found in namespace: {NAMESPACE}. Skipping /sasviyabackup checks.")
        BACKUP_LIST = []
        return
    
    print(f"[INFO] Found pod: {POD_NAME}")
    
    print("[ACTION] Checking mount point details for /sasviyabackup...")
    mount_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "df", "-h", "/sasviyabackup"])
    if mount_output is None:
        print(f"[ERROR] Failed to get mount point details for /sasviyabackup on pod {POD_NAME}. Skipping /sasviyabackup checks.")
        BACKUP_LIST = []
        return
    print(mount_output)
    mount_lines = mount_output.splitlines()
    if len(mount_lines) > 1:
        mount_details = re.split(r'\s+', mount_lines[1].strip())
        MOUNT_TOTAL_SIZE = mount_details[1]
        print(f"[INFO] /sasviyabackup mount total size: {MOUNT_TOTAL_SIZE}")
    else:
        print("[ERROR] Could not parse mount point size. Skipping /sasviyabackup checks.")
        BACKUP_LIST = []
        return

    print("[ACTION] Checking directory size in /sasviyabackup...")
    total_size_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "sh", "-c", "du -sh /sasviyabackup"])
    if total_size_output is None:
        print(f"[ERROR] Failed to get directory size for /sasviyabackup on pod {POD_NAME}. Skipping /sasviyabackup checks.")
        BACKUP_LIST = []
        return
    TOTAL_SIZE = total_size_output.split()[0]
    print(f"[INFO] /sasviyabackup directory total size: {TOTAL_SIZE}")
    
    print("[ACTION] Checking folder sizes inside /sasviyabackup...")
    backup_list_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "sh", "-c", "du -sh /sasviyabackup/*"])
    if backup_list_output is None or not backup_list_output.strip():
        print(f"[WARNING] No backup directories found in /sasviyabackup on pod {POD_NAME} or command failed.")
        BACKUP_LIST = []
    else:
        print(backup_list_output)
        BACKUP_LIST = backup_list_output.splitlines()
    print("")

def manage_and_delete_backups():
    """Step 10: Manage and delete old backups, retaining the last successful one."""
    print("========================================")
    print("🗑️ STEP 10: MANAGE AND DELETE OLD BACKUPS (RETAIN LAST SUCCESSFUL)")
    print("========================================")
    
    global STEP10_DELETIONS
    if not SCHEDULED_BACKUPS:
        print("[ERROR] No scheduled backups available. Skipping backup deletion.")
        return

    current_date = datetime.now(timezone.utc)
    one_day_ago = current_date - timedelta(days=1)

    successful_backups = [b for b in SCHEDULED_BACKUPS if b["SAS-BACKUP-JOB-STATUS"] == "Completed"]
    if not successful_backups:
        print("[WARNING] No successful backups found. No deletions will be performed.")
        return
    
    last_successful_backup = max(successful_backups, key=lambda x: x["TIMESTAMP"])
    last_successful_id = last_successful_backup["SAS-BACKUP-ID"]
    print(f"[INFO] Last successful backup to retain: {last_successful_id} at {last_successful_backup['TIMESTAMP']}")

    if not BACKUP_LIST:
        print("[WARNING] No backup directories found in /sasviyabackup. Skipping deletion process.")
        return

    old_backups = []
    total_old_size = 0.0

    for line in BACKUP_LIST:
        try:
            size, path = line.split(maxsplit=1)
            folder = os.path.basename(path)
            date_part = folder.split('-')[0]
            backup_time = datetime.strptime(date_part, "%Y%m%d").replace(tzinfo=timezone.utc)
            if backup_time < one_day_ago and last_successful_id not in folder:
                old_backups.append(line)
                total_old_size += parse_size(size)
        except (ValueError, IndexError):
            continue

    if not old_backups:
        print("[INFO] No backups older than 1 day found (excluding last successful backup).")
        return

    print(f"[INFO] Found {len(old_backups)} backups older than 1 day eligible for deletion:")
    for backup in old_backups:
        print(backup)
    print(f"[INFO] Total space to be released if deleted: {total_old_size:.2f}G")
    print(f"[INFO] Last successful backup ({last_successful_id}) will be retained.")
    
    STEP10_DELETIONS["before_size"] = TOTAL_SIZE
    STEP10_DELETIONS["mount_before"] = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "df", "-h", "/sasviyabackup"])

    auto_delete = input("Do you want to delete these backups automatically? (y/n, default n): ").strip().lower()
    if auto_delete == 'y':
        print("[ACTION] Deleting old backups automatically...")
        for backup in old_backups:
            path = backup.split(maxsplit=1)[1]
            run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "rm", "-rf", path], capture_output=False)
            STEP10_DELETIONS["count"] += 1
            STEP10_DELETIONS["space_reclaimed"] += parse_size(backup.split()[0])
            print(f"[INFO] Deleted {path}")
        total_size_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "sh", "-c", "du -sh /sasviyabackup"])
        STEP10_DELETIONS["after_size"] = total_size_output.split()[0] if total_size_output else "Unknown"
        STEP10_DELETIONS["mount_after"] = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", POD_NAME, "--", "df", "-h", "/sasviyabackup"])
    else:
        print("[INFO] Manual deletion selected. Use: kubectl -n {NAMESPACE} exec -it {POD_NAME} -- rm -rf <path>")
        STEP10_DELETIONS["after_size"] = TOTAL_SIZE
        STEP10_DELETIONS["mount_after"] = STEP10_DELETIONS["mount_before"]
    print("")

def attach_pvc_and_verify():
    """Step 11: Attach PVC and verify data with improved timeout."""
    print("========================================")
    print("🔗 STEP 11: ATTACHING PVC AND VERIFYING DATA")
    print("========================================")
    
    global STEP11_DELETIONS
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pod_name = f"pvc-common-data-{NAMESPACE}-{timestamp}"
    pvc_name = "sas-common-backup-data"

    pod_yaml = f"""---
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {NAMESPACE}
spec:
  containers:
  - name: backup-checker
    image: busybox
    command: ["sleep", "3600"]
    volumeMounts:
    - mountPath: /mnt/backup
      name: backup-data
  volumes:
  - name: backup-data
    persistentVolumeClaim:
      claimName: {pvc_name}
  restartPolicy: Never
"""
    with open("pod.yaml", "w") as f:
        f.write(pod_yaml)
    run_command(["kubectl", "apply", "-f", "pod.yaml"], capture_output=False)
    os.remove("pod.yaml")

    print(f"[ACTION] Waiting for pod {pod_name} to be Running (up to 60 seconds)...")
    timeout = 60
    interval = 2
    for i in range(timeout // interval):
        status = run_command(["kubectl", "-n", NAMESPACE, "get", "pod", pod_name, "-o", "custom-columns=STATUS:.status.phase", "--no-headers"])
        if status == "Running":
            print(f"[INFO] Pod {pod_name} is Running after {i * interval} seconds.")
            break
        print(f"[INFO] Waiting... ({i * interval}s elapsed, status: {status or 'Unknown'})")
        time.sleep(interval)
    else:
        print(f"[ERROR] Pod {pod_name} did not reach Running state after {timeout} seconds.")
        detailed_status = run_command(["kubectl", "-n", NAMESPACE, "describe", "pod", pod_name])
        print(f"[DEBUG] Pod description:\n{detailed_status}")
        return

    print(f"[INFO] Listing data in /mnt/backup on pod {pod_name}:")
    output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "sh", "-c", "du -sh /mnt/backup/*"])
    if not output:
        print("[INFO] No backups found in /mnt/backup.")
        STEP11_DELETIONS["before_size"] = STEP11_DELETIONS["after_size"] = "0G"
        return
    print(output)
    backup_list = output.splitlines()
    
    before_size_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "sh", "-c", "du -sh /mnt/backup"])
    STEP11_DELETIONS["before_size"] = before_size_output.split()[0] if before_size_output else "Unknown"
    
    successful_backups = [b for b in SCHEDULED_BACKUPS if b["SAS-BACKUP-JOB-STATUS"] == "Completed"]
    last_successful_id = max(successful_backups, key=lambda x: x["TIMESTAMP"])["SAS-BACKUP-ID"] if successful_backups else None
    if last_successful_id:
        print(f"[INFO] Last successful backup to retain: {last_successful_id}")
    
    old_backups = []
    total_old_size = 0.0
    for line in backup_list:
        try:
            size, path = line.split(maxsplit=1)
            folder = os.path.basename(path)
            if last_successful_id and last_successful_id not in folder:
                old_backups.append(line)
                total_old_size += parse_size(size)
        except (ValueError, IndexError):
            continue

    if not old_backups:
        print("[INFO] No backups eligible for deletion (retaining last successful backup).")
        STEP11_DELETIONS["after_size"] = STEP11_DELETIONS["before_size"]
        return
    
    print(f"[INFO] Found {len(old_backups)} backups eligible for deletion:")
    for backup in old_backups:
        print(backup)
    print(f"[INFO] Total space to be released if deleted: {total_old_size:.2f}G")
    
    STEP11_DELETIONS["mount_before"] = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "df", "-h", "/mnt/backup"])
    
    auto_delete = input("\nDelete these backups and pod automatically? (y/n, default n): ").strip().lower()
    if auto_delete == 'y':
        print("[ACTION] Deleting backups in /mnt/backup...")
        for line in old_backups:
            path = line.split(maxsplit=1)[1]
            run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "rm", "-rf", path], capture_output=False)
            STEP11_DELETIONS["count"] += 1
            STEP11_DELETIONS["space_reclaimed"] += parse_size(line.split()[0])
            print(f"[INFO] Deleted {path}")
        after_size_output = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "sh", "-c", "du -sh /mnt/backup"])
        STEP11_DELETIONS["after_size"] = after_size_output.split()[0] if after_size_output else "Unknown"
        print(f"[ACTION] Deleting pod {pod_name}...")
        run_command(["kubectl", "-n", NAMESPACE, "delete", "pod", pod_name], capture_output=False)
        print(f"[INFO] Pod {pod_name} deleted")
        STEP11_DELETIONS["mount_after"] = run_command(["kubectl", "-n", NAMESPACE, "exec", "-it", pod_name, "--", "df", "-h", "/mnt/backup"])
    else:
        print("[INFO] Manual deletion selected. Use: kubectl -n {NAMESPACE} exec -it {pod_name} -- rm -rf <path>")
        print(f"[INFO] To delete pod manually: kubectl -n {NAMESPACE} delete pod {pod_name}")
        STEP11_DELETIONS["after_size"] = STEP11_DELETIONS["before_size"]
        STEP11_DELETIONS["mount_after"] = STEP11_DELETIONS["mount_before"]
    print("")

def main():
    """Main execution logic with enhanced summary."""
    print(BANNER)
    print(INDEX)

    get_namespace_and_rgn()
    check_prerequisites()
    list_postgres_clusters()

    if POSTGRES_CLUSTERS:
        for cluster in POSTGRES_CLUSTERS:
            if check_cluster_pods(cluster):
                leader_pod = find_cluster_leader(cluster)
                if leader_pod:
                    check_pgdata_size(leader_pod)
                    run_psql_queries(leader_pod)
    else:
        print("[INFO] Skipping Postgres cluster-specific steps (Steps 3-6) due to no clusters found.")

    validate_backups()
    check_azure_snapshots()
    check_sasviyabackup_size()
    manage_and_delete_backups()
    attach_pvc_and_verify()

    # Enhanced Summary
    print("========================================")
    print("🎉 SCRIPT EXECUTION SUMMARY - DETAILED REPORT")
    print("========================================")
    print(f"[SUMMARY] Processed Postgres Clusters: {' '.join(POSTGRES_CLUSTERS) if POSTGRES_CLUSTERS else 'None'}")
    print(f"[SUMMARY] Initial /sasviyabackup Directory Size (from du -sh): {TOTAL_SIZE}")
    print(f"[SUMMARY] Initial /sasviyabackup Mount Total Size (from df -h): {MOUNT_TOTAL_SIZE}")

    if SCHEDULED_BACKUPS:
        successful_backups = [b for b in SCHEDULED_BACKUPS if b["SAS-BACKUP-JOB-STATUS"] == "Completed"]
        if successful_backups:
            last_successful = max(successful_backups, key=lambda x: x["TIMESTAMP"])
            print(f"[SUMMARY] Last Successful Backup: {last_successful['SAS-BACKUP-ID']} at {last_successful['TIMESTAMP']}")
        else:
            print("[SUMMARY] No successful backups found.")
        latest_backup = max(SCHEDULED_BACKUPS, key=lambda x: x["TIMESTAMP"])
        print(f"[SUMMARY] Latest Backup Attempt: {latest_backup['SAS-BACKUP-ID']} at {latest_backup['TIMESTAMP']} (Status: {latest_backup['SAS-BACKUP-JOB-STATUS']})")

        if STEP10_DELETIONS["count"] > 0:
            print("\n[SUMMARY] Step 10 - Automatic Backup Deletion Results:")
            print(f"  Description: Removed {STEP10_DELETIONS['count']} old backups from /sasviyabackup, keeping the last successful backup.")
            print(f"  Backups Deleted: {STEP10_DELETIONS['count']}")
            print(f"  Directory Size Before Deletion: {STEP10_DELETIONS['before_size']}")
            print(f"  Directory Size After Deletion: {STEP10_DELETIONS['after_size']}")
            print(f"  Mount Details Before Deletion:")
            print(STEP10_DELETIONS['mount_before'])
            print(f"  Mount Details After Deletion:")
            print(STEP10_DELETIONS['mount_after'])
            print(f"  Space Reclaimed: {STEP10_DELETIONS['space_reclaimed']:.2f}G")
        else:
            print("\n[SUMMARY] No backups deleted in Step 10.")

        if STEP11_DELETIONS["count"] > 0:
            print("\n[SUMMARY] Step 11 - Automatic Backup Deletion Results:")
            print(f"  Description: Removed {STEP11_DELETIONS['count']} old backups from PVC mount /mnt/backup.")
            print(f"  Backups Deleted: {STEP11_DELETIONS['count']}")
            print(f"  Directory Size Before Deletion: {STEP11_DELETIONS['before_size']}")
            print(f"  Directory Size After Deletion: {STEP11_DELETIONS['after_size']}")
            print(f"  Mount Details Before Deletion:")
            print(STEP11_DELETIONS['mount_before'])
            print(f"  Mount Details After Deletion:")
            print(STEP11_DELETIONS['mount_after'])
            print(f"  Space Reclaimed: {STEP11_DELETIONS['space_reclaimed']:.2f}G")
        else:
            print("\n[SUMMARY] No backups deleted in Step 11.")
    else:
        print("[SUMMARY] No backup status information available.")
    print("========================================")

if __name__ == "__main__":
    main()
