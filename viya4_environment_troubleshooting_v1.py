#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import json
from datetime import datetime

# ANSI color codes for terminal
YELLOW = "\033[93m"
RESET = "\033[0m"

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

def get_namespace():
    """Get namespace from user input."""
    namespace = input("Please enter the Kubernetes namespace: ").strip()
    if not namespace:
        print("Namespace cannot be empty!")
        sys.exit(1)
    return namespace

def list_pods(namespace, html_data):
    """List all pods in the given namespace."""
    print(f"\n2. [📋] Listing all pods in namespace '{namespace}'...")
    print("----------------------------------------")
    command = f"kubectl get pods -n {namespace} --no-headers"
    output = run_command(command)
    if output:
        lines = output.split('\n')
        headers = ["NAME", "READY", "STATUS", "RESTARTS", "AGE"]
        rows = [line.split(maxsplit=4) for line in lines if line.strip()]
        print_table(headers, rows)
        html_data['pods'] = {'headers': headers, 'rows': rows}
    else:
        print(f"Failed to list pods in namespace '{namespace}'.")
        html_data['pods'] = {'headers': ["Message"], 'rows': [["Failed to list pods"]]}

def sas_readiness_check(namespace, html_data):
    """Check the status of the sas-readiness pod."""
    print(f"\n3. [✅] SAS Readiness Check...")
    print("----------------------------------------")
    
    command = f"kubectl get pod -n {namespace} -l app=sas-readiness -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready --no-headers"
    output = run_command(command)
    
    readiness_output = ""
    if not output:
        readiness_output = f"No sas-readiness pod found in namespace '{namespace}'."
        print(readiness_output)
    else:
        pod_info = output.split()
        if len(pod_info) < 2:
            readiness_output = f"Could not parse status for sas-readiness pod in namespace '{namespace}'."
            print(readiness_output)
        else:
            pod_name, readiness = pod_info[0], pod_info[1]
            command = f"kubectl get pod -n {namespace} {pod_name} -o jsonpath='{{.status.containerStatuses[*].ready}}'"
            readiness_status = run_command(command)
            is_ready = readiness_status == "true" and readiness == "true"
            
            if is_ready:
                log_command = f"kubectl logs -n {namespace} {pod_name} --tail=1"
                last_log = run_command(log_command)
                if last_log and "All checks passed. Marking as ready" in last_log:
                    readiness_output = f"SAS Readiness Check: All good! Pod '{pod_name}' is 1/1 and ready."
                    print(readiness_output)
                else:
                    readiness_output = f"SAS Readiness Check: Pod '{pod_name}' is 1/1 but not fully confirmed ready."
                    print(readiness_output)
            else:
                log_command = f"kubectl logs -n {namespace} {pod_name} --tail=1"
                last_log = run_command(log_command)
                readiness_output = (f"SAS Readiness Check: Pod '{pod_name}' is not fully ready (not 1/1). Last log line:\n"
                                   f"----------------------------------------\n"
                                   f"{last_log if last_log else 'No logs available'}\n"
                                   f"----------------------------------------")
                print(readiness_output)
    html_data['readiness'] = readiness_output

def list_nodes_and_utilization(namespace, html_data):
    """List all nodes with basic utilization from kubectl top."""
    print(f"\n4. [💻] Listing all nodes and their utilization (Overview)...")
    print("----------------------------------------")
    command = "kubectl top nodes --no-headers"
    output = run_command(command)
    if output:
        lines = output.split('\n')
        headers = ["NAME", "CPU(cores)", "CPU%", "MEMORY(bytes)", "MEMORY%"]
        rows = [line.split(maxsplit=4) for line in lines if line.strip()]
        print_table(headers, rows)
        html_data['nodes'] = {'headers': headers, 'rows': rows}
    else:
        print("Failed to list nodes. Ensure 'kubectl top' is supported and metrics-server is running.")
        html_data['nodes'] = {'headers': ["Message"], 'rows': [["Failed to list nodes"]]}

def parse_resource_value(value, is_cpu=False):
    """Convert resource values to millicores for CPU or Gi for memory, handling whitespace."""
    if not value or value == "0":
        return 0
    value = value.strip()  # Remove leading/trailing whitespace
    try:
        if is_cpu:
            if value.endswith('m'):
                return float(value[:-1])
            elif value.isdigit():
                return float(value) * 1000
        else:
            if value.endswith('Ki'):
                return float(value[:-2]) / (1024 * 1024)
            elif value.endswith('Mi'):
                return float(value[:-2]) / 1024
            elif value.endswith('Gi'):
                return float(value[:-2])
            elif value.isdigit():
                return float(value) / (1024 * 1024 * 1024)
        return float(value)
    except ValueError:
        print(f"Warning: Could not parse resource value '{value}'")
        return 0

def node_resource_utilization(namespace, html_data):
    """List detailed node resource utilization (reserved resources) from kubectl describe node."""
    print(f"\n5. [📊] Node Resource Utilization (Reserved Resources)...")
    print("----------------------------------------")
    
    node_output = run_command("kubectl get nodes --no-headers")
    if not node_output:
        print("Failed to get node list.")
        html_data['resources'] = {'headers': ["Message"], 'rows': [["Failed to get node list"]]}
        return
    
    nodes = [line.split()[0] for line in node_output.split('\n') if line.strip()]
    
    headers = ["Node", "Allocatable CPU", "CPU Requests", "CPU Req %", "CPU Limits", "CPU Lim %", "CPU Remaining", 
               "Allocatable Memory", "Memory Requests", "Memory Req %", "Memory Limits", "Memory Lim %", "Memory Remaining"]
    table_data = []
    
    for node in nodes:
        describe_output = run_command(f"kubectl describe node {node}")
        if not describe_output:
            print(f"Failed to describe node '{node}'.")
            continue
        
        allocatable = {}
        allocated = {'cpu': {'requests': 0, 'limits': 0}, 'memory': {'requests': 0, 'limits': 0}}
        
        lines = describe_output.split('\n')
        in_allocatable = False
        in_allocated = False
        
        for line in lines:
            line = line.strip()
            if line.startswith("Allocatable:"):
                in_allocatable = True
                continue
            elif line.startswith("Allocated resources:"):
                in_allocatable = False
                in_allocated = True
                continue
            elif line and "Events:" in line:
                in_allocated = False
                continue
            
            if in_allocatable and ':' in line:
                key, value = [x.strip() for x in line.split(':', 1)]
                if key in ['cpu', 'memory']:
                    allocatable[key] = parse_resource_value(value, is_cpu=(key == 'cpu'))
            
            if in_allocated and line and not line.startswith(('(', 'Resource', '----')):
                match = re.match(r'(\w+)\s+(\d+(?:m|[KMG]i)?)\s*\((\d+)%\)\s+(\d+(?:m|[KMG]i)?)\s*\((\d+)%\)', line)
                if match:
                    resource, req_val, req_pct, lim_val, lim_pct = match.groups()
                    if resource in ['cpu', 'memory']:
                        allocated[resource]['requests'] = parse_resource_value(req_val, is_cpu=(resource == 'cpu'))
                        allocated[resource]['limits'] = parse_resource_value(lim_val, is_cpu=(resource == 'cpu'))
        
        alloc_cpu = allocatable.get('cpu', 0)
        req_cpu = allocated['cpu']['requests']
        lim_cpu = allocated['cpu']['limits']
        remaining_cpu = alloc_cpu - req_cpu
        req_percent_cpu = float(req_cpu / alloc_cpu * 100) if alloc_cpu else 0
        lim_percent_cpu = float(lim_cpu / alloc_cpu * 100) if alloc_cpu else 0
        
        alloc_mem = allocatable.get('memory', 0)
        req_mem = allocated['memory']['requests']
        lim_mem = allocated['memory']['limits']
        remaining_mem = alloc_mem - req_mem
        req_percent_mem = float(req_mem / alloc_mem * 100) if alloc_mem else 0
        lim_percent_mem = float(lim_mem / alloc_mem * 100) if alloc_mem else 0
        
        row = [
            node,
            f"{alloc_cpu:.1f}m", f"{req_cpu:.1f}m", f"{req_percent_cpu:.1f}%", f"{lim_cpu:.1f}m", f"{lim_percent_cpu:.1f}%", f"{remaining_cpu:.1f}m",
            f"{alloc_mem:.1f}Gi", f"{req_mem:.1f}Gi", f"{req_percent_mem:.1f}%", f"{lim_mem:.1f}Gi", f"{lim_percent_mem:.1f}%", f"{remaining_mem:.1f}Gi"
        ]
        table_data.append((row, req_percent_mem > 90, False))
    
    print_table(headers, [row for row, _, _ in table_data])
    html_data['resources'] = {'headers': headers, 'rows': table_data}

def check_pods_for_errors(namespace, html_data):
    """Check specified SAS pods for unique ERROR and WARN messages."""
    print(f"\n6. [🚨] Check Pods for Errors...")
    print("----------------------------------------")
    
    sas_pods = [
        "sas-arke", "sas-authorization", "sas-compute",
        "sas-configuration", "sas-credentials", "sas-feature-flags", "sas-files",
        "sas-identities", "sas-job-execution", "sas-job-execution-app",
        "sas-launcher", "sas-logon-app", "sas-microanalytic-score",
        "sas-readiness", "sas-scheduler", "sas-search", "sas-studio-app",
        "sas-visual-analytics", "sas-visual-analytics-app"
    ]
    
    valid_levels = {"error", "warn"}
    log_entries = []
    has_errors_or_warns = False
    
    command = f"kubectl get pods -n {namespace} --no-headers"
    output = run_command(command)
    if not output:
        print(f"Failed to list pods in namespace '{namespace}'.")
        html_data['errors'] = {'headers': ["Message"], 'rows': [["Failed to list pods"]]}
        return
    
    running_pods = {line.split()[0] for line in output.split('\n') if line.strip()}
    
    first_pod = True
    for pod_prefix in sas_pods:
        for pod in running_pods:
            if pod.startswith(pod_prefix):
                log_command = f"kubectl logs -n {namespace} {pod}"
                logs = run_command(log_command)
                if logs:
                    unique_messages = {}
                    for line in logs.split('\n'):
                        level = None
                        message = None
                        try:
                            log_entry = json.loads(line)
                            level = log_entry.get("level", "").lower()
                            if level in valid_levels:
                                message = log_entry.get("message", "")
                                context = log_entry.get("source", "")
                                full_message = f"[{context}] - {message}"
                                unique_messages[full_message] = level
                        except json.JSONDecodeError:
                            match = re.match(r"(ERROR|WARN) (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d+ [+-]\d{4}) \[([^\]]+)\] - (.+)", line)
                            if match:
                                level, _, _, context, message = match.groups()
                                if level.lower() in valid_levels:
                                    full_message = f"[{context}] - {message}"
                                    unique_messages[full_message] = level.lower()
                    
                    if unique_messages:
                        has_errors_or_warns = True
                        if not first_pod:
                            print("\n")
                        print(f"Pod name: {pod}")
                        print("----------")
                        pod_messages = list(unique_messages.items())[:10]
                        for msg, lvl in pod_messages:
                            print(f"{lvl.upper()}: {msg}")
                        print("----------------------------------------")
                        log_entries.extend([[pod, "", "", lvl.upper(), msg] for msg, lvl in pod_messages])
                        first_pod = False
    
    if not has_errors_or_warns:
        print("No ERROR or WARN messages found in pod logs.")
        log_entries.append(["No messages", "", "", "", "All pods checked, no ERROR/WARN lines detected"])
    
    html_data['errors'] = {'headers': ["POD_NAME", "DATE", "TIME", "LOG_CODE", "MESSAGE"], 'rows': log_entries}

def pod_resource_utilization(namespace, html_data):
    """List resource utilization for sas-authorization pods against limits only."""
    print(f"\n7. [📈] Pod Resource Utilization (Actual vs Limits) for sas-authorization...")
    print("----------------------------------------")
    
    # Get pod list, filter for sas-authorization
    pod_output = run_command(f"kubectl get pods -n {namespace} --no-headers")
    if not pod_output:
        print(f"Failed to list pods in namespace '{namespace}'.")
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["Failed to list pods"]]}
        return
    
    pods = [line.split()[0] for line in pod_output.split('\n') if line.strip() and line.split()[0].startswith('sas-authorization')]
    if not pods:
        print("No sas-authorization pods found in the namespace.")
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["No sas-authorization pods found"]]}
        return
    
    # Get actual usage from kubectl top
    top_output = run_command(f"kubectl top pods -n {namespace} --no-headers")
    if not top_output:
        print("Failed to get pod utilization. Ensure 'kubectl top' is supported and metrics-server is running.")
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["Failed to get pod utilization"]]}
        return
    
    # Parse top output with flexible whitespace handling
    usage_data = {}
    for line in top_output.split('\n'):
        if line.strip():
            match = re.match(r'(\S+)\s+(\d+m?)\s+(\d+(?:Mi|Gi|Ki)?)', line)
            if match:
                pod_name, cpu_usage, mem_usage = match.groups()
                if pod_name.startswith('sas-authorization'):
                    usage_data[pod_name] = {
                        'cpu_usage': parse_resource_value(cpu_usage, is_cpu=True),
                        'mem_usage': parse_resource_value(mem_usage, is_cpu=False)
                    }
    
    headers = ["Pod Name", "CPU Usage", "CPU Lim", "CPU Lim %", "Memory Usage", "Mem Lim", "Mem Lim %"]
    table_data = []
    
    for pod in pods:
        describe_output = run_command(f"kubectl describe pod -n {namespace} {pod}")
        if not describe_output:
            print(f"Failed to describe pod '{pod}'.")
            continue
        
        cpu_lim, mem_lim = 0, 0  # Only parse limits
        in_containers = False
        lines = describe_output.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("Containers:"):
                in_containers = True
            elif in_containers and line.startswith("Limits:"):
                i += 1
                while i < len(lines) and lines[i].strip():
                    subline = lines[i].strip()
                    if subline.startswith("cpu:"):
                        cpu_lim = parse_resource_value(subline.split(":")[1].strip(), is_cpu=True)
                    elif subline.startswith("memory:"):
                        mem_lim = parse_resource_value(subline.split(":")[1].strip(), is_cpu=False)
                    elif subline.startswith("Requests:"):  # Skip Requests section
                        break
                    i += 1
            i += 1
        
        pod_usage = usage_data.get(pod, {'cpu_usage': 0, 'mem_usage': 0})
        cpu_usage = pod_usage['cpu_usage']
        mem_usage = pod_usage['mem_usage']
        
        cpu_lim_pct = float(cpu_usage / cpu_lim * 100) if cpu_lim else 0
        mem_lim_pct = float(mem_usage / mem_lim * 100) if mem_lim else 0
        
        row = [
            pod,
            f"{cpu_usage:.1f}m", f"{cpu_lim:.1f}m", f"{cpu_lim_pct:.1f}%",
            f"{mem_usage:.1f}Gi", f"{mem_lim:.1f}Gi", f"{mem_lim_pct:.1f}%"
        ]
        table_data.append((row, mem_lim_pct > 90, False))
    
    print_table(headers, [row for row, _, _ in table_data])
    html_data['pod_resources'] = {'headers': headers, 'rows': table_data}

def generate_html(namespace, html_data):
    """Generate and save the HTML report."""
    try:
        print("Generating HTML content...")
        content = ""
        
        content += "<h2>List All Pods in the Namespace</h2>\n"
        if 'pods' in html_data:
            content += "<table>\n<tr>" + "".join(f"<th>{h}</th>" for h in html_data['pods']['headers']) + "</tr>\n"
            for row in html_data['pods']['rows']:
                content += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>\n"
            content += "</table>\n"
        
        content += "<h2>SAS Readiness Check</h2>\n"
        if 'readiness' in html_data:
            content += f"<pre>{html_data['readiness']}</pre>\n"
        
        content += "<h2>List Nodes and Their Utilization (Overview)</h2>\n"
        if 'nodes' in html_data:
            content += "<table>\n<tr>" + "".join(f"<th>{h}</th>" for h in html_data['nodes']['headers']) + "</tr>\n"
            for row in html_data['nodes']['rows']:
                content += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>\n"
            content += "</table>\n"
        
        content += "<h2>Node Resource Utilization (Reserved Resources)</h2>\n"
        if 'resources' in html_data:
            content += "<table>\n<tr>" + "".join(f"<th>{h}</th>" for h in html_data['resources']['headers']) + "</tr>\n"
            for row, req_high, _ in html_data['resources']['rows']:
                content += "<tr>"
                for i, cell in enumerate(row):
                    class_attr = ""
                    if i == 9 and req_high:  # Memory Req %
                        class_attr = ' class="high-usage"'
                    content += f"<td{class_attr}>{cell}</td>"
                content += "</tr>\n"
            content += "</table>\n"
        
        content += "<h2>Check Pods for Errors</h2>\n"
        if 'errors' in html_data:
            content += "<pre>\n"
            first_pod = True
            prev_pod = None
            for row in html_data['errors']['rows']:
                pod_name = row[0]
                level = row[3]
                message = row[4]
                if pod_name and pod_name != prev_pod:
                    if not first_pod:
                        content += "\n"
                    content += f"Pod name: {pod_name}\n----------\n"
                    prev_pod = pod_name
                    first_pod = False
                if message and "All pods checked" not in message:
                    content += f"{level}: {message}\n"
                elif "All pods checked" in message:
                    content += f"{message}\n"
                if pod_name and pod_name != prev_pod:
                    content += "----------------------------------------\n"
            content += "</pre>\n"
        
        content += "<h2>Pod Resource Utilization (Actual vs Limits) for sas-authorization</h2>\n"
        if 'pod_resources' in html_data:
            content += "<table>\n<tr>" + "".join(f"<th>{h}</th>" for h in html_data['pod_resources']['headers']) + "</tr>\n"
            for row, lim_high, _ in html_data['pod_resources']['rows']:
                content += "<tr>"
                for i, cell in enumerate(row):
                    class_attr = ""
                    if i == 6 and lim_high:  # Mem Lim % (index 6)
                        class_attr = ' class="high-usage"'
                    content += f"<td{class_attr}>{cell}</td>"
                content += "</tr>\n"
            content += "</table>\n"
        
        timestamp = datetime.now().strftime("%Y-%m-d %H:%M:%S")
        dt_for_path = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = os.path.expanduser(f"~/viya4/k8s_troubleshoot/{namespace}/{dt_for_path}")
        os.makedirs(report_dir, exist_ok=True)
        report_path = f"{report_dir}/sas_viya_report_{namespace}_{dt_for_path}.html"
        
        html_content = HTML_HEAD.format(namespace=namespace, timestamp=timestamp) + content + HTML_FOOT
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"\nHTML report generated: {report_path}")
    except Exception as e:
        import traceback
        print(f"Error in generate_html: {str(e)}")
        print("Full traceback:")
        print(traceback.format_exc())
        raise

def main():
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
