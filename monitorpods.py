#!/usr/bin/env python3
import logging
import sys
import os
import subprocess
import json
import time
import shutil
from datetime import datetime

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

# Console handler for INFO and above
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

def monitor_pods(ns="nseprod", ticket="NSE-123"):
    """Monitor pod statuses with a single table updated in place using ANSI codes"""
    # Simulate prior output (mimicking Steps 0–8)
    prior_output = [
        "Simulated prior output (Steps 0–8):",
        "✅ Step 1: Configuration loaded",
        "✅ Step 2: Zabbix setup completed",
        "✅ Step 3: Pods stopped",
        "✅ Step 4: Pods deleted",
        "✅ Step 5: Environment started",
        "✅ Step 6: Consul pods verified",
        "⏱️ Time taken for prior steps: 5 minutes, 30 seconds"
    ]
    for line in prior_output:
        print(line)
    
    start_time = time.time()
    iteration = 0
    table_lines = 10  # Header, subtitle, separator, timestamp, instruction, separator, headers, data, separator, empty line
    prior_lines = len(prior_output) + 2  # Prior output + separator + 1 newline
    
    # Log terminal and environment details
    term_type = os.environ.get("TERM", "unknown")
    in_tmux = os.environ.get("TMUX", "no") != "no"
    in_screen = os.environ.get("STY", "no") != "no"
    is_tty = sys.stdout.isatty()
    logger.info(f"Terminal type: {term_type}, TMUX: {in_tmux}, SCREEN: {in_screen}, TTY: {is_tty}")
    
    # Check if appending is forced or ANSI is unsupported
    force_append = os.environ.get("FORCE_APPEND", "0") == "1"
    use_ansi = not force_append and term_type != "dumb" and is_tty
    
    # Print separator with minimal padding
    terminal_width = min(shutil.get_terminal_size().columns, 80)
    print(f"\n{'=' * terminal_width}")  # Single newline before separator
    
    try:
        while True:
            iteration += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_pods = running = crashloop = error = completed = 0
            
            try:
                cmd = ["kubectl", "get", "pods", "-n", ns, "-o", "json"]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
                pod_data = json.loads(result.stdout)
                pods = pod_data.get("items", [])
                
                total_pods = len(pods)
                for pod in pods:
                    phase = pod["status"].get("phase", "Unknown")
                    container_statuses = pod["status"].get("containerStatuses", [])
                    
                    if phase == "Succeeded":
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
                
                logger.debug(f"Iteration {iteration}: Total={total_pods}, Running={running}, CrashLoopBackOff={crashloop}, Error={error}, Completed={completed}")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to fetch pod statuses: {e.stderr}")
                print(f"❌ Error fetching pod statuses at {timestamp}: {e.stderr}")
                total_pods = running = crashloop = error = completed = 0
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse pod JSON data: {e}")
                print(f"❌ Error parsing pod data at {timestamp}: {e}")
                total_pods = running = crashloop = error = completed = 0
            except subprocess.TimeoutExpired as e:
                logger.error(f"kubectl command timed out: {e}")
                print(f"❌ kubectl command timed out at {timestamp}")
                total_pods = running = crashloop = error = completed = 0
            
            # Build table
            separator = "-" * terminal_width
            table = [
                f"{'=' * terminal_width}",
                f"Pod Monitoring (Namespace: {ns}, Ticket: {ticket})",
                f"{'=' * terminal_width}",
                f"Timestamp: {timestamp} | Iteration: {iteration}",
                f"Press Ctrl+C to stop monitoring",
                separator,
                f"{'Sr. No.':<10}{'Total Pods':<12}{'Running':<10}{'CrashLoopBackOff':<18}{'Error':<10}{'Completed':<10}",
                separator,
                f"{iteration:<10}{total_pods:<12}{running:<10}{crashloop:<18}{error:<10}{completed:<10}",
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
            
            # Check if all pods are in terminal states
            if total_pods > 0 and running + completed == total_pods and crashloop == 0 and error == 0:
                print("\n✅ All pods are in Running or Completed states. Exiting monitoring.")
                break
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        separator = "-" * terminal_width
        table = [
            f"{'=' * terminal_width}",
            f"Pod Monitoring (Namespace: {ns}, Ticket: {ticket})",
            f"{'=' * terminal_width}",
            f"Timestamp: {timestamp} | Iteration: {iteration}",
            f"Monitoring stopped by user",
            separator,
            f"{'Sr. No.':<10}{'Total Pods':<12}{'Running':<10}{'CrashLoopBackOff':<18}{'Error':<10}{'Completed':<10}",
            separator,
            f"{iteration:<10}{total_pods:<12}{running:<10}{crashloop:<18}{error:<10}{completed:<10}",
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

if __name__ == "__main__":
    monitor_pods()