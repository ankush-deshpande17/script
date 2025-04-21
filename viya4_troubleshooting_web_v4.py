#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import json
from datetime import datetime
import requests
from flask import Flask, request, render_template_string, redirect, url_for, session, send_file, jsonify, Response
import shutil
import logging
import shlex
import psutil
import time
from io import StringIO, BytesIO
import pandas as pd
from flask_session import Session
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_VERSION = os.environ.get("SCRIPT_VERSION", "v1.8.3")
GITHUB_REPO = "ankush-deshpande17/script"
GITHUB_BRANCH = "main"
VERSION_FILE = "latest_version.txt"

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Configure server-side session storage
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = '/tmp/flask_session'
app.config['SESSION_PERMANENT'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = 3600
Session(app)

if not os.path.exists(app.config['SESSION_FILE_DIR']):
    os.makedirs(app.config['SESSION_FILE_DIR'])

SERVICES = ["NSE_VML_VIYA4_DEV", "NSE_VML_VIYA4_PROD", "TDG_VDS_VIYA4_Prod", "TDG_VDS_VIYA4_Test", "GFB_ALM_VIYA4_Prod", "GFB_ALM_VIYA4_Test"]

executor = ThreadPoolExecutor(max_workers=6)
task_results = defaultdict(dict)

# Group services by TLA
def group_services_by_tla(services):
    grouped = defaultdict(list)
    for service in services:
        tla = service.split('_')[0]
        grouped[tla].append(service)
    return grouped

# HTML Template with updated UI
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SAS Viya 4 Troubleshooting Portal</title>
    <style>
    {% raw %}
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Arial, sans-serif; background-color: #f0f2f5; color: #333; display: flex; min-height: 100vh; }
        
        /* Vertical Menu */
        .sidebar {
            width: 250px;
            background-color: #2c3e50;
            color: white;
            padding: 20px;
            height: 100vh;
            position: fixed;
            overflow-y: auto;
        }
        .sidebar h2 {
            font-size: 18px;
            margin-bottom: 10px;
            color: #ecf0f1;
        }
        .sidebar .filter-box {
            width: 100%;
            padding: 8px;
            margin-bottom: 20px;
            border: none;
            border-radius: 5px;
            font-size: 14px;
        }
        .sidebar .tla-group {
            margin-bottom: 10px;
        }
        .sidebar .tla-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px;
            background-color: #34495e;
            cursor: pointer;
            border-radius: 5px;
        }
        .sidebar .tla-header:hover {
            background-color: #3e5c76;
        }
        .sidebar .tla-header span {
            font-size: 16px;
            font-weight: bold;
        }
        .sidebar .tla-header .toggle-icon {
            font-size: 14px;
        }
        .sidebar .service-list {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-in-out;
        }
        .sidebar .service-list.open {
            max-height: 500px; /* Adjust based on content */
        }
        .sidebar .service-list a {
            display: block;
            padding: 10px 20px;
            color: #ecf0f1;
            text-decoration: none;
            font-size: 14px;
        }
        .sidebar .service-list a:hover {
            background-color: #3e5c76;
        }
        .sidebar .service-list a.active {
            background-color: #3498db;
            font-weight: bold;
        }
        .sidebar .service-list a.hidden {
            display: none;
        }

        /* Main Content */
        .main-content {
            margin-left: 250px;
            padding: 30px;
            flex: 1;
        }
        .main-content h1 {
            font-size: 28px;
            color: #2c3e50;
            margin-bottom: 20px;
        }
        .service-details {
            background-color: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .service-details h2 {
            font-size: 24px;
            color: #2c3e50;
            margin-bottom: 10px;
        }
        .service-details p {
            font-size: 16px;
            margin-bottom: 5px;
            color: #555;
        }
        .service-details .status-pass {
            color: #27ae60;
            font-weight: bold;
        }
        .service-details .status-fail {
            color: #e74c3c;
            font-weight: bold;
        }

        /* Tabs */
        .tabs {
            display: flex;
            border-bottom: 2px solid #3498db;
            margin-bottom: 20px;
        }
        .tabs .tab {
            padding: 10px 20px;
            font-size: 16px;
            cursor: pointer;
            color: #7f8c8d;
            transition: color 0.3s, border-bottom 0.3s;
        }
        .tabs .tab.active {
            color: #3498db;
            border-bottom: 2px solid #3498db;
            font-weight: bold;
        }
        .tabs .tab:hover {
            color: #3498db;
        }
        .tab-content {
            display: none;
            background-color: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .tab-content.active {
            display: block;
        }

        /* Manual Run Tab */
        .manual-run table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .manual-run th, .manual-run td {
            padding: 12px 15px;
            text-align: left;
            border: 1px solid #ddd;
        }
        .manual-run th {
            background-color: #3498db;
            color: white;
        }
        .manual-run tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .status-running { color: #3498db; font-weight: bold; }
        .status-completed { color: #27ae60; font-weight: bold; }
        .status-failed { color: #e74c3c; font-weight: bold; }
        .status-ready { color: #7f8c8d; font-weight: bold; }
        .action-button { position: relative; display: inline-block; }
        .action-button button {
            padding: 5px 10px;
            background-color: #3498db;
            color: white;
            border: none;
            cursor: pointer;
            border-radius: 3px;
        }
        .action-button button:hover {
            background-color: #2980b9;
        }
        .action-button button:disabled {
            background-color: #7f8c8d;
            cursor: not-allowed;
        }
        .action-dropdown {
            display: none;
            position: absolute;
            background-color: #fff;
            box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
            z-index: 1;
            min-width: 160px;
            border-radius: 3px;
        }
        .action-dropdown a {
            color: black;
            padding: 12px 16px;
            text-decoration: none;
            display: block;
        }
        .action-dropdown a:hover {
            background-color: #f1f1f1;
        }
        .action-button:hover .action-dropdown {
            display: block;
        }
        .collapsible-table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
        }
        .collapsible-table th, .collapsible-table td {
            padding: 8px 10px;
            border: 1px solid #ddd;
        }
        .collapsible-table th {
            background-color: #2c3e50;
            color: white;
        }
        .toggle-btn {
            cursor: pointer;
            width: 20px;
            text-align: center;
        }
        .substep-table {
            max-height: 0;
            overflow: hidden;
            width: 90%;
            margin-left: 20px;
            transition: max-height 0.3s ease-in-out;
        }
        .substep-table.open {
            max-height: 500px; /* Adjust based on content */
        }
        .spinner {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid #f3f3f3;
            border-top: 2px solid #3498db;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-right: 5px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .tick { color: #27ae60; font-size: 16px; }
        .cross { color: #e74c3c; font-size: 16px; }
        .download-button {
            background-color: #2ecc71;
            padding: 5px 10px;
            color: white;
            text-decoration: none;
            border-radius: 3px;
            display: inline-block;
        }
        .download-button:hover {
            background-color: #27ae60;
        }

        /* Recent Activity Tab */
        .recent-activity table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .recent-activity th, .recent-activity td {
            padding: 12px 15px;
            text-align: left;
            border: 1px solid #ddd;
        }
        .recent-activity th {
            background-color: #3498db;
            color: white;
        }
        .recent-activity tr:nth-child(even) {
            background-color: #f9f9f9;
        }

        /* Result Tab */
        .result-content h2 {
            color: #34495e;
            border-bottom: 2px solid #3498db;
            padding-bottom: 5px;
            margin-bottom: 15px;
        }
        .result-content table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .result-content th, .result-content td {
            padding: 12px 15px;
            text-align: left;
            border: 1px solid #ddd;
        }
        .result-content th {
            background-color: #3498db;
            color: white;
        }
        .result-content tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .result-content pre {
            background-color: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            white-space: pre-wrap;
        }
        .result-content .high-usage {
            background-color: #e74c3c;
            color: white;
        }
    {% endraw %}
    </style>
    <script>
        // Store intervals for polling each service
        const pollingIntervals = {};

        function toggleTlaGroup(tla) {
            const serviceList = document.getElementById('service-list-' + tla);
            const toggleIcon = document.getElementById('toggle-icon-' + tla);
            if (serviceList.classList.contains('open')) {
                serviceList.classList.remove('open');
                toggleIcon.textContent = '+';
            } else {
                serviceList.classList.add('open');
                toggleIcon.textContent = '−';
            }
        }

        function filterServices() {
            const filter = document.getElementById('service-filter').value.toLowerCase();
            const serviceLinks = document.querySelectorAll('.service-list a');
            serviceLinks.forEach(link => {
                const serviceName = link.textContent.toLowerCase();
                if (serviceName.includes(filter)) {
                    link.classList.remove('hidden');
                } else {
                    link.classList.add('hidden');
                }
            });
        }

        function toggleCollapsible(serviceName) {
            const table = document.getElementById('workflow-' + serviceName);
            const btn = document.getElementById('toggle-' + serviceName);
            if (table.style.display === 'none' || table.style.display === '') {
                table.style.display = 'table';
                btn.textContent = '−';
            } else {
                table.style.display = 'none';
                btn.textContent = '+';
            }
        }

        function toggleSubsteps(serviceName) {
            const substepTable = document.getElementById('substep-table-' + serviceName);
            const btn = document.getElementById('substep-toggle-' + serviceName);
            if (substepTable.classList.contains('open')) {
                substepTable.classList.remove('open');
                btn.textContent = '+';
            } else {
                substepTable.classList.add('open');
                btn.textContent = '−';
            }
        }

        function switchTab(tabName, serviceName) {
            const tabs = document.querySelectorAll('.tab');
            const tabContents = document.querySelectorAll('.tab-content');
            tabs.forEach(tab => tab.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));
            document.getElementById('tab-' + tabName).classList.add('active');
            document.getElementById('content-' + tabName + '-' + serviceName).classList.add('active');
        }

        function updateStatus(serviceName, isSuccess) {
            const statusCell = document.getElementById('status-' + serviceName);
            const loginStatus = document.getElementById('login-status-' + serviceName).innerHTML;
            const troubleshootStatus = document.getElementById('troubleshoot-status-' + serviceName).innerHTML;
            if (loginStatus.includes('✅') && troubleshootStatus.includes('✅')) {
                statusCell.innerHTML = '<span class="status-completed">Completed</span>';
            } else if (loginStatus.includes('❌') || troubleshootStatus.includes('❌')) {
                statusCell.innerHTML = '<span class="status-failed">Failed</span>';
            } else {
                statusCell.innerHTML = '<span class="status-ready">Ready</span>';
            }
            if (!isSuccess) {
                statusCell.innerHTML = '<span class="status-failed">Failed</span>';
            }
        }

        function runHealthCheck(serviceName) {
            const statusCell = document.getElementById('status-' + serviceName);
            const lastRunCell = document.getElementById('last-run-' + serviceName);
            const workflowTable = document.getElementById('workflow-' + serviceName);
            const toggleBtn = document.getElementById('toggle-' + serviceName);
            const actionButton = document.getElementById('action-button-' + serviceName);
            actionButton.disabled = true; // Disable the button while running
            workflowTable.style.display = 'table';
            toggleBtn.textContent = '−';
            statusCell.innerHTML = '<span class="status-running">Running</span>';
            document.getElementById('login-status-' + serviceName).innerHTML = '<span class="spinner"></span> Logging in...';
            document.getElementById('troubleshoot-status-' + serviceName).innerHTML = '<span class="spinner"></span> Running...';
            document.getElementById('download-report-' + serviceName).innerHTML = 'Waiting...';
            for (let i = 0; i < 6; i++) {
                document.getElementById('substep-' + serviceName + '-' + i).innerHTML = 'Waiting...';
            }
            const parts = serviceName.split('_');
            const tla = parts[0];
            const env = parts[parts.length - 1];
            fetch('/run-login-async', {
                method: 'POST',
                headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                body: new URLSearchParams({
                    'tla': tla,
                    'env': env,
                    'service': serviceName
                })
            })
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    document.getElementById('login-status-' + serviceName).innerHTML = '<span class="cross">❌</span> Failed: ' + data.message;
                    updateStatus(serviceName, false);
                    lastRunCell.innerHTML = new Date().toLocaleString();
                    actionButton.disabled = false; // Re-enable the button on failure
                } else {
                    // Start polling for this service
                    pollStatus(serviceName);
                }
            })
            .catch(error => {
                document.getElementById('login-status-' + serviceName).innerHTML = '<span class="cross">❌</span> Error: ' + error;
                updateStatus(serviceName, false);
                lastRunCell.innerHTML = new Date().toLocaleString();
                actionButton.disabled = false; // Re-enable the button on error
            });
        }

        function pollStatus(serviceName) {
            // If already polling for this service, don't start a new interval
            if (pollingIntervals[serviceName]) {
                return;
            }

            let interval = setInterval(() => {
                fetch(`/status?service=${serviceName}`)
                    .then(response => response.json())
                    .then(data => {
                        const statusCell = document.getElementById('status-' + serviceName);
                        const lastRunCell = document.getElementById('last-run-' + serviceName);
                        const actionButton = document.getElementById('action-button-' + serviceName);
                        statusCell.innerHTML = `<span class="status-${data.status.toLowerCase()}">${data.status}</span>`;
                        lastRunCell.innerHTML = data.last_run;

                        // Update Manual Run tab
                        if (data.login_running) {
                            document.getElementById('login-status-' + serviceName).innerHTML = '<span class="spinner"></span> Logging in...';
                        } else if (data.login_completed) {
                            document.getElementById('login-status-' + serviceName).innerHTML = '<span class="tick">✅</span> Success';
                            if (data.troubleshoot_running) {
                                document.getElementById('troubleshoot-status-' + serviceName).innerHTML = '<span class="spinner"></span> Running...';
                                const substeps = ['List Pods', 'SAS Readiness Check', 'List Nodes', 'Node Utilization', 'Check Errors', 'Pod Utilization'];
                                for (let i = 0; i < 6; i++) {
                                    if (data.substep_completed[i]) {
                                        document.getElementById('substep-' + serviceName + '-' + i).innerHTML = '<span class="tick">✅</span> ' + substeps[i];
                                    } else if (data.substep_running[i]) {
                                        document.getElementById('substep-' + serviceName + '-' + i).innerHTML = '<span class="spinner"></span> ' + substeps[i];
                                    } else {
                                        document.getElementById('substep-' + serviceName + '-' + i).innerHTML = 'Waiting...';
                                    }
                                }
                            } else if (data.troubleshoot_completed) {
                                document.getElementById('troubleshoot-status-' + serviceName).innerHTML = '<span class="tick">✅</span> Completed';
                                document.getElementById('download-report-' + serviceName).innerHTML = `
                                    <a href="#" onclick="downloadReport('${serviceName}');" class="download-button">Download Report</a>
                                `;
                                const substeps = ['List Pods', 'SAS Readiness Check', 'List Nodes', 'Node Utilization', 'Check Errors', 'Pod Utilization'];
                                for (let i = 0; i < 6; i++) {
                                    document.getElementById('substep-' + serviceName + '-' + i).innerHTML = '<span class="tick">✅</span> ' + substeps[i];
                                }
                            }
                        } else if (data.login_failed) {
                            document.getElementById('login-status-' + serviceName).innerHTML = '<span class="cross">❌</span> Failed: ' + data.login_message;
                        }

                        // Update Result tab
                        const resultContent = document.getElementById('content-result-' + serviceName);
                        if (resultContent) {
                            resultContent.innerHTML = data.results;
                        }

                        // Update Recent Activity tab
                        const recentActivityTableBody = document.querySelector('#content-recent-activity-' + serviceName + ' tbody');
                        if (recentActivityTableBody) {
                            let html = '';
                            if (data.past_runs.length > 0) {
                                data.past_runs.forEach(run => {
                                    html += `
                                        <tr>
                                            <td>${run.timestamp}</td>
                                            <td>
                                                <a href="#" onclick="downloadPastReport('${serviceName}', '${run.timestamp}');" class="download-button">Download Report</a>
                                            </td>
                                        </tr>
                                    `;
                                });
                            } else {
                                html = '<tr><td colspan="2">No past runs available.</td></tr>';
                            }
                            recentActivityTableBody.innerHTML = html;
                        }

                        // Stop polling and clean up when done
                        if (data.status === 'Completed' || data.status === 'Failed') {
                            clearInterval(interval);
                            delete pollingIntervals[serviceName];
                            actionButton.disabled = false; // Re-enable the button when done
                            const lastHealthCheck = document.getElementById('last-health-check-' + serviceName);
                            if (lastHealthCheck) {
                                lastHealthCheck.innerHTML = `Last Health Check (last run: ${data.last_run}): <span class="status-${data.status === 'Completed' ? 'pass' : 'fail'}">${data.status === 'Completed' ? 'PASS' : 'FAIL'}</span>`;
                            }
                        }
                    })
                    .catch(error => {
                        console.error('Polling error for ' + serviceName + ':', error);
                        clearInterval(interval);
                        delete pollingIntervals[serviceName];
                        const actionButton = document.getElementById('action-button-' + serviceName);
                        actionButton.disabled = false; // Re-enable the button on error
                    });
            }, 2000); // Poll every 2 seconds
            pollingIntervals[serviceName] = interval;
        }

        function downloadReport(serviceName) {
            window.location.href = '/download-report?service=' + serviceName;
        }

        function downloadPastReport(serviceName, timestamp) {
            window.location.href = `/download-past-report?service=${serviceName}&timestamp=${timestamp}`;
        }

        // Start polling for all services that are running on page load
        document.addEventListener('DOMContentLoaded', function() {
            const selectedService = new URLSearchParams(window.location.search).get('service');
            if (selectedService) {
                switchTab('manual-run', selectedService);
            }

            // Start polling for all services that are in "Running" state
            const services = {{ services | tojson }};
            services.forEach(service => {
                fetch(`/status?service=${service}`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.status === 'Running') {
                            pollStatus(service);
                        }
                    })
                    .catch(error => console.error('Error checking status for ' + service + ':', error));
            });
        });
    </script>
</head>
<body>
    <!-- Vertical Menu -->
    <div class="sidebar">
        <h2>Services</h2>
        <input type="text" id="service-filter" class="filter-box" placeholder="Filter services..." onkeyup="filterServices()">
        {% for tla, service_list in grouped_services.items() %}
            <div class="tla-group">
                <div class="tla-header" onclick="toggleTlaGroup('{{ tla }}')">
                    <span>{{ tla }}</span>
                    <span class="toggle-icon" id="toggle-icon-{{ tla }}">+</span>
                </div>
                <div class="service-list" id="service-list-{{ tla }}">
                    {% for service in service_list %}
                        <a href="?service={{ service }}" class="{% if selected_service == service %}active{% endif %}">{{ service }}</a>
                    {% endfor %}
                </div>
            </div>
        {% endfor %}
    </div>

    <!-- Main Content -->
    <div class="main-content">
        <h1>SAS Viya 4 Troubleshooting Portal</h1>
        {% if selected_service %}
            <div class="service-details">
                <h2>{{ selected_service }}</h2>
                <p><strong>Kubernetes Version:</strong> {{ kube_version }}</p>
                <p><strong>Resource Group:</strong> {{ resource_group }}</p>
                <p><strong>Namespace:</strong> {{ namespace }}</p>
                <p><strong>State:</strong> {{ sas_deployment.get('state', 'N/A') }}</p>
                <p><strong>Cadence Name:</strong> {{ sas_deployment.get('cadence_name', 'N/A') }}</p>
                <p><strong>Cadence Version:</strong> {{ sas_deployment.get('cadence_version', 'N/A') }}</p>
                <p><strong>Cadence Release:</strong> {{ sas_deployment.get('cadence_release', 'N/A') }}</p>
                <p id="last-health-check-{{ selected_service }}">
                    <strong>Last Health Check (last run: {{ session.get('last_run_' + selected_service, 'N/A') }}):</strong>
                    <span class="status-{% if session.get('status_' + selected_service, 'Ready') == 'Completed' %}pass{% else %}fail{% endif %}">
                        {% if session.get('status_' + selected_service, 'Ready') == 'Completed' %}
                            PASS
                        {% elif session.get('status_' + selected_service, 'Ready') == 'Failed' %}
                            FAIL
                        {% else %}
                            N/A
                        {% endif %}
                    </span>
                </p>
            </div>

            <!-- Tabs -->
            <div class="tabs">
                <div class="tab" id="tab-manual-run" onclick="switchTab('manual-run', '{{ selected_service }}')">Manual Run</div>
                <div class="tab" id="tab-recent-activity" onclick="switchTab('recent-activity', '{{ selected_service }}')">Recent Activity</div>
                <div class="tab" id="tab-result" onclick="switchTab('result', '{{ selected_service }}')">Result</div>
            </div>

            <!-- Tab Contents -->
            <!-- Manual Run Tab -->
            <div class="tab-content manual-run" id="content-manual-run-{{ selected_service }}">
                <table>
                    <thead>
                        <tr>
                            <th>Service Name</th>
                            <th>TLA</th>
                            <th>Env</th>
                            <th>Last Trouble Run</th>
                            <th>Status</th>
                            <th>Actions</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {% set parts = selected_service.split('_') %}
                        {% set tla = parts[0] %}
                        {% set env = parts[-1] %}
                        {% set service_status = session.get('status_' + selected_service, 'Ready') %}
                        {% set last_run = session.get('last_run_' + selected_service, '') %}
                        <tr>
                            <td>{{ selected_service }}</td>
                            <td>{{ tla }}</td>
                            <td>{{ env }}</td>
                            <td id="last-run-{{ selected_service }}">{{ last_run }}</td>
                            <td id="status-{{ selected_service }}">
                                {% if service_status == 'Running' %}
                                    <span class="status-running">Running</span>
                                {% elif service_status == 'Completed' %}
                                    <span class="status-completed">Completed</span>
                                {% elif service_status == 'Failed' %}
                                    <span class="status-failed">Failed</span>
                                {% else %}
                                    <span class="status-ready">Ready</span>
                                {% endif %}
                            </td>
                            <td>
                                <div class="action-button">
                                    <button id="action-button-{{ selected_service }}" {% if service_status == 'Running' %}disabled{% endif %}>Action ▼</button>
                                    <div class="action-dropdown">
                                        <a href="#" onclick="runHealthCheck('{{ selected_service }}'); return false;">Run Health Check</a>
                                    </div>
                                </div>
                            </td>
                            <td class="toggle-btn" id="toggle-{{ selected_service }}" onclick="toggleCollapsible('{{ selected_service }}')">+</td>
                        </tr>
                        <tr>
                            <td colspan="7">
                                <table id="workflow-{{ selected_service }}" class="collapsible-table" style="display: none;">
                                    <thead>
                                        <tr>
                                            <th>Step</th>
                                            <th>Status</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>Login to Cluster</td>
                                            <td id="login-status-{{ selected_service }}">Waiting...</td>
                                        </tr>
                                        <tr>
                                            <td>
                                                Viya Troubleshooting
                                                <span class="toggle-btn" id="substep-toggle-{{ selected_service }}" onclick="toggleSubsteps('{{ selected_service }}')">+</span>
                                            </td>
                                            <td id="troubleshoot-status-{{ selected_service }}">Waiting...</td>
                                        </tr>
                                        <tr>
                                            <td colspan="2">
                                                <table id="substep-table-{{ selected_service }}" class="substep-table">
                                                    <tbody>
                                                        {% for i in range(6) %}
                                                            <tr>
                                                                <td id="substep-{{ selected_service }}-{{ i }}">Waiting...</td>
                                                            </tr>
                                                        {% endfor %}
                                                    </tbody>
                                                </table>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Download Report</td>
                                            <td id="download-report-{{ selected_service }}">Waiting...</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <!-- Recent Activity Tab -->
            <div class="tab-content recent-activity" id="content-recent-activity-{{ selected_service }}">
                <table>
                    <thead>
                        <tr>
                            <th>Timestamp</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for run in past_runs %}
                            <tr>
                                <td>{{ run.timestamp }}</td>
                                <td>
                                    <a href="#" onclick="downloadPastReport('{{ selected_service }}', '{{ run.timestamp }}');" class="download-button">Download Report</a>
                                </td>
                            </tr>
                        {% else %}
                            <tr>
                                <td colspan="2">No past runs available.</td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <!-- Result Tab -->
            <div class="tab-content result-content" id="content-result-{{ selected_service }}">
                {{ last_report | safe }}
            </div>
        {% else %}
            <p>Please select a service from the menu to begin troubleshooting.</p>
        {% endif %}
    </div>
</body>
</html>
"""

def run_command(command, timeout=10, env=None):
    logger.debug(f"Executing command: {command}")
    try:
        env = env or os.environ.copy()
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True, timeout=timeout, env=env)
        return result.stdout.strip(), result.stderr.strip(), 0
    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out: {command}")
        return "", f"Error: Command timed out after {timeout} seconds", 1
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {command}, Error: {e}, Stderr: {e.stderr}")
        return e.stdout.strip(), e.stderr.strip(), e.returncode
    except Exception as e:
        logger.error(f"Unexpected error executing command: {command}, Error: {e}")
        return "", f"Error: {e}", 1

def check_kubeconfig_context(kubeconfig_path):
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    current_context, _, _ = run_command("kubectl config current-context", env=env)
    current_namespace, _, _ = run_command("kubectl config view --minify --output 'jsonpath={..namespace}'", env=env)
    logger.info(f"Current kubeconfig context: {current_context}")
    logger.info(f"Current namespace in context: {current_namespace}")

def check_az_authentication():
    logger.info("Checking az CLI authentication")
    stdout, stderr, returncode = run_command("az account show --output json", timeout=10)
    if returncode == 0 and stdout:
        try:
            json.loads(stdout)
            logger.info("az CLI is authenticated")
            return True
        except json.JSONDecodeError:
            logger.error("az account show returned invalid JSON")
            return False
    logger.error(f"az CLI is not authenticated. Stderr: {stderr}")
    return False

def get_kube_version(kubeconfig_path):
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command("kubectl version | grep Server | cut -d'\"' -f 6", env=env)
    if returncode == 0 and stdout:
        return stdout
    return "N/A"

def get_sas_deployment_info(namespace, kubeconfig_path):
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command(f"kubectl get sasdeployment -n {namespace} --no-headers", env=env)
    if returncode == 0 and stdout:
        parts = stdout.split()
        if len(parts) >= 5:
            return {
                'state': parts[1],
                'cadence_name': parts[2],
                'cadence_version': parts[3],
                'cadence_release': parts[4]
            }
    return {
        'state': 'N/A',
        'cadence_name': 'N/A',
        'cadence_version': 'N/A',
        'cadence_release': 'N/A'
    }

def run_login_script(tla, env, service):
    tla = tla.lower()
    env = env.lower()
    if not check_az_authentication():
        return False, "Error: az CLI is not authenticated. Please run 'az login --use-device-code' in the terminal and try again.", None
    logger.info(f"Running login.sh with TLA: {tla}, Env: {env}, Service: {service}")
    ns = f"{tla}{env}"
    kubeconfig_path = f"/home/anzdes/kubeconfig/{ns}/.kube/config"
    log_file = f"/tmp/login_sh_output_{service}.log"
    login_script_path = "/home/anzdes/viya-upgrade-scripts/login.sh"
    if not os.path.isfile(login_script_path):
        logger.error(f"login.sh not found at {login_script_path}")
        return False, f"Error: login.sh not found at {login_script_path}", None
    if not os.access(login_script_path, os.X_OK):
        logger.warning(f"login.sh is not executable. Attempting to make it executable.")
        try:
            os.chmod(login_script_path, 0o755)
        except Exception as e:
            logger.error(f"Failed to make login.sh executable: {e}")
            return False, f"Error: Failed to make login.sh executable: {e}", None
    command = f"{login_script_path} {shlex.quote(tla)} {shlex.quote(env)} > {log_file} 2>&1 & echo $!"
    stdout, stderr, returncode = run_command(command, timeout=10)
    if returncode != 0:
        logger.error(f"Failed to start login.sh: Stdout: {stdout}, Stderr: {stderr}")
        return False, f"Error: Failed to start login.sh: {stderr}", None
    pid = stdout.strip()
    if not pid.isdigit():
        logger.error(f"Could not get PID of login.sh: {stdout}")
        return False, f"Error: Could not get PID of login.sh: {stdout}", None
    pid = int(pid)
    logger.info(f"login.sh started with PID: {pid} for service: {service}")
    max_wait_time = 300
    poll_interval = 10
    elapsed_time = 0
    while elapsed_time < max_wait_time:
        try:
            process = psutil.Process(pid)
            if not process.is_running():
                with open(log_file, "r") as f:
                    output = f.read()
                if re.search(r'\S+\s+\d+/\d+\s+(Running|Completed)\s+\d+', output):
                    logger.info(f"login.sh completed successfully for {service}")
                    return True, "Login successful.", pid
                else:
                    exit_code = process.returncode
                    logger.error(f"login.sh failed with exit code {exit_code} for {service}. Output: {output}")
                    return False, f"Error: login.sh failed with exit code {exit_code}. Output: {output}", pid
        except psutil.NoSuchProcess:
            with open(log_file, "r") as f:
                output = f.read()
            if re.search(r'\S+\s+\d+/\d+\s+(Running|Completed)\s+\d+', output):
                logger.info(f"login.sh completed successfully for {service}")
                return True, "Login successful.", pid
            else:
                logger.error(f"login.sh failed for {service}. Output: {output}")
                return False, f"Error: login.sh failed. Output: {output}", pid
        time.sleep(poll_interval)
        elapsed_time += poll_interval
    logger.error(f"login.sh timed out after 300 seconds for {service}")
    try:
        process = psutil.Process(pid)
        process.terminate()
    except psutil.NoSuchProcess:
        pass
    with open(log_file, "r") as f:
        output = f.read()
    return False, f"Error: login.sh timed out after 300 seconds. Output: {output}", pid

def check_for_updates():
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
    script_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/viya4_environment_troubleshooting_v1.py"
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

def generate_results_html(html_data):
    content = ""
    for key, data in html_data.items():
        if key in ('pods', 'nodes', 'resources', 'pod_resources'):
            content += f"<h2>{key.replace('_', ' ').title()}</h2>\n"
            if not data['rows']:
                content += "<p>No data available.</p>\n"
            else:
                if data['headers'] == ["Message"]:
                    content += "<pre>\n"
                    for row in data['rows']:
                        content += f"{row[0]}\n"
                    content += "</pre>\n"
                else:
                    content += "<table>\n<tr>"
                    content += "".join(f"<th>{h}</th>" for h in data['headers']) + "</tr>\n"
                    for row_data in data['rows']:
                        if isinstance(row_data, tuple) and len(row_data) == 3:
                            row, high_usage, _ = row_data
                        else:
                            row = row_data
                            high_usage = False
                        content += "<tr>"
                        for i, cell in enumerate(row):
                            class_attr = ' class="high-usage"' if (key == 'resources' and i == 9 and high_usage) or (key == 'pod_resources' and i == 6 and high_usage) else ''
                            content += f"<td{class_attr}>{cell}</td>"
                        content += "</tr>\n"
                    content += "</table>\n"
        elif key == 'readiness':
            content += "<h2>SAS Readiness Check</h2>\n<pre>" + data + "</pre>\n"
        elif key == 'errors':
            content += "<h2>Check Pods for Errors</h2>\n"
            if not data['rows']:
                content += "<p>No error data available.</p>\n"
            else:
                if data['headers'] == ["Message"]:
                    content += "<pre>\n"
                    for row in data['rows']:
                        content += f"{row[0]}\n"
                    content += "</pre>\n"
                else:
                    content += "<pre>\n"
                    first_pod = True
                    prev_pod = None
                    for row in data['rows']:
                        pod_name, _, _, level, message = row
                        if pod_name and pod_name != prev_pod and "No messages" not in pod_name:
                            if not first_pod:
                                content += "\n"
                            content += f"Pod name: {pod_name}\n----------\n"
                            prev_pod = pod_name
                            first_pod = False
                        if message and "All pods checked" not in message:
                            content += f"{level}: {message}\n"
                        elif "All pods checked" in message:
                            content += f"{message}\n"
                    content += "</pre>\n"
    return content

def list_pods(namespace, html_data, kubeconfig_path):
    logger.info(f"Listing pods in namespace: {namespace}")
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command(f"kubectl get pods -n {namespace} --no-headers", env=env)
    if returncode == 0 and stdout:
        lines = stdout.split('\n')
        headers = ["NAME", "READY", "STATUS", "RESTARTS", "AGE"]
        rows = [line.split(maxsplit=4) for line in lines if line.strip()]
        html_data['pods'] = {'headers': headers, 'rows': rows}
    else:
        html_data['pods'] = {'headers': ["Message"], 'rows': [[f"Failed to list pods: {stderr}"]]}

def sas_readiness_check(namespace, html_data, kubeconfig_path):
    logger.info(f"Checking SAS readiness in namespace: {namespace}")
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command(f"kubectl get pod -n {namespace} -l app=sas-readiness -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready --no-headers", env=env)
    if returncode != 0 or not stdout:
        html_data['readiness'] = f"No sas-readiness pod found or error: {stderr}"
    else:
        pod_info = stdout.split()
        if len(pod_info) < 2:
            html_data['readiness'] = "Could not parse sas-readiness status"
        else:
            pod_name, readiness = pod_info[0], pod_info[1]
            readiness_status, _, _ = run_command(f"kubectl get pod -n {namespace} {pod_name} -o jsonpath='{{.status.containerStatuses[*].ready}}'", env=env)
            is_ready = readiness_status == "true" and readiness == "true"
            last_log, _, _ = run_command(f"kubectl logs -n {namespace} {pod_name} --tail=1", env=env)
            if is_ready and last_log and "All checks passed" in last_log:
                html_data['readiness'] = f"SAS Readiness Check: All good! Pod '{pod_name}' is ready."
            else:
                html_data['readiness'] = f"SAS Readiness Check: Pod '{pod_name}' not fully ready.\nLast log: {last_log or 'No logs'}"

def list_nodes_and_utilization(namespace, html_data, kubeconfig_path):
    logger.info("Listing nodes and utilization")
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command("kubectl top nodes --no-headers", env=env)
    if returncode == 0 and stdout:
        lines = stdout.split('\n')
        headers = ["NAME", "CPU(cores)", "CPU%", "MEMORY(bytes)", "MEMORY%"]
        rows = [line.split(maxsplit=4) for line in lines if line.strip()]
        html_data['nodes'] = {'headers': headers, 'rows': rows}
    else:
        html_data['nodes'] = {'headers': ["Message"], 'rows': [["Failed to list nodes"]]}

def parse_resource_value(value, is_cpu=False):
    if not value or value == "0":
        return 0
    value = value.strip()
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
        logger.warning(f"Could not parse resource value: {value}")
        return 0

def node_resource_utilization(namespace, html_data, kubeconfig_path):
    logger.info("Checking node resource utilization")
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command("kubectl get nodes --no-headers", env=env)
    if returncode != 0 or not stdout:
        html_data['resources'] = {'headers': ["Message"], 'rows': [["Failed to get nodes"]]}
        return
    nodes = [line.split()[0] for line in stdout.split('\n') if line.strip()]
    headers = ["Node", "Allocatable CPU", "CPU Requests", "CPU Req %", "CPU Limits", "CPU Lim %", "CPU Remaining", 
               "Allocatable Memory", "Memory Requests", "Memory Req %", "Memory Limits", "Memory Lim %", "Memory Remaining"]
    table_data = []
    for node in nodes:
        describe_output, _, _ = run_command(f"kubectl describe node {node}", env=env)
        if not describe_output:
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
    html_data['resources'] = {'headers': headers, 'rows': table_data}

def check_pods_for_errors(namespace, html_data, kubeconfig_path):
    logger.info(f"Checking pods for errors in namespace: {namespace}")
    sas_pods = ["sas-arke", "sas-authorization", "sas-compute", "sas-configuration", "sas-credentials", 
                "sas-feature-flags", "sas-files", "sas-identities", "sas-job-execution", "sas-job-execution-app",
                "sas-launcher", "sas-logon-app", "sas-microanalytic-score", "sas-readiness", "sas-scheduler", 
                "sas-search", "sas-studio-app", "sas-visual-analytics", "sas-visual-analytics-app"]
    valid_levels = {"error", "warn"}
    log_entries = []
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command(f"kubectl get pods -n {namespace} --no-headers", env=env)
    if returncode != 0 or not stdout:
        html_data['errors'] = {'headers': ["Message"], 'rows': [["Failed to list pods"]]}
        return
    running_pods = {line.split()[0] for line in stdout.split('\n') if line.strip()}
    for pod_prefix in sas_pods:
        for pod in running_pods:
            if pod.startswith(pod_prefix):
                logs, _, _ = run_command(f"kubectl logs -n {namespace} {pod}", env=env)
                if logs:
                    unique_messages = {}
                    for line in logs.split('\n'):
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
                        log_entries.extend([[pod, "", "", lvl.upper(), msg] for msg, lvl in unique_messages.items()][:10])
    if not log_entries:
        log_entries.append(["No messages", "", "", "", "All pods checked, no ERROR/WARN lines detected"])
    html_data['errors'] = {'headers': ["POD_NAME", "DATE", "TIME", "LOG_CODE", "MESSAGE"], 'rows': log_entries}

def pod_resource_utilization(namespace, html_data, kubeconfig_path):
    logger.info(f"Checking pod resource utilization in namespace: {namespace}")
    pod_prefixes = ('sas-authorization', 'sas-identities', 'sas-search', 'sas-arke', 'sas-studio-app', 'sas-studio', 'sas-launcher', 'sas-credentials', 'sas-crunchy-platform-postgres', 'sas-rabbitmq-server', 'sas-consul-server')
    env = os.environ.copy()
    env['KUBECONFIG'] = kubeconfig_path
    stdout, stderr, returncode = run_command(f"kubectl get pods -n {namespace} --no-headers", env=env)
    if returncode != 0 or not stdout:
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["Failed to list pods"]]}
        return
    pods = [line.split()[0] for line in stdout.split('\n') if line.strip() and line.split()[0].startswith(pod_prefixes)]
    if not pods:
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["No specified pods found"]]}
        return
    top_output, _, _ = run_command(f"kubectl top pods -n {namespace} --no-headers", env=env)
    if not top_output:
        html_data['pod_resources'] = {'headers': ["Message"], 'rows': [["Failed to get pod utilization"]]}
        return
    usage_data = {}
    for line in top_output.split('\n'):
        if line.strip():
            match = re.match(r'(\S+)\s+(\d+m?)\s+(\d+(?:Mi|Gi|Ki)?)', line)
            if match:
                pod_name, cpu_usage, mem_usage = match.groups()
                if pod_name.startswith(pod_prefixes):
                    usage_data[pod_name] = {
                        'cpu_usage': parse_resource_value(cpu_usage, is_cpu=True),
                        'mem_usage': parse_resource_value(mem_usage, is_cpu=False)
                    }
    headers = ["Pod Name", "CPU Usage", "CPU Lim", "CPU Lim %", "Memory Usage", "Mem Lim", "Mem Lim %"]
    table_data = []
    for pod in pods:
        describe_output, _, _ = run_command(f"kubectl describe pod -n {namespace} {pod}", env=env)
        if not describe_output:
            continue
        cpu_lim, mem_lim = 0, 0
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
                    elif subline.startswith("Requests:"):
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
    html_data['pod_resources'] = {'headers': headers, 'rows': table_data}

def generate_report_html(tla, env, results):
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SAS Viya 4 Troubleshooting Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; color: #333; }}
            h1 {{ text-align: center; color: #2c3e50; }}
            h2 {{ color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; box-shadow: 0 2px 5px rgba(0,0,0,0.1); background-color: #fff; }}
            th, td {{ padding: 12px 15px; text-align: left; border: 1px solid #ddd; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            tr.highlight {{ background-color: #f1c40f; color: #333; }}
            td.high-usage {{ background-color: #e74c3c; color: white; }}
            pre {{ background-color: #ecf0f1; padding: 15px; border-radius: 5px; white-space: pre-wrap; }}
        </style>
    </head>
    <body>
        <h1>SAS Viya 4 Troubleshooting Report</h1>
        <p><strong>Cluster:</strong> {tla}{env}</p>
        <p><strong>Generated on:</strong> {timestamp}</p>
        {results}
    </body>
    </html>
    """
    return html_content.format(
        tla=tla,
        env=env,
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        results=results
    )

def troubleshoot_service(service, tla, env):
    namespace = f"{tla.lower()}{env.lower()}"
    kubeconfig_path = f"/home/anzdes/kubeconfig/{namespace}/.kube/config"
    check_kubeconfig_context(kubeconfig_path)
    try:
        html_data = {}
        substeps = ['list_pods', 'sas_readiness_check', 'list_nodes_and_utilization', 
                    'node_resource_utilization', 'check_pods_for_errors', 'pod_resource_utilization']
        substep_functions = {
            'list_pods': list_pods,
            'sas_readiness_check': sas_readiness_check,
            'list_nodes_and_utilization': list_nodes_and_utilization,
            'node_resource_utilization': node_resource_utilization,
            'check_pods_for_errors': check_pods_for_errors,
            'pod_resource_utilization': pod_resource_utilization
        }
        
        # Initialize substep states in task_results
        if 'substep_running' not in task_results[service]:
            task_results[service]['substep_running'] = [False] * len(substeps)
            task_results[service]['substep_completed'] = [False] * len(substeps)
        
        logger.info(f"Starting troubleshooting steps for {service}")
        for i, substep in enumerate(substeps):
            task_results[service]['substep_running'][i] = True
            logger.info(f"Running substep {substep} for {service}")
            substep_functions[substep](namespace, html_data, kubeconfig_path)
            task_results[service]['substep_running'][i] = False
            task_results[service]['substep_completed'][i] = True
            logger.info(f"Completed substep {substep} for {service}")
        
        results = generate_results_html(html_data)
        return True, "", {'results': results, 'html_data': html_data}
    except Exception as e:
        logger.error(f"Error during processing for {service}: {e}", exc_info=True)
        return False, str(e), None

@app.route('/', methods=['GET'])
def index():
    logger.info("Received GET request to /")
    selected_service = request.args.get('service', None)
    if selected_service and selected_service not in SERVICES:
        selected_service = None

    # Reset status-related variables on page reload, except for "Running"
    for service in SERVICES:
        current_status = session.get('status_' + service, 'Ready')
        if current_status != 'Running':
            session['status_' + service] = 'Ready'
            session.pop('last_run_' + service, None)
            # Preserve results and html_data to keep the Result tab populated
            session.pop('login_running_' + service, None)
            session.pop('login_completed_' + service, None)
            session.pop('login_failed_' + service, None)
            session.pop('login_message_' + service, None)
            session.pop('troubleshoot_running_' + service, None)
            session.pop('troubleshoot_completed_' + service, None)
            for i in range(6):
                session.pop(f'substep_running_{service}_{i}', None)
                session.pop(f'substep_completed_{service}_{i}', None)

    # Group services by TLA
    grouped_services = group_services_by_tla(SERVICES)

    if not selected_service:
        return render_template_string(HTML_TEMPLATE, 
                                     grouped_services=grouped_services,
                                     selected_service=None,
                                     services=SERVICES)

    # Fetch Kubernetes version, resource group, and namespace
    tla = selected_service.split('_')[0].lower()
    env = selected_service.split('_')[-1].lower()
    namespace = f"{tla}{env}"
    resource_group = f"{tla}-{env}"
    kubeconfig_path = f"/home/anzdes/kubeconfig/{namespace}/.kube/config"
    kube_version = get_kube_version(kubeconfig_path)
    sas_deployment = get_sas_deployment_info(namespace, kubeconfig_path)

    # Get past runs and last report
    past_runs = session.get(f'past_runs_{selected_service}', [])
    last_report = session.get(f'results_{selected_service}', '<p>No results available.</p>')

    return render_template_string(HTML_TEMPLATE, 
                                 grouped_services=grouped_services,
                                 selected_service=selected_service,
                                 services=SERVICES,
                                 kube_version=kube_version,
                                 resource_group=resource_group,
                                 namespace=namespace,
                                 sas_deployment=sas_deployment,
                                 past_runs=past_runs,
                                 last_report=last_report)

@app.route('/run-login-async', methods=['POST'])
def run_login_async():
    logger.info("Received POST request to /run-login-async")
    tla = request.form.get('tla', '').strip()
    env = request.form.get('env', '').strip()
    service = request.form.get('service', '').strip()
    logger.info(f"Form inputs - TLA: {tla}, Env: {env}, Service: {service}")

    # Check if a health check is already running for this service
    current_status = session.get(f'status_{service}', 'Ready')
    if current_status == 'Running':
        return jsonify({'success': False, 'message': 'A health check is already running for this service. Please wait until it completes.'})

    # Clear previous results before starting a new run
    session.pop(f'results_{service}', None)
    session.pop(f'html_data_{service}', None)
    session[f'tla_{service}'] = tla
    session[f'env_{service}'] = env
    session[f'status_{service}'] = 'Running'
    session[f'login_running_{service}'] = True
    session[f'login_completed_{service}'] = False
    session[f'login_failed_{service}'] = False
    session[f'login_message_{service}'] = ""
    session[f'troubleshoot_running_{service}'] = False
    session[f'troubleshoot_completed_{service}'] = False
    future = executor.submit(run_login_script, tla, env, service)
    task_results[service]['login_future'] = future
    troubleshoot_future = executor.submit(troubleshoot_service, service, tla, env)
    task_results[service]['troubleshoot_future'] = troubleshoot_future
    return jsonify({'success': True, 'message': 'Login process started'})

@app.route('/status', methods=['GET'])
def get_status():
    service = request.args.get('service', '').strip()
    if service not in SERVICES:
        return jsonify({'error': 'Service not found'}), 404
    
    # Existing status variables
    status = session.get(f'status_{service}', 'Ready')
    last_run = session.get(f'last_run_{service}', '')
    login_running = session.get(f'login_running_{service}', False)
    login_completed = session.get(f'login_completed_{service}', False)
    login_failed = session.get(f'login_failed_{service}', False)
    login_message = session.get(f'login_message_{service}', '')
    troubleshoot_running = session.get(f'troubleshoot_running_{service}', False)
    troubleshoot_completed = session.get(f'troubleshoot_completed_{service}', False)
    
    # Sync substep states from task_results to session
    substep_running = task_results[service].get('substep_running', [False] * 6)
    substep_completed = task_results[service].get('substep_completed', [False] * 6)
    for i in range(6):
        session[f'substep_running_{service}_{i}'] = substep_running[i]
        session[f'substep_completed_{service}_{i}'] = substep_completed[i]
    
    # Handle login future
    if 'login_future' in task_results[service]:
        login_future = task_results[service]['login_future']
        if login_future.done():
            success, message, pid = login_future.result()
            if success:
                session[f'login_running_{service}'] = False
                session[f'login_completed_{service}'] = True
                session[f'login_script_pid_{service}'] = pid
                logger.info(f"Login completed for {service}")
                if not troubleshoot_running and not troubleshoot_completed:
                    session[f'troubleshoot_running_{service}'] = True
            else:
                session[f'login_running_{service}'] = False
                session[f'login_failed_{service}'] = True
                session[f'login_message_{service}'] = message
                session[f'status_{service}'] = 'Failed'
                session[f'last_run_{service}'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if pid:
                    session[f'login_script_pid_{service}'] = pid
                logger.error(f"Login failed for {service}: {message}")
            del task_results[service]['login_future']

    # Handle troubleshoot future
    if 'troubleshoot_future' in task_results[service] and login_completed:
        troubleshoot_future = task_results[service]['troubleshoot_future']
        if troubleshoot_future.done():
            t_success, t_message, t_data = troubleshoot_future.result()
            if t_success:
                session[f'results_{service}'] = t_data['results']
                session[f'html_data_{service}'] = t_data['html_data']
                session[f'troubleshoot_running_{service}'] = False
                session[f'troubleshoot_completed_{service}'] = True
                session[f'status_{service}'] = 'Completed'
                last_run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                session[f'last_run_{service}'] = last_run_time
                # Store the run in past_runs
                past_runs = session.get(f'past_runs_{service}', [])
                past_runs.append({
                    'timestamp': last_run_time,
                    'results': t_data['results'],
                    'html_data': t_data['html_data']
                })
                session[f'past_runs_{service}'] = past_runs[-5:]  # Keep only the last 5 runs
                logger.info(f"Troubleshooting completed for {service}")
            else:
                session[f'troubleshoot_running_{service}'] = False
                session[f'status_{service}'] = 'Failed'
                session[f'last_run_{service}'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logger.error(f"Troubleshooting failed for {service}: {t_message}")
                # Clean up task_results for this service
                if 'troubleshoot_future' in task_results[service]:
                    del task_results[service]['troubleshoot_future']
                # Reset substep states
                task_results[service]['substep_running'] = [False] * 6
                task_results[service]['substep_completed'] = [False] * 6
                for i in range(6):
                    session[f'substep_running_{service}_{i}'] = False
                    session[f'substep_completed_{service}_{i}'] = False

    # Add results and past_runs to the response
    results = session.get(f'results_{service}', '<p>No results available.</p>')
    past_runs = session.get(f'past_runs_{service}', [])

    # Prepare the response for the UI
    response = {
        'status': session.get(f'status_{service}', 'Ready'),
        'last_run': session.get(f'last_run_{service}', ''),
        'login_running': session.get(f'login_running_{service}', False),
        'login_completed': session.get(f'login_completed_{service}', False),
        'login_failed': session.get(f'login_failed_{service}', False),
        'login_message': session.get(f'login_message_{service}', ''),
        'troubleshoot_running': session.get(f'troubleshoot_running_{service}', False),
        'troubleshoot_completed': session.get(f'troubleshoot_completed_{service}', False),
        'substep_running': [session.get(f'substep_running_{service}_{i}', False) for i in range(6)],
        'substep_completed': [session.get(f'substep_completed_{service}_{i}', False) for i in range(6)],
        'results': results,  # Add results data
        'past_runs': [{'timestamp': run['timestamp']} for run in past_runs]  # Add past runs timestamps
    }
    return jsonify(response)

@app.route('/download-report', methods=['GET'])
def download_report():
    service = request.args.get('service', '').strip()
    if service not in SERVICES:
        return jsonify({'error': 'Service not found'}), 404
    results = session.get(f'results_{service}', None)
    if not results:
        return jsonify({'error': 'No report available'}), 404
    tla = session.get(f'tla_{service}', '')
    env = session.get(f'env_{service}', '')
    html_content = generate_report_html(tla, env, results)
    buffer = BytesIO()
    buffer.write(html_content.encode('utf-8'))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"{service}_troubleshooting_report.html",
        mimetype='text/html'
    )

@app.route('/download-past-report', methods=['GET'])
def download_past_report():
    service = request.args.get('service', '').strip()
    timestamp = request.args.get('timestamp', '').strip()
    if service not in SERVICES:
        return jsonify({'error': 'Service not found'}), 404
    past_runs = session.get(f'past_runs_{service}', [])
    selected_run = next((run for run in past_runs if run['timestamp'] == timestamp), None)
    if not selected_run:
        return jsonify({'error': 'Past report not found'}), 404
    tla = service.split('_')[0].lower()
    env = service.split('_')[-1].lower()
    html_content = generate_report_html(tla, env, selected_run['results'])
    buffer = BytesIO()
    buffer.write(html_content.encode('utf-8'))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"{service}_troubleshooting_report_{timestamp.replace(' ', '_').replace(':', '-')}.html",
        mimetype='text/html'
    )

if __name__ == '__main__':
    has_update, latest_version = check_for_updates()
    if has_update:
        logger.info(f"New version {latest_version} is available. Current version: {SCRIPT_VERSION}")
        if update_script():
            logger.info("Script updated successfully. Please restart the application.")
            sys.exit(0)
        else:
            logger.error("Failed to update the script. Continuing with the current version.")
    else:
        logger.info(f"Script is up-to-date. Running version: {SCRIPT_VERSION}")

    # Start the Flask application
    try:
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logger.error(f"Failed to start Flask application: {e}")
        sys.exit(1)
