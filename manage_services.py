#!/usr/bin/env python3
"""
AIS Services Management Script
Manages the AIS Collector and L&W Correction Service using Docker Compose
"""

import os
import sys
import subprocess
import argparse
import time

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n{description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False

def check_docker():
    """Check if Docker and Docker Compose are available"""
    print("Checking Docker availability...")
    
    # Check Docker
    if not run_command("docker --version", "Checking Docker"):
        print("Docker is not installed or not running. Please install Docker first.")
        return False
    
    # Check Docker Compose
    if not run_command("docker-compose --version", "Checking Docker Compose"):
        print("Docker Compose is not installed. Please install Docker Compose first.")
        return False
    
    return True

def start_services():
    """Start AIS services using Docker Compose"""
    print("=" * 60)
    print("STARTING AIS SERVICES")
    print("=" * 60)
    
    if not check_docker():
        return False
    
    # Build and start services
    commands = [
        ("docker-compose build", "Building Docker images"),
        ("docker-compose up -d", "Starting AIS services"),
    ]
    
    for command, description in commands:
        if not run_command(command, description):
            return False
    
    # Wait a moment and check status
    time.sleep(3)
    run_command("docker-compose ps", "Checking service status")
    
    print("\n" + "=" * 60)
    print("AIS SERVICES STARTED SUCCESSFULLY")
    print("=" * 60)
    print("Services running:")
    print("  • AIS Collector (ais_collector.py)")
    print("  • L&W Correction Service (lw_correction_service.py)")
    print("  • Cloud SQL Proxy")
    print("\nTo view logs: python manage_services.py logs")
    print("To stop services: python manage_services.py stop")
    
    return True

def stop_services():
    """Stop AIS services"""
    print("=" * 60)
    print("STOPPING AIS SERVICES")
    print("=" * 60)
    
    if not run_command("docker-compose down", "Stopping AIS services"):
        return False
    
    print("\n" + "=" * 60)
    print("AIS SERVICES STOPPED SUCCESSFULLY")
    print("=" * 60)
    
    return True

def restart_services():
    """Restart AIS services"""
    print("=" * 60)
    print("RESTARTING AIS SERVICES")
    print("=" * 60)
    
    stop_services()
    time.sleep(2)
    start_services()

def show_status():
    """Show status of AIS services"""
    print("=" * 60)
    print("AIS SERVICES STATUS")
    print("=" * 60)
    
    run_command("docker-compose ps", "Service status")

def show_logs(service=None, follow=False):
    """Show logs for AIS services"""
    print("=" * 60)
    print("AIS SERVICES LOGS")
    print("=" * 60)
    
    if service:
        command = f"docker-compose logs {'--follow' if follow else ''} {service}"
        description = f"Showing logs for {service}"
    else:
        command = f"docker-compose logs {'--follow' if follow else ''}"
        description = "Showing logs for all services"
    
    run_command(command, description)

def clean_services():
    """Clean up Docker containers, images and volumes"""
    print("=" * 60)
    print("CLEANING AIS SERVICES")
    print("=" * 60)
    
    commands = [
        ("docker-compose down -v", "Stopping services and removing volumes"),
        ("docker-compose down --rmi all", "Removing Docker images"),
        ("docker system prune -f", "Cleaning up Docker system"),
    ]
    
    for command, description in commands:
        run_command(command, description)
    
    print("\n" + "=" * 60)
    print("AIS SERVICES CLEANED SUCCESSFULLY")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(description="Manage AIS Services")
    parser.add_argument("action", choices=[
        "start", "stop", "restart", "status", "logs", "clean"
    ], help="Action to perform")
    
    parser.add_argument("--service", choices=[
        "ais_collector", "lw_correction_service", "cloud_sql_proxy"
    ], help="Specific service to target (for logs command)")
    
    parser.add_argument("--follow", "-f", action="store_true", 
                       help="Follow logs in real-time")
    
    args = parser.parse_args()
    
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Check if docker-compose.yml exists
    if not os.path.exists("docker-compose.yml"):
        print("Error: docker-compose.yml not found in current directory")
        sys.exit(1)
    
    # Execute the requested action
    if args.action == "start":
        success = start_services()
    elif args.action == "stop":
        success = stop_services()
    elif args.action == "restart":
        restart_services()
        success = True
    elif args.action == "status":
        show_status()
        success = True
    elif args.action == "logs":
        show_logs(args.service, args.follow)
        success = True
    elif args.action == "clean":
        success = clean_services()
    else:
        parser.print_help()
        sys.exit(1)
    
    if not success and args.action in ["start", "stop", "clean"]:
        sys.exit(1)

if __name__ == "__main__":
    main() 