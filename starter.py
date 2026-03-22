"""
starter.py
Purpose: Manage the Docker environment lifecycle separately from the pipeline.

Usage:
    python starter.py start   — starts all Docker containers and waits for readiness
    python starter.py end     — stops all Docker containers cleanly
"""

import sys
import socket
import subprocess
import time
import project_config


def wait_for_port(port, host='localhost', timeout=30):
    """Polls a port until it's open or timeout is reached."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(1)
    return False


def start():
    print("[*] Launching Docker Infrastructure...")
    try:
        subprocess.run(
            ["docker-compose", "-f", project_config.DOCKER_COMPOSE_FILE, "up", "-d"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"[X] Docker Compose failed to start: {e}")
        print("[!] Is Docker Desktop running?")
        sys.exit(1)

    print("[*] Waiting for PostgreSQL (port 5432)...")
    if not wait_for_port(5432):
        print("[X] Timeout: PostgreSQL did not start in time.")
        sys.exit(1)
    print("[+] PostgreSQL is ready.")

    print("[*] Waiting for MongoDB (port 27017)...")
    if not wait_for_port(27017):
        print("[X] Timeout: MongoDB did not start in time.")
        sys.exit(1)
    print("[+] MongoDB is ready.")

    print("[*] Waiting for API (port 8000)...")
    if not wait_for_port(8000):
        print("[X] Timeout: API did not start in time.")
        sys.exit(1)
    print("[+] API is ready.")

    time.sleep(2)  # final safety buffer for internal DB initialization
    print("\n[+] All services are online. You can now run:")
    print("      python main.py initialise 500")
    print("      python main.py fetch 100")
    print("      python main.py query")


def end():
    print("[*] Stopping all Docker containers...")
    subprocess.run(
        ["docker-compose", "-f", project_config.DOCKER_COMPOSE_FILE, "down", "--timeout", "5"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False
    )
    print("[+] All containers stopped.")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("start", "end"):
        print("Usage:")
        print("  python starter.py start   — start Docker environment")
        print("  python starter.py end     — stop Docker environment")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":
        start()
    elif command == "end":
        end()


if __name__ == "__main__":
    main()