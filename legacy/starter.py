"""
starter.py
Purpose: Manage the Docker environment lifecycle separately from the pipeline.

Usage:
    python legacy/starter.py start   - starts all Docker containers and waits for readiness
    python legacy/starter.py end     - stops all Docker containers cleanly
"""

import os
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


def get_compose_command():
    """Return an available docker compose command (v2 preferred, v1 fallback)."""
    try:
        subprocess.run(["docker", "compose", "version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return ["docker", "compose"]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ["docker-compose"]


def start():
    compose_cmd = get_compose_command()

    print("[*] Checking if Docker Desktop is running...")
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[+] Docker Desktop is already running.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("[!] Docker Engine is off. Attempting to launch Docker Desktop...")
        docker_path = r"C:\Program Files\Docker\Docker\Docker Desktop.exe"
        if os.path.exists(docker_path):
            subprocess.Popen([docker_path, "--minimized"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("[*] Launching Docker Desktop. Waiting for engine readiness (up to 2 mins)...")

            ready = False
            for _ in range(60):
                try:
                    subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    ready = True
                    break
                except subprocess.CalledProcessError:
                    time.sleep(2)
            if not ready:
                print("[X] Docker Engine failed to start. Please open it manually.")
                sys.exit(1)
            print("[+] Docker Engine is now Ready.")
            time.sleep(3)
        else:
            print(f"[X] Docker Desktop not found at {docker_path}. Please start it manually.")
            sys.exit(1)
    try:
        subprocess.run(
            [*compose_cmd, "-f", project_config.DOCKER_COMPOSE_FILE, "up", "-d"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"[X] Docker Compose failed to start: {e}")
        print("[!] Is Docker Desktop running?")
        sys.exit(1)

    print(f"[*] Waiting for PostgreSQL (port {project_config.PG_PORT})...")
    if not wait_for_port(project_config.PG_PORT, timeout=project_config.DOCKER_STARTUP_TIMEOUT):
        print("[X] Timeout: PostgreSQL did not start in time.")
        sys.exit(1)
    print("[+] PostgreSQL is ready.")

    print(f"[*] Waiting for MongoDB (port {project_config.MONGO_PORT})...")
    if not wait_for_port(project_config.MONGO_PORT, timeout=project_config.DOCKER_STARTUP_TIMEOUT):
        print("[X] Timeout: MongoDB did not start in time.")
        sys.exit(1)
    print("[+] MongoDB is ready.")

    print(f"[*] Waiting for API (port {project_config.API_PORT})...")
    if not wait_for_port(project_config.API_PORT, timeout=project_config.DOCKER_STARTUP_TIMEOUT):
        print("[X] Timeout: API did not start in time.")
        sys.exit(1)
    print("[+] API is ready.")

    time.sleep(2)
    print("\n[+] All services are online. You can now run:")
    print("      python main.py initialise 500")
    print("      python main.py fetch 100")
    print("      python main.py query")


def end():
    print("[*] Stopping all Docker containers...")
    compose_cmd = get_compose_command()
    subprocess.run(
        [*compose_cmd, "-f", project_config.DOCKER_COMPOSE_FILE, "down", "--timeout", "5"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False
    )
    print("[+] All containers stopped.")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("start", "end"):
        print("Usage:")
        print("  python legacy/starter.py start   - start Docker environment")
        print("  python legacy/starter.py end     - stop Docker environment")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":
        start()
    elif command == "end":
        end()


if __name__ == "__main__":
    main()
