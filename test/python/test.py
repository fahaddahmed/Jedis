import subprocess
from typing import List
import time

def run_redis_test(command: List[str]) -> None:
    try:
        new_list = ['redis-cli'] + command
        result = subprocess.run(new_list, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Command executed successfully.")
            print("Output:", result.stdout.strip())
        else:
            print("Error occurred:")
            print(result.stderr)
    except FileNotFoundError:
        print("redis-cli is not installed or not found in the system PATH.")
    except Exception as e:
        print(f"An error occurred: {e}")


def run_redis_test_with_formatting(commmand: str) -> None:
    formatted_commands = commmand.split()
    print("TEST:")
    print("-" * 50)
    run_redis_test(formatted_commands)
    print("-" * 50)

run_redis_test_with_formatting('PING')

run_redis_test_with_formatting('SET foo bar')
run_redis_test_with_formatting('GET foo')



run_redis_test_with_formatting('SET test123 gonsnoig px 5000')

run_redis_test_with_formatting('GET test123')
time.sleep(6)
run_redis_test_with_formatting('GET test123')
