import logging
import sys
from datetime import datetime, timezone
DISABLE_API_LOG = True

# ------------------------
# Global Configuration
# ------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# ------------------------
# Logging and Utility
# ------------------------

async def log_api_request(message: str):
    """Asynchronously logs API requests if enabled."""
    if DISABLE_API_LOG:
        return
    try:
        # This is a simplified async file write. For high performance,
        # a dedicated logging library like 'aiologger' would be better.
        with open("api_requests.log", "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} - {message}\n")
    except IOError as e:
        logging.error(f"Failed to write to api_requests.log: {e}")
