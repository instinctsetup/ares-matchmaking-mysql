"""Central configuration for the Ares matchmaking backend.

Everything environment-specific lives here. Override any value with the
matching environment variable.
"""

import os

# MySQL connection settings.
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "ares_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ares_password")
MYSQL_DB = os.getenv("MYSQL_DB", "ares_db")
# Optional: connect over a local socket instead of TCP.
MYSQL_SOCKET = os.getenv("MYSQL_SOCKET") or None

# Regions the matchmaking loop sweeps, in priority order.
ACTIVE_REGIONS = ["IN", "US-EAST"]

# Match shape.
TEAM_SIZE = 5                 # players per team
PLAYERS_PER_MATCH = 2 * TEAM_SIZE

# Engine behaviour.
MATCHMAKER_TICK_SECONDS = 3   # how often the loop sweeps the queue
CLEANUP_TICK_SECONDS = 15     # how often stale players are purged
HEARTBEAT_TIMEOUT_SECONDS = 300  # queue entry dies without a heartbeat

# Queue fetch batch size per sweep.
QUEUE_BATCH_SIZE = 100

# A player waiting longer than this pulls the whole region into a GLOBAL
# (cross-region) match attempt instead of a LOCAL one.
GLOBAL_MATCH_WAIT_SECONDS = 60

# Elo matching tolerance: starts at BASE and relaxes by STEP every
# RELAX_EVERY_SECONDS the oldest player has been waiting.
ELO_THRESHOLD_BASE = 50
ELO_THRESHOLD_STEP = 50
ELO_THRESHOLD_RELAX_EVERY_SECONDS = 15

# Elo stakes applied when a match is resolved.
ELO_WIN_DELTA = 25
ELO_LOSS_DELTA = -20

# Fallback map when the maps table is empty.
DEFAULT_MAP = "Training Grounds"
