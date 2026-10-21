"""MySQL data-access layer (aiomysql).

Every SQL statement in the project lives in this file. The engine and the
HTTP routes call these functions and never build SQL themselves.

Row shape note: queue-candidate rows are plain dicts with at least
player_id, party_id, username, total_elo, region, latency, entered_at.
UUIDs are stored as CHAR(36) strings; timestamps are UTC.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiomysql

from . import config

_pool: aiomysql.Pool | None = None


async def connect() -> None:
    """Create the global connection pool."""
    global _pool
    kwargs = dict(
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB,
        autocommit=True,
        cursorclass=aiomysql.DictCursor,
        init_command="SET time_zone = '+00:00'",
    )
    if config.MYSQL_SOCKET:
        kwargs["unix_socket"] = config.MYSQL_SOCKET
    else:
        kwargs.update(host=config.MYSQL_HOST, port=config.MYSQL_PORT)
    _pool = await aiomysql.create_pool(**kwargs)


async def close() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None


def pool() -> aiomysql.Pool:
    assert _pool is not None, "database.connect() has not been called"
    return _pool


def acquire():
    """Expose the pool's acquire() so the engine can own a transaction."""
    return pool().acquire()


@asynccontextmanager
async def transaction(conn):
    """One atomic unit of work on an acquired connection."""
    await conn.begin()
    try:
        yield
    except BaseException:
        # BaseException covers asyncio.CancelledError (a task cancelled
        # mid-transaction must still roll back, else its locks hang schema
        # changes and later connections).
        await conn.rollback()
        raise
    else:
        await conn.commit()


async def _fetchval(cursor, sql, args=()):
    await cursor.execute(sql, args)
    row = await cursor.fetchone()
    return next(iter(row.values())) if row else None


async def _fetchall(sql, args=()) -> list[dict]:
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            return list(await cur.fetchall())


async def _execute(sql, args=()) -> int:
    """Run a single write statement. Returns affected row count."""
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            return await cur.execute(sql, args)


SCHEMA_STATEMENTS = [
    "SET FOREIGN_KEY_CHECKS = 0",
    "DROP TABLE IF EXISTS match_participants",
    "DROP TABLE IF EXISTS matches",
    "DROP TABLE IF EXISTS matchmaking_queue",
    "DROP TABLE IF EXISTS maps",
    "DROP TABLE IF EXISTS players",
    "SET FOREIGN_KEY_CHECKS = 1",
    """
    CREATE TABLE players (
        player_id CHAR(36) PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        total_elo INT DEFAULT 1200,
        penalty_until TIMESTAMP NULL DEFAULT NULL
    )
    """,
    """
    CREATE TABLE matchmaking_queue (
        player_id CHAR(36) PRIMARY KEY,
        party_id CHAR(36) NOT NULL,
        region VARCHAR(20) NOT NULL,
        latency INT NOT NULL,
        entered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """,
    """
    CREATE TABLE maps (
        map_id INT AUTO_INCREMENT PRIMARY KEY,
        map_name VARCHAR(50) UNIQUE NOT NULL,
        is_active BOOLEAN DEFAULT TRUE
    )
    """,
    """
    CREATE TABLE matches (
        match_id CHAR(36) PRIMARY KEY,
        map_name VARCHAR(50),
        region VARCHAR(20),
        avg_elo INT,
        match_type VARCHAR(10),           -- 'LOCAL' or 'GLOBAL'
        winner CHAR(1) DEFAULT NULL,      -- 'A', 'B', or NULL while ongoing
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE match_participants (
        match_id CHAR(36),
        player_id CHAR(36),
        team CHAR(1),
        PRIMARY KEY (match_id, player_id),
        FOREIGN KEY (match_id) REFERENCES matches(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """,
    "CREATE INDEX idx_queue_region ON matchmaking_queue (region, last_seen)",
    "INSERT IGNORE INTO maps (map_name) VALUES ('Bind'), ('Haven'), ('Split'), ('Ascent')",
]


async def init_schema() -> None:
    """Drop and recreate the full schema. Destroys all existing data."""
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            for statement in SCHEMA_STATEMENTS:
                await cur.execute(statement)


# --------------------------------------------------------------------------
# Queue writes
# --------------------------------------------------------------------------

async def join_party(players, party_id: uuid.UUID) -> None:
    """Upsert every player and put the whole party into the queue."""
    async with pool().acquire() as conn:
        async with transaction(conn):
            async with conn.cursor() as cur:
                for player in players:
                    await cur.execute(
                        """
                        INSERT INTO players (player_id, username, total_elo)
                        VALUES (%s, %s, %s) AS new
                        ON DUPLICATE KEY UPDATE total_elo = new.total_elo
                        """,
                        (str(uuid.uuid4()), player.username, player.elo),
                    )
                    # MySQL has no RETURNING; look the id back up by username.
                    player_id = await _fetchval(
                        cur, "SELECT player_id FROM players WHERE username = %s", (player.username,)
                    )
                    await cur.execute(
                        """
                        INSERT INTO matchmaking_queue (player_id, party_id, region, latency)
                        VALUES (%s, %s, %s, %s) AS new
                        ON DUPLICATE KEY UPDATE
                            party_id = new.party_id,
                            last_seen = UTC_TIMESTAMP(),
                            region = new.region,
                            latency = new.latency
                        """,
                        (player_id, str(party_id), player.region, player.latency),
                    )


async def heartbeat(party_id: uuid.UUID) -> None:
    await _execute(
        "UPDATE matchmaking_queue SET last_seen = UTC_TIMESTAMP() WHERE party_id = %s",
        (str(party_id),),
    )


async def purge_stale_players(stale_after_seconds: int) -> int:
    """Delete queue entries whose heartbeat went quiet. Returns rows deleted."""
    return await _execute(
        """
        DELETE FROM matchmaking_queue
        WHERE last_seen < UTC_TIMESTAMP() - INTERVAL %s SECOND
        """,
        (stale_after_seconds,),
    )


# --------------------------------------------------------------------------
# Matchmaking reads and writes (used by the engine loop)
# --------------------------------------------------------------------------

async def oldest_wait_seconds(region: str, conn=None) -> float | None:
    """Seconds the oldest queue entry in a region has waited. None if empty.

    Pass an open connection to run inside the caller's transaction.
    """
    query = """
        SELECT entered_at FROM matchmaking_queue
        WHERE region = %s ORDER BY entered_at ASC LIMIT 1
    """
    if conn is not None:
        async with conn.cursor() as cur:
            entered_at = await _fetchval(cur, query, (region,))
    else:
        rows = await _fetchall(query, (region,))
        entered_at = rows[0]["entered_at"] if rows else None
    if entered_at is None:
        return None
    return (datetime.now(timezone.utc) - entered_at.replace(tzinfo=timezone.utc)).total_seconds()


async def fetch_queue_candidates(conn, region: str | None, limit: int) -> list[dict]:
    """Fetch queue rows joined with player data, oldest first.

    region=None means a GLOBAL sweep across every region. Must be called
    inside the caller's transaction.
    """
    base = """
        SELECT q.*, p.username, p.total_elo
        FROM matchmaking_queue q
        JOIN players p ON q.player_id = p.player_id
    """
    async with conn.cursor() as cur:
        if region is None:
            await cur.execute(base + " ORDER BY q.entered_at ASC LIMIT %s", (limit,))
        else:
            await cur.execute(
                base + " WHERE q.region = %s ORDER BY q.entered_at ASC LIMIT %s",
                (region, limit),
            )
        return [dict(row) for row in await cur.fetchall()]


async def create_match(
    conn,
    match_id: uuid.UUID,
    map_name: str,
    region: str,
    avg_elo: int,
    match_type: str,
    team_a: list[tuple[str, list[dict]]],
    team_b: list[tuple[str, list[dict]]],
) -> None:
    """Record a match, its participants, and clear the queue entries.

    Teams are lists of (party_id, players) pairs. Runs inside the caller's
    transaction so a crash mid-write never leaves a match without its
    participants.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO matches (match_id, map_name, region, avg_elo, match_type)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (str(match_id), map_name, region, avg_elo, match_type),
        )
        for team, side in ((team_a, "A"), (team_b, "B")):
            for _party_id, players in team:
                for player in players:
                    await cur.execute(
                        "INSERT INTO match_participants (match_id, player_id, team) VALUES (%s, %s, %s)",
                        (str(match_id), str(player["player_id"]), side),
                    )
        party_ids = [party_id for party_id, _ in team_a + team_b]
        placeholders = ", ".join(["%s"] * len(party_ids))
        await cur.execute(
            f"DELETE FROM matchmaking_queue WHERE party_id IN ({placeholders})",
            party_ids,
        )


async def random_map() -> str | None:
    rows = await _fetchall(
        "SELECT map_name FROM maps WHERE is_active = TRUE ORDER BY RAND() LIMIT 1"
    )
    return rows[0]["map_name"] if rows else None


# --------------------------------------------------------------------------
# Match resolution
# --------------------------------------------------------------------------

async def get_match_winner(match_id: uuid.UUID):
    """Returns the winner ('A'/'B'/None), or raises KeyError if no such match."""
    rows = await _fetchall("SELECT winner FROM matches WHERE match_id = %s", (str(match_id),))
    if not rows:
        raise KeyError(str(match_id))
    return rows[0]["winner"]


async def resolve_match(match_id: uuid.UUID, winner: str) -> None:
    """Set the winner and apply Elo stakes to every participant."""
    async with pool().acquire() as conn:
        async with transaction(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE matches SET winner = %s WHERE match_id = %s", (winner, str(match_id))
                )
                await cur.execute(
                    "SELECT player_id, team FROM match_participants WHERE match_id = %s",
                    (str(match_id),),
                )
                participants = await cur.fetchall()
                for participant in participants:
                    delta = config.ELO_WIN_DELTA if participant["team"] == winner else config.ELO_LOSS_DELTA
                    await cur.execute(
                        "UPDATE players SET total_elo = total_elo + %s WHERE player_id = %s",
                        (delta, participant["player_id"]),
                    )


# --------------------------------------------------------------------------
# Read-only reporting endpoints
# --------------------------------------------------------------------------

async def queue_snapshot() -> tuple[dict, int]:
    rows = await _fetchall(
        """
        SELECT q.party_id, p.username, p.total_elo, q.latency, q.region, q.entered_at
        FROM matchmaking_queue q
        JOIN players p ON q.player_id = p.player_id
        ORDER BY q.entered_at ASC
        """
    )
    parties: dict[str, list[dict]] = {}
    for row in rows:
        entry = dict(row)
        entry["entered_at"] = entry["entered_at"].isoformat()
        parties.setdefault(entry["party_id"], []).append(entry)
    return parties, len(rows)


async def recent_matches(limit: int = 50) -> list[dict]:
    rows = await _fetchall(
        """
        SELECT match_id, map_name AS map, region, avg_elo, match_type, winner, created_at
        FROM matches ORDER BY created_at DESC LIMIT %s
        """,
        (limit,),
    )
    for match in rows:
        match["created_at"] = match["created_at"].isoformat()
    return rows


async def leaderboard(limit: int = 10) -> list[dict]:
    return await _fetchall(
        "SELECT username, total_elo FROM players ORDER BY total_elo DESC LIMIT %s", (limit,)
    )


async def system_stats() -> dict:
    queue_count = (await _fetchall("SELECT count(*) AS c FROM matchmaking_queue"))[0]["c"]
    matches_today = (await _fetchall(
        "SELECT count(*) AS c FROM matches WHERE created_at > UTC_TIMESTAMP() - INTERVAL 24 HOUR"
    ))[0]["c"]
    matched_players = (await _fetchall(
        "SELECT COUNT(DISTINCT player_id) AS c FROM match_participants"
    ))[0]["c"]
    total_players = (await _fetchall("SELECT COUNT(*) AS c FROM players"))[0]["c"]

    success_rate = round(matched_players / total_players * 100, 1) if total_players else 0
    engine_load = min(int(queue_count), 100)
    return {
        "active_queue": queue_count,
        "matches_today": matches_today,
        "success_rate": f"{success_rate}%",
        "engine_load": f"{engine_load}%",
    }


async def throughput() -> dict:
    by_minute = await _fetchall(
        """
        SELECT DATE_FORMAT(created_at, '%%H:%%i') AS time, count(*) AS matches
        FROM matches
        WHERE created_at > UTC_TIMESTAMP() - INTERVAL 30 MINUTE
        GROUP BY time ORDER BY time ASC
        """
    )
    by_region = await _fetchall(
        "SELECT region AS name, count(*) AS value FROM matches GROUP BY region"
    )
    return {
        "throughput": [dict(row) for row in by_minute],
        "regions": [dict(row) for row in by_region],
    }
