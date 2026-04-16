"""
Recovery services for handling failed operations and restoring state.
This module includes functions for:
- Retrying failed operations with exponential backoff
- Compensating for failed operations by performing inverse actions
- Querying the log of events to determine what actions have been taken
- Providing APIs for manual intervention if needed
"""

import asyncio
import logging
import time
from collections import defaultdict
import psycopg
import app 


async def retry_with_backoff(
    cur,
    cursor: psycopg.AsyncCursor,
):
    """
    Keeps polling the db to search for timed out operations and retries them.
    """
    while True:
        with await cursor.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, order_id, status, step, results, version
                    FROM received_events WHERE status = 'RUNNING' AND step = 'PAYMENT' AND (NOW() - updated_at) > INTERVAL '30 seconds'
                    """
                )
                rows = await cur.fetchall()
                for row in rows:
                    event = app.dispatch_event(
                        
                    )
                    logging.info(f"Retrying payment for saga {row['id']} and order {row['order_id']}")
                    # If retry fails again, we can update the saga status to FAILED or COMPENSATING as needed
        await asyncio.sleep(5)  # Sleep for a while before checking again
    
