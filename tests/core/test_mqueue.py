"""
mqueue message-id uniqueness (RES-4).

The message id is the primary key of ``message_queue.messages`` and the
correlator used to ack a delivery. It must be unique per delivery: previously it
was ``md5(channel + consumer + message + send_time)``, so two identical messages
to the same consumer within one ``current_time()`` tick collided on the PK and
failed the whole send transaction.

These tests exercise the id generator directly and then the real PK/ack path in
the database (the full broker/consumer HTTP round-trip has no test harness, so
the delivery itself is not simulated here).
"""
from __future__ import annotations

from datetime import datetime as dt

from sqlalchemy import text

from orcha.utils.mqueue import Broker

from helpers import get_engine

# Identical inputs: same channel, consumer, message and send tick.
_ARGS = ("run_failed", "consumer_a", "the same message", dt(2026, 1, 1, 0, 0, 0))


def test_generate_message_id_is_unique_for_identical_input():
    ids = {Broker._generate_message_id(*_ARGS) for _ in range(1000)}
    assert len(ids) == 1000
    # Still a stable 32-char hex string PK.
    assert all(len(i) == 32 and int(i, 16) >= 0 for i in ids)


def _insert_message(conn, message_id: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO message_queue.messages
                (id, created_at, channel, consumer_name, message, acked, send_status)
            VALUES
                (:id, :created_at, :channel, :consumer, :message, 'false', 'pending')
            """
        ),
        {
            "id": message_id,
            "created_at": _ARGS[3],
            "channel": _ARGS[0],
            "consumer": _ARGS[1],
            "message": _ARGS[2],
        },
    )


def test_identical_messages_get_distinct_rows_and_ack_resolves_one():
    id1 = Broker._generate_message_id(*_ARGS)
    id2 = Broker._generate_message_id(*_ARGS)
    assert id1 != id2

    # Two identical messages must both persist -- no PK collision (the bug).
    with get_engine().begin() as conn:
        _insert_message(conn, id1)
        _insert_message(conn, id2)

    with get_engine().begin() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM message_queue.messages")
        ).scalar()
    assert count == 2

    # Acking by id must resolve exactly the one row (correct correlation).
    with get_engine().begin() as conn:
        conn.execute(
            text("UPDATE message_queue.messages SET acked = 'true' WHERE id = :id"),
            {"id": id1},
        )
        acked = (
            conn.execute(
                text("SELECT id FROM message_queue.messages WHERE acked = 'true'")
            )
            .scalars()
            .all()
        )
    assert acked == [id1]
