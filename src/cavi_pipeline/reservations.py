"""The runs a store knows and the resources their nodes hold, shared by every process using it."""

import sqlite3
import time
from contextlib import contextmanager
from pathlib import PurePosixPath

# Output paths are held under this prefix; resource names are identifiers and never contain it.
OUTPUT = "output:"

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run TEXT PRIMARY KEY,
    graph TEXT NOT NULL,
    run_dir TEXT NOT NULL,
    state TEXT NOT NULL,
    created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    resource TEXT PRIMARY KEY,
    run TEXT NOT NULL,
    node TEXT NOT NULL,
    at REAL NOT NULL
);
"""


class Registry:
    """Runs and reservations in the store's SQLite file."""

    def __init__(self, path):
        self.path = path
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.executescript(SCHEMA)
        finally:
            db.close()

    @contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("COMMIT")
        finally:
            db.close()

    def add_run(self, run, graph, run_dir):
        """Register a run; one live run per run directory."""
        with self._transaction() as db:
            live = db.execute("SELECT run FROM runs WHERE run_dir = ? AND state = 'live'",
                              (run_dir,)).fetchone()
            if live:
                raise ValueError(f"Run {live[0]} is already live in {run_dir}; cancel it first")
            db.execute("INSERT INTO runs VALUES (?, ?, ?, 'live', ?)",
                       (run, graph, run_dir, time.time()))

    def run(self, run) -> dict:
        with self._transaction() as db:
            row = db.execute("SELECT run, graph, run_dir, state, created FROM runs WHERE run = ?",
                             (run,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown run {run}")
        return dict(zip(("run", "graph", "run_dir", "state", "created"), row, strict=True))

    def live(self, run_dir) -> dict | None:
        """The live run in ``run_dir``, or None."""
        with self._transaction() as db:
            row = db.execute("SELECT run, graph, run_dir, state, created FROM runs "
                             "WHERE run_dir = ? AND state = 'live'", (run_dir,)).fetchone()
        return None if row is None else dict(
            zip(("run", "graph", "run_dir", "state", "created"), row, strict=True))

    def runs(self) -> list[dict]:
        with self._transaction() as db:
            rows = db.execute("SELECT run, graph, run_dir, state, created FROM runs "
                              "ORDER BY created").fetchall()
        return [dict(zip(("run", "graph", "run_dir", "state", "created"), row, strict=True))
                for row in rows]

    def cancel_run(self, run, keep=()):
        """Mark the run cancelled and release everything it holds except the ``keep`` nodes'."""
        with self._transaction() as db:
            db.execute("UPDATE runs SET state = 'cancelled' WHERE run = ?", (run,))
            kept = list(keep)
            db.execute("DELETE FROM reservations WHERE run = ? AND node NOT IN "
                       f"({', '.join('?' * len(kept))})", (run, *kept))

    def holder(self, resource):
        """The (run, node) holding ``resource``, or None."""
        with self._transaction() as db:
            return db.execute("SELECT run, node FROM reservations WHERE resource = ?",
                              (resource,)).fetchone()

    @staticmethod
    def _conflict(db, run, node, resources, outputs):
        for resource in resources:
            row = db.execute("SELECT run, node FROM reservations WHERE resource = ?",
                             (resource,)).fetchone()
            if row is not None and tuple(row) != (run, node):
                return f"{resource} is held by run {row[0]} node {row[1]}"
        if outputs:
            held = db.execute("SELECT resource, run, node FROM reservations "
                              "WHERE resource LIKE ? || '%'", (OUTPUT,)).fetchall()
            for key, other_run, other_node in held:
                if (other_run, other_node) == (run, node):
                    continue
                theirs = PurePosixPath(key.removeprefix(OUTPUT))
                for path in outputs:
                    mine = PurePosixPath(path.casefold())
                    if mine.is_relative_to(theirs) or theirs.is_relative_to(mine):
                        return (f"output {path} overlaps {theirs} held by run {other_run} "
                                f"node {other_node}")
        return None

    def conflict(self, run, node, resources, outputs=()) -> str | None:
        """Why ``node`` of ``run`` cannot hold these resources and output paths now, or None."""
        with self._transaction() as db:
            return self._conflict(db, run, node, resources, outputs)

    def reserve(self, run, node, resources, outputs=()):
        """Hold every resource and output path for ``node`` of ``run``, or none of them.

        A resource the same node already holds stays held; one held by any other node refuses.
        Output paths compare without case, and a path inside or around a held one refuses.
        """
        with self._transaction() as db:
            reason = self._conflict(db, run, node, resources, outputs)
            if reason:
                raise ValueError(reason)
            for resource in [*resources, *(OUTPUT + path.casefold() for path in outputs)]:
                db.execute("INSERT OR IGNORE INTO reservations VALUES (?, ?, ?, ?)",
                           (resource, run, node, time.time()))

    def release(self, run, node):
        with self._transaction() as db:
            db.execute("DELETE FROM reservations WHERE run = ? AND node = ?", (run, node))

    def held(self, run) -> dict[str, list[str]]:
        """The resources each node of ``run`` holds."""
        with self._transaction() as db:
            rows = db.execute("SELECT node, resource FROM reservations WHERE run = ? "
                              "ORDER BY resource", (run,)).fetchall()
        found = {}
        for node, resource in rows:
            found.setdefault(node, []).append(resource)
        return found
