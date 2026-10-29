"""AI-generated reference implementation; see the development authorship record."""
import argparse
import json
import sqlite3
import sys
import time


class Reject(Exception):
    pass


def identifier(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 100:
        raise Reject("invalid")
    return value


def integer(value, low=0, high=1_000_000_000):
    if type(value) is not int or not low <= value <= high:
        raise Reject("invalid")
    return value


def encode(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError):
        raise Reject("invalid")


def delay(attempts):
    return min(60, 2 ** (attempts - 1))


class Queue:
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=12, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        # WAL setup itself can contend while several processes create a new DB.
        deadline = time.monotonic() + 12
        while True:
            try:
                self.db.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, id TEXT NOT NULL,
                spec TEXT NOT NULL, payload TEXT NOT NULL, priority INTEGER NOT NULL,
                run_at INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                depends_on TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0, token INTEGER NOT NULL DEFAULT 0,
                worker TEXT, lease_until INTEGER, UNIQUE(tenant,id))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS dependencies (
                tenant TEXT NOT NULL, job TEXT NOT NULL, parent TEXT NOT NULL,
                PRIMARY KEY(tenant,job,parent))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, id TEXT NOT NULL,
                kind TEXT NOT NULL, attempt INTEGER NOT NULL, token INTEGER NOT NULL)""")
            self.db.execute("CREATE INDEX IF NOT EXISTS sched ON jobs(tenant,state,run_at)")
            self.db.execute("CREATE INDEX IF NOT EXISTS history ON events(tenant,seq)")
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def row(self, tenant, job_id):
        row = self.db.execute("SELECT * FROM jobs WHERE tenant=? AND id=?", (tenant, job_id)).fetchone()
        if row is None:
            raise Reject("not_found")
        return row

    @staticmethod
    def view(row):
        keys = ("id", "tenant", "priority", "run_at", "max_attempts", "state",
                "attempts", "token", "worker", "lease_until")
        value = {key: row[key] for key in keys}
        value["payload"] = json.loads(row["payload"])
        value["depends_on"] = json.loads(row["depends_on"])
        return value

    def event(self, tenant, job_id, kind):
        job = self.row(tenant, job_id)
        self.db.execute("INSERT INTO events(tenant,id,kind,attempt,token) VALUES(?,?,?,?,?)",
                        (tenant, job_id, kind, job["attempts"], job["token"]))

    def submit(self, tenant, request):
        jobs = request["jobs"]
        if not isinstance(jobs, list) or not 1 <= len(jobs) <= 1000:
            raise Reject("invalid")
        normalized = []
        seen = set()
        for item in jobs:
            if not isinstance(item, dict):
                raise Reject("invalid")
            job_id = identifier(item["id"])
            if job_id in seen:
                raise Reject("invalid")
            seen.add(job_id)
            deps = item.get("depends_on", [])
            if not isinstance(deps, list):
                raise Reject("invalid")
            deps = [identifier(d) for d in deps]
            if len(set(deps)) != len(deps) or job_id in deps:
                raise Reject("invalid")
            normalized.append(dict(
                id=job_id, payload=item["payload"],
                priority=integer(item.get("priority", 0), -100, 100),
                run_at=integer(item.get("run_at", 0)),
                max_attempts=integer(item.get("max_attempts", 3), 1, 10),
                depends_on=sorted(deps),
            ))
        existing = {row["id"]: row for row in self.db.execute("SELECT * FROM jobs WHERE tenant=?", (tenant,))}
        graph = {key: json.loads(row["depends_on"]) for key, row in existing.items()}
        for item in normalized:
            key = item["id"]
            if key in existing and existing[key]["spec"] != encode(item):
                raise Reject("conflict")
            graph[key] = item["depends_on"]
        for deps in graph.values():
            if any(dep not in graph for dep in deps):
                raise Reject("invalid")
        # Kahn's algorithm avoids a recursion-depth limit on long dependency chains.
        degree = {key: len(deps) for key, deps in graph.items()}
        children = {key: [] for key in graph}
        for key, deps in graph.items():
            for dep in deps:
                children[dep].append(key)
        ready = [key for key, count in degree.items() if count == 0]
        processed = 0
        while ready:
            key = ready.pop()
            processed += 1
            for child in children[key]:
                degree[child] -= 1
                if degree[child] == 0:
                    ready.append(child)
        if processed != len(graph):
            raise Reject("invalid")
        for item in normalized:
            if item["id"] in existing:
                continue
            self.db.execute("""INSERT INTO jobs
                (tenant,id,spec,payload,priority,run_at,max_attempts,depends_on)
                VALUES(?,?,?,?,?,?,?,?)""", (
                    tenant, item["id"], encode(item), encode(item["payload"]),
                    item["priority"], item["run_at"], item["max_attempts"], encode(item["depends_on"])))
            self.db.executemany("INSERT INTO dependencies VALUES(?,?,?)",
                                [(tenant, item["id"], dep) for dep in item["depends_on"]])
            self.event(tenant, item["id"], "submitted")
        return [self.view(self.row(tenant, item["id"])) for item in normalized]

    def claim(self, tenant, request):
        worker = identifier(request["worker"])
        now = integer(request["now"])
        lease = integer(request["lease_sec"], 1, 3600)
        expired = self.db.execute("""SELECT * FROM jobs WHERE tenant=? AND state='leased'
            AND lease_until<=? ORDER BY seq""", (tenant, now)).fetchall()
        for job in expired:
            exhausted = job["attempts"] >= job["max_attempts"]
            self.db.execute("""UPDATE jobs SET state=?,run_at=?,worker=NULL,lease_until=NULL
                WHERE seq=?""", ("dead" if exhausted else "queued",
                                    job["run_at"] if exhausted else job["lease_until"] + delay(job["attempts"]),
                                    job["seq"]))
            self.event(tenant, job["id"], "expired")
        job = self.db.execute("""SELECT j.* FROM jobs j
            WHERE j.tenant=? AND j.state='queued' AND j.run_at<=?
            AND NOT EXISTS (SELECT 1 FROM dependencies d JOIN jobs p
                ON p.tenant=d.tenant AND p.id=d.parent
                WHERE d.tenant=j.tenant AND d.job=j.id AND p.state!='done')
            ORDER BY j.priority DESC,j.run_at,j.seq LIMIT 1""", (tenant, now)).fetchone()
        if job is None:
            return None
        self.db.execute("""UPDATE jobs SET state='leased',attempts=attempts+1,token=token+1,
            worker=?,lease_until=? WHERE seq=?""", (worker, now + lease, job["seq"]))
        self.event(tenant, job["id"], "claimed")
        return self.view(self.row(tenant, job["id"]))

    def mutate_lease(self, tenant, request):
        job_id = identifier(request["id"])
        worker = identifier(request["worker"])
        token = integer(request["token"], 1)
        now = integer(request["now"])
        op = request["op"]
        lease = integer(request["lease_sec"], 1, 3600) if op == "extend" else None
        job = self.row(tenant, job_id)
        if (job["state"] != "leased" or job["worker"] != worker or job["token"] != token
                or job["lease_until"] <= now):
            raise Reject("stale")
        if op == "extend":
            self.db.execute("UPDATE jobs SET lease_until=? WHERE seq=?",
                            (max(job["lease_until"], now + lease), job["seq"]))
            kind = "extended"
        else:
            state = "done" if op == "ack" else ("dead" if job["attempts"] >= job["max_attempts"] else "queued")
            run_at = now + delay(job["attempts"]) if state == "queued" else job["run_at"]
            self.db.execute("UPDATE jobs SET state=?,run_at=?,worker=NULL,lease_until=NULL WHERE seq=?",
                            (state, run_at, job["seq"]))
            kind = "completed" if op == "ack" else "failed"
        self.event(tenant, job_id, kind)
        return self.view(self.row(tenant, job_id))

    def handle(self, request):
        if not isinstance(request, dict):
            raise Reject("invalid")
        tenant = identifier(request["tenant"])
        op = request["op"]
        if not isinstance(op, str):
            raise Reject("invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if op == "submit":
                result = self.submit(tenant, request)
            elif op == "claim":
                result = self.claim(tenant, request)
            elif op in ("ack", "nack", "extend"):
                result = self.mutate_lease(tenant, request)
            elif op in ("get", "cancel"):
                job_id = identifier(request["id"])
                job = self.row(tenant, job_id)
                if op == "cancel":
                    if job["state"] in ("done", "dead"):
                        raise Reject("conflict")
                    if job["state"] != "cancelled":
                        self.db.execute("UPDATE jobs SET state='cancelled',worker=NULL,lease_until=NULL WHERE seq=?",
                                        (job["seq"],))
                        self.event(tenant, job_id, "cancelled")
                result = self.view(self.row(tenant, job_id))
            elif op == "events":
                after = integer(request.get("after", 0), 0, 2**63 - 1)
                limit = integer(request.get("limit", 100), 1, 1000)
                result = [dict(row) for row in self.db.execute(
                    "SELECT * FROM events WHERE tenant=? AND seq>? ORDER BY seq LIMIT ?", (tenant, after, limit))]
            else:
                raise Reject("invalid")
            self.db.execute("COMMIT")
            return result
        except BaseException:
            self.db.execute("ROLLBACK")
            raise


def reject_constant(value):
    raise ValueError(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    queue = Queue(args.db)
    for line in sys.stdin:
        try:
            request = json.loads(line, parse_constant=reject_constant)
            result = queue.handle(request)
            response = {"ok": True, "result": result}
        except Reject as exc:
            response = {"ok": False, "error": str(exc)}
        except (KeyError, TypeError, ValueError, OverflowError):
            response = {"ok": False, "error": "invalid"}
        print(json.dumps(response, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
