"""Trusted black-box verifier. Never import candidate code into this process."""
import concurrent.futures
import json
import os
import pathlib
import random
import select
import shutil
import signal
import subprocess
import tempfile
import time
import unittest


ENTRY = "/app/queue.py"
JOB_KEYS = {"id", "tenant", "payload", "priority", "run_at", "max_attempts", "depends_on",
            "state", "attempts", "token", "worker", "lease_until"}
EVENT_KEYS = {"seq", "tenant", "id", "kind", "attempt", "token"}
METRICS = {}


class Client:
    def __init__(self, db):
        self.closed = False
        self.p = subprocess.Popen(
            ["/usr/local/bin/python3", "-I", "-u", ENTRY, "--db", str(db)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            cwd="/app", env={"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": "/home/agent",
                             "PYTHONDONTWRITEBYTECODE": "1"},
            user=1000, group=1000, extra_groups=[], start_new_session=True,
        )
        self.buffer = b""

    def send(self, value):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.p.stdin.write(data + b"\n")
        self.p.stdin.flush()

    def read(self):
        deadline = time.monotonic() + 15
        while b"\n" not in self.buffer:
            left = deadline - time.monotonic()
            if left <= 0 or not select.select([self.p.stdout], [], [], left)[0]:
                raise AssertionError("request exceeded 15 seconds")
            part = os.read(self.p.stdout.fileno(), 65536)
            if not part:
                raise AssertionError("candidate exited without a response")
            self.buffer += part
            if len(self.buffer) > 8_000_000:
                raise AssertionError("response exceeds 8 MB")
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line)

    def raw(self, value):
        self.send(value)
        return self.read()

    def call(self, op, tenant="alpha", **kw):
        value = self.raw(dict(op=op, tenant=tenant, **kw))
        assert value.get("ok") is True, value
        assert set(value) == {"ok", "result"}, value
        return value["result"]

    def error(self, code, op, tenant="alpha", **kw):
        value = self.raw(dict(op=op, tenant=tenant, **kw))
        assert value == {"ok": False, "error": code}, value

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            os.killpg(self.p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.p.wait(timeout=5)
        self.p.stdin.close()
        self.p.stdout.close()


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.directory = pathlib.Path(tempfile.mkdtemp(prefix="queue-eval-"))
        os.chown(self.directory, 1000, 1000)
        self.db = self.directory / "queue.db"
        self.clients = []
        self.c = self.client()

    def client(self, db=None):
        value = Client(db or self.db)
        self.clients.append(value)
        return value

    def tearDown(self):
        for client in self.clients:
            client.close()
        shutil.rmtree(self.directory)

    def submit(self, *jobs, tenant="alpha", client=None):
        return (client or self.c).call("submit", tenant=tenant, jobs=list(jobs))

    def claim(self, now=0, worker="w", lease_sec=10, client=None, tenant="alpha"):
        return (client or self.c).call("claim", tenant=tenant, worker=worker, now=now, lease_sec=lease_sec)

    def finish(self, job, op="ack", now=1, client=None, **kw):
        return (client or self.c).call(op, tenant=job["tenant"], id=job["id"],
                                     worker=job["worker"], token=job["token"], now=now, **kw)

    def history(self, tenant="alpha"):
        values = []
        while True:
            page = self.c.call("events", tenant=tenant, after=values[-1]["seq"] if values else 0, limit=1000)
            values.extend(page)
            if len(page) < 1000:
                return values

    def test_01_contract_and_payloads(self):
        payloads = [None, False, 0, 1.0, "雪", [], {"b": [1, None], "a": True}]
        rows = self.submit(*[dict(id=str(i), payload=p) for i, p in enumerate(payloads)])
        for i, row in enumerate(rows):
            self.assertEqual(set(row), JOB_KEYS)
            self.assertEqual(row, dict(id=str(i), tenant="alpha", payload=payloads[i], priority=0,
                run_at=0, max_attempts=3, depends_on=[], state="queued", attempts=0,
                token=0, worker=None, lease_until=None))
        for event in self.history():
            self.assertEqual(set(event), EVENT_KEYS)
            self.assertEqual(event["kind"], "submitted")
            self.assertEqual((event["attempt"], event["token"]), (0, 0))

    def test_02_validation_is_atomic(self):
        self.submit(dict(id="existing", payload=1))
        before = self.history()
        invalid_batches = [[], [dict(id="missing-payload")], [dict(id="", payload=0)],
            [dict(id="x", payload=1, priority=True)], [dict(id="x", payload=1, run_at=-1)],
            [dict(id="x", payload=1, max_attempts=0)], [dict(id="x", payload=1, priority=101)],
            [dict(id="x", payload=1, depends_on=["no-such-job"])],
            [dict(id="x", payload=1), dict(id="x", payload=1)],
            [dict(id="x", payload=1, depends_on=["existing", "existing"])],
            [dict(id="x", payload=1, depends_on=["x"])],
            [dict(id="x", payload=1, depends_on=["y"]), dict(id="y", payload=1, depends_on=["x"])],
            [dict(id="new", payload=0), dict(id="bad", payload=0, max_attempts=11)],
            [dict(id="x", payload=1, depends_on="existing")],
            [dict(id="x" * 101, payload=1)]]
        for jobs in invalid_batches:
            self.c.error("invalid", "submit", jobs=jobs)
            self.assertEqual(self.history(), before)
        self.c.error("conflict", "submit", jobs=[dict(id="new", payload=0), dict(id="existing", payload=2)])
        self.c.error("not_found", "get", id="new")
        self.c.error("invalid", "claim", now=0, worker="w", lease_sec=0)
        self.c.error("invalid", "claim", now=True, worker="w", lease_sec=1)
        self.c.error("invalid", "events", after=-1)
        self.c.error("invalid", "events", limit=1001)
        self.c.error("invalid", "events", tenant="")
        self.c.error("invalid", "unknown")
        self.assertEqual(self.c.raw(b"not json"), {"ok": False, "error": "invalid"})
        self.assertEqual(self.c.raw([]), {"ok": False, "error": "invalid"})
        self.assertEqual(self.history(), before)

    def test_03_idempotency_after_state_changes(self):
        original = dict(id="job", payload={"a": 1, "b": [False]}, run_at=2)
        self.submit(original)
        leased = self.claim(now=2)
        failed = self.finish(leased, op="nack", now=3)
        self.assertEqual(failed["run_at"], 4)
        before = self.history()
        repeated = self.submit(dict(id="job", payload={"b": [False], "a": 1}, run_at=2))[0]
        self.assertEqual(repeated, failed)
        self.assertEqual(self.history(), before)
        self.c.error("conflict", "submit", jobs=[dict(original, run_at=4)])
        self.finish(self.claim(now=4), now=5)
        self.assertEqual(self.submit(original)[0]["state"], "done")
        self.submit(dict(id="integer", payload=1))
        self.c.error("conflict", "submit", jobs=[dict(id="integer", payload=True)])
        self.c.error("conflict", "submit", jobs=[dict(id="integer", payload=1.0)])

    def test_04_order_dependencies_and_forward_references(self):
        self.submit(dict(id="child", payload=0, priority=100, depends_on=["b", "a"]),
                    dict(id="a", payload=0, priority=2), dict(id="b", payload=0, priority=2),
                    dict(id="late", payload=0, priority=99, run_at=20),
                    dict(id="low", payload=0, priority=-1))
        self.assertEqual(self.submit(dict(id="child", payload=0, priority=100, depends_on=["a", "b"]))[0]["depends_on"], ["a", "b"])
        a = self.claim(); self.assertEqual(a["id"], "a")
        b = self.claim(); self.assertEqual(b["id"], "b")
        self.finish(a)
        low = self.claim(now=1); self.assertEqual(low["id"], "low")
        self.finish(b)
        child = self.claim(now=1); self.assertEqual(child["id"], "child")
        self.assertIsNone(self.claim(now=2))
        self.finish(low, now=2); self.finish(child, now=2)
        self.assertEqual(self.claim(now=20)["id"], "late")

    def test_05_lease_boundary_fencing_and_extension(self):
        self.submit(dict(id="job", payload=0))
        old = self.claim()
        for op in ("ack", "nack", "extend"):
            fields = dict(id="job", worker="w", token=1, now=10)
            if op == "extend": fields["lease_sec"] = 1
            self.c.error("stale", op, **fields)
        self.assertEqual(self.c.call("get", id="job")["state"], "leased")
        self.assertIsNone(self.claim(now=10))
        current = self.claim(now=11, worker="new")
        self.assertEqual((current["token"], current["attempts"]), (2, 2))
        self.c.error("stale", "ack", id="job", worker="w", token=1, now=12)
        self.c.error("stale", "ack", id="job", worker="new", token=1, now=12)
        self.c.error("stale", "ack", id="job", worker="wrong", token=2, now=12)
        extended = self.finish(current, op="extend", now=12, lease_sec=1)
        self.assertEqual(extended["lease_until"], 21)
        extended = self.finish(current, op="extend", now=12, lease_sec=30)
        self.assertEqual(extended["lease_until"], 42)
        self.assertIsNone(self.claim(now=22))
        self.finish(extended, now=41)
        self.c.error("stale", "nack", id="job", worker="new", token=2, now=41)
        self.assertEqual([x["kind"] for x in self.history()],
            ["submitted", "claimed", "expired", "claimed", "extended", "extended", "completed"])

    def test_06_retry_backoff_and_attempt_limit(self):
        self.submit(dict(id="job", payload=0, max_attempts=10))
        now = 0
        for attempt in range(1, 11):
            job = self.claim(now=now)
            self.assertEqual((job["attempts"], job["token"]), (attempt, attempt))
            result = self.finish(job, op="nack", now=now)
            self.assertIsNone(result["worker"])
            if attempt < 10:
                due = now + min(60, 2 ** (attempt - 1))
                self.assertEqual(result["run_at"], due)
                self.assertIsNone(self.claim(now=due-1))
                now = due
            else:
                self.assertEqual(result["state"], "dead")
        self.assertIsNone(self.claim(now=now+100))
        self.c.error("conflict", "cancel", id="job")

    def test_07_expiry_order_and_terminal_dependencies(self):
        self.submit(dict(id="first", payload=0, max_attempts=1),
                    dict(id="second", payload=0, max_attempts=1, priority=1),
                    dict(id="child", payload=0, depends_on=["first"]))
        self.claim(); self.claim()
        self.assertIsNone(self.claim(now=10))
        expired = [e["id"] for e in self.history() if e["kind"] == "expired"]
        self.assertEqual(expired, ["first", "second"])
        self.assertEqual(self.c.call("get", id="first")["state"], "dead")
        self.assertEqual(self.c.call("get", id="child")["state"], "queued")
        self.submit(dict(id="parent2", payload=0), dict(id="child2", payload=0, depends_on=["parent2"]))
        self.c.call("cancel", id="parent2")
        before = self.history()
        self.c.call("cancel", id="parent2")
        self.assertEqual(self.history(), before)
        self.assertIsNone(self.claim(now=100))
        self.c.call("cancel", id="child")
        self.assertEqual(self.c.call("get", id="child")["state"], "cancelled")

    def test_08_tenants_and_pagination(self):
        for tenant in ("alpha", "beta", "alpha", "beta"):
            self.submit(dict(id=str(len(self.history(tenant))), payload=tenant), tenant=tenant)
        a = self.claim(tenant="alpha")
        self.c.error("not_found", "get", tenant="absent", id=a["id"])
        self.c.error("stale", "ack", tenant="beta", id=a["id"], token=a["token"], worker="w", now=1)
        self.submit(dict(id="beta-only", payload=0), tenant="beta")
        self.c.error("invalid", "submit", jobs=[dict(id="bad", payload=0, depends_on=["beta-only"])])
        events = self.history("alpha")
        page = []
        for _ in events:
            page.extend(self.c.call("events", after=page[-1]["seq"] if page else 0, limit=1))
        self.assertEqual(page, events)
        all_events = sorted(events + self.history("beta"), key=lambda e: e["seq"])
        self.assertEqual([e["seq"] for e in all_events], list(range(1, len(all_events)+1)))
        self.assertTrue(all(e["tenant"] == "alpha" for e in events))

    def test_09_concurrent_initialization_and_duplicate_submit(self):
        clients = [self.client(self.directory / "fresh.db") for _ in range(8)]
        def submit(client):
            return client.call("submit", jobs=[dict(id="same", payload={"v": 1})])
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            results = list(pool.map(submit, clients))
        self.assertTrue(all(r == results[0] for r in results))
        self.assertEqual(len(clients[0].call("events")), 1)

    def test_10_concurrent_claims_and_conflicting_batches(self):
        count = 64
        self.submit(*[dict(id=str(i), payload=i) for i in range(count)])
        clients = [self.client() for _ in range(8)]
        def drain(pair):
            index, client = pair
            ids = []
            while True:
                job = self.claim(worker=str(index), lease_sec=3600, client=client)
                if job is None: return ids
                ids.append(job["id"])
                self.finish(job, now=1, client=client)
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            claimed = sum(pool.map(drain, enumerate(clients)), [])
        self.assertEqual(len(claimed), count)
        self.assertEqual(len(set(claimed)), count)
        self.assertEqual(len(self.history()), count * 3)
        def conflicting(pair):
            index, client = pair
            return client.raw(dict(op="submit", tenant="beta", jobs=[
                dict(id="unique-"+str(index), payload=0), dict(id="race", payload=index)]))
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            results = list(pool.map(conflicting, enumerate(clients)))
        self.assertEqual(sum(r["ok"] is True for r in results), 1)
        self.assertTrue(all(r["ok"] or r["error"] == "conflict" for r in results))
        self.assertEqual(len(self.history("beta")), 2)

    def test_11_acknowledged_state_survives_sigkill(self):
        self.submit(dict(id="done", payload={"data": [1, 2]}), dict(id="leased", payload=1))
        self.finish(self.claim())
        leased = self.claim()
        expected = self.history()
        self.c.close()
        self.c = self.client()
        self.assertEqual(self.history(), expected)
        self.assertEqual(self.c.call("get", id="done")["state"], "done")
        self.assertEqual(self.c.call("get", id="leased"), leased)
        self.assertEqual(self.claim(now=11)["token"], 2)

    def test_12_interrupted_batch_is_all_or_nothing(self):
        METRICS["interrupted_batch_committed_rows"] = []
        for round_, pause in enumerate((0, .002, .006, .012, .025, .05)):
            db = self.directory / ("crash-%d.db" % round_)
            client = self.client(db)
            client.call("events")  # Ensure schema initialization finished.
            jobs = [dict(id=str(i), payload="x" * 200) for i in range(1000)]
            client.send(dict(op="submit", tenant="alpha", jobs=jobs))
            time.sleep(pause)
            client.close()
            recovered = self.client(db)
            events = recovered.call("events", limit=1000)
            METRICS["interrupted_batch_committed_rows"].append(len(events))
            self.assertIn(len(events), (0, 1000))
            # Replaying the interrupted request must complete it without duplicates.
            result = recovered.call("submit", jobs=jobs)
            self.assertEqual(len(result), 1000)
            self.assertEqual(len(recovered.call("events", limit=1000)), 1000)
            self.assertEqual(len({e["seq"] for e in recovered.call("events", limit=1000)}), 1000)

    def test_13_generated_dependency_schedules(self):
        rng = random.Random(829173)
        jobs = []
        for i in range(120):
            parents = rng.sample(range(i), min(i, rng.randrange(4)))
            jobs.append(dict(id="j%03d" % i, payload={"n": i}, priority=rng.randrange(-3, 4),
                             run_at=rng.randrange(12), depends_on=["j%03d" % p for p in parents]))
        rng.shuffle(jobs)
        self.submit(*jobs)
        finished = set()
        now = 0
        for _ in range(150):
            candidates = [(i, job) for i, job in enumerate(jobs) if job["id"] not in finished
                and job["run_at"] <= now and set(job["depends_on"]) <= finished]
            job = self.claim(now=now)
            if not candidates:
                self.assertIsNone(job)
                now += 1
                continue
            _, expected = min(candidates, key=lambda pair: (-pair[1]["priority"], pair[1]["run_at"], pair[0]))
            self.assertEqual(job["id"], expected["id"])
            self.finish(job, now=now)
            finished.add(job["id"])
            if len(finished) == len(jobs): break
        self.assertEqual(len(finished), len(jobs))
        self.assertEqual(len(self.history()), 360)

    def test_14_cancel_fences_active_worker_and_missing_ids(self):
        self.submit(dict(id="job", payload=0))
        leased = self.claim()
        cancelled = self.c.call("cancel", id="job")
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertIsNone(cancelled["worker"])
        self.assertIsNone(cancelled["lease_until"])
        self.c.error("stale", "ack", id="job", worker=leased["worker"], token=leased["token"], now=1)
        self.assertIsNone(self.claim(now=100))
        for op in ("get", "cancel", "ack", "nack", "extend"):
            self.c.error("not_found", op, id="missing", worker="w", token=1, now=0, lease_sec=1)
        self.submit(dict(id="done", payload=0))
        self.finish(self.claim())
        self.c.error("conflict", "cancel", id="done")

    def test_15_throughput_and_long_dependency_chain(self):
        start = time.monotonic()
        jobs = [dict(id=str(i), payload=i, depends_on=[str(i-1)] if i else []) for i in range(1000)]
        self.submit(*reversed(jobs))
        for i in range(1000):
            job = self.claim()
            self.assertEqual(job["id"], str(i))
            self.finish(job)
        elapsed = time.monotonic() - start
        METRICS["thousand_job_chain_seconds"] = round(elapsed, 4)
        self.assertLess(elapsed, 30)
        self.assertEqual(len(self.history()), 3000)

    def test_16_generated_retry_history_and_restart(self):
        rng = random.Random(716331)
        expected = []
        for index in range(20):
            job_id = "history-" + str(index)
            self.submit(dict(id=job_id, payload=index, max_attempts=5))
            expected.append((job_id, "submitted", 0, 0))
            now = 100 * index
            for attempt in range(1, 6):
                job = self.claim(now=now, worker="owner", lease_sec=4)
                self.assertEqual(job["id"], job_id)
                expected.append((job_id, "claimed", attempt, attempt))
                if attempt == 5:
                    if index % 2:
                        self.finish(job, now=now+1)
                        expected.append((job_id, "completed", attempt, attempt))
                    else:
                        self.finish(job, op="nack", now=now+1)
                        expected.append((job_id, "failed", attempt, attempt))
                    break
                if rng.choice([True, False]):
                    self.finish(job, op="nack", now=now+1)
                    expected.append((job_id, "failed", attempt, attempt))
                    now += 1 + 2**(attempt-1)
                else:
                    self.assertIsNone(self.claim(now=now+4))
                    expected.append((job_id, "expired", attempt, attempt))
                    now += 4 + 2**(attempt-1)
                if attempt == 3:
                    self.c.close()
                    self.c = self.client()
            actual = self.history()
            self.assertEqual([(e["id"], e["kind"], e["attempt"], e["token"]) for e in actual], expected)
            self.assertEqual([e["seq"] for e in actual], list(range(1, len(expected)+1)))


if __name__ == "__main__":
    assert os.geteuid() == 0, "Verifier must start as root, candidates run as uid 1000"
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(QueueTests)
    outcome = unittest.TextTestRunner(verbosity=2).run(suite)
    report = dict(tests=outcome.testsRun, failures=len(outcome.failures), errors=len(outcome.errors),
                  passed=outcome.wasSuccessful(), metrics=METRICS)
    pathlib.Path("/logs/verifier/summary.json").write_text(json.dumps(report, indent=2) + "\n")
    raise SystemExit(0 if outcome.wasSuccessful() else 1)
