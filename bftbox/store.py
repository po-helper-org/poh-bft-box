"""Хранилище для чтения: итерации и инициативы. Истина — состояние воркфлоу в Temporal; сюда пишет активность
`persist` после каждого изменения, отсюда читают get/status/list и страница /bft, не трогая воркфлоу."""
from __future__ import annotations

import json
import threading

SCHEMA_SQL = """
create table if not exists initiatives (
  initiative_id text primary key, owner text not null, goal text not null, state text not null, team text,
  latest_iteration_id text not null, external_refs jsonb not null default '{}', status jsonb not null,
  package_version int not null default 0, created_at timestamptz not null default now(), updated_at timestamptz not null default now());
create table if not exists iterations (
  iteration_id text primary key, initiative_id text not null references initiatives(initiative_id),
  number int not null, body jsonb not null, updated_at timestamptz not null default now());
create index if not exists iterations_initiative on iterations(initiative_id, number);
create table if not exists start_keys (
  owner text not null, key text not null, body_hash text not null, initiative_id text not null,
  primary key (owner, key));
"""


class MemoryStore:
    """Для тестов и локального прогона без Postgres."""

    def __init__(self):
        self.ini, self.its, self.keys = {}, {}, {}
        self.lock = threading.Lock()

    def save(self, snap: dict):
        with self.lock:
            i = snap["initiative"]
            self.ini[i["initiative_id"]] = i
            for it in snap["iterations"]:
                self.its[it["iteration_id"]] = it

    def iteration(self, iteration_id):
        return self.its.get(iteration_id)

    def initiative(self, initiative_id):
        return self.ini.get(initiative_id)

    def initiatives(self, owner=None):
        return [i for i in self.ini.values() if owner is None or i["owner"] == owner]

    def latest(self, initiative_id):
        ini = self.ini.get(initiative_id)
        return self.its.get(ini["latest_iteration_id"]) if ini else None

    def claim_start(self, owner, key, body_hash, initiative_id):
        """Первый вызов с ключом записывается, повтор получает записанное (хэш тела, id инициативы)."""
        with self.lock:
            return self.keys.setdefault((owner, key), (body_hash, initiative_id))


class PgStore:
    def __init__(self, dsn: str):
        import psycopg
        self.psycopg, self.dsn = psycopg, dsn

    def _conn(self):
        return self.psycopg.connect(self.dsn, autocommit=True)

    def init(self):
        with self._conn() as c:
            c.execute(SCHEMA_SQL)

    def save(self, snap: dict):
        i = snap["initiative"]
        with self._conn() as c, c.transaction():
            c.execute("""insert into initiatives (initiative_id, owner, goal, state, team, latest_iteration_id, external_refs, status, package_version)
                         values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                         on conflict (initiative_id) do update set state=excluded.state, team=excluded.team,
                           latest_iteration_id=excluded.latest_iteration_id, status=excluded.status,
                           package_version=excluded.package_version, updated_at=now()""",
                      (i["initiative_id"], i["owner"], i["goal"], i["state"], i.get("team"), i["latest_iteration_id"],
                       json.dumps(i.get("external_refs") or {}), json.dumps(i["status"]), i.get("package_version", 0)))
            for it in snap["iterations"]:
                c.execute("""insert into iterations (iteration_id, initiative_id, number, body) values (%s,%s,%s,%s)
                             on conflict (iteration_id) do update set body=excluded.body, updated_at=now()""",
                          (it["iteration_id"], i["initiative_id"], it["number"], json.dumps(it)))

    def iteration(self, iteration_id):
        with self._conn() as c:
            row = c.execute("select body from iterations where iteration_id=%s", (iteration_id,)).fetchone()
        return row[0] if row else None

    _COLS = "initiative_id, owner, goal, state, team, latest_iteration_id, external_refs, status, package_version, created_at, updated_at"

    def _row(self, r):
        keys = [k.strip() for k in self._COLS.split(",")]
        d = dict(zip(keys, r))
        d["created_at"], d["updated_at"] = d["created_at"].isoformat(), d["updated_at"].isoformat()
        return d

    def initiative(self, initiative_id):
        with self._conn() as c:
            row = c.execute(f"select {self._COLS} from initiatives where initiative_id=%s", (initiative_id,)).fetchone()
        return self._row(row) if row else None

    def initiatives(self, owner=None):
        with self._conn() as c:
            rows = c.execute(f"select {self._COLS} from initiatives" + (" where owner=%s" if owner else "")
                             + " order by updated_at desc", (owner,) if owner else ()).fetchall()
        return [self._row(r) for r in rows]

    def latest(self, initiative_id):
        ini = self.initiative(initiative_id)
        return self.iteration(ini["latest_iteration_id"]) if ini else None

    def claim_start(self, owner, key, body_hash, initiative_id):
        """Атомарно: первый вызов с ключом записывается, повтор получает записанное."""
        with self._conn() as c:
            c.execute("insert into start_keys (owner, key, body_hash, initiative_id) values (%s,%s,%s,%s) "
                      "on conflict do nothing", (owner, key, body_hash, initiative_id))
            row = c.execute("select body_hash, initiative_id from start_keys where owner=%s and key=%s", (owner, key)).fetchone()
        return tuple(row)
