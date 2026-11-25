from datetime import datetime, timezone
import io
import json

from app.engine import Engine
from app.store import Store


def test_existing_rate_cooldown_is_shortened_to_fixed_interval(tmp_path):
    from datetime import timedelta
    class Vault:
        def read(self): return {'auth_token': 'fake'}
    for source in ('', 'server'):
        store = Store(tmp_path / (source or 'legacy'))
        later = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        store.save_settings({'retry_upgrade_done': True, 'rate_limit_until': later, 'rate_limit_source': source})
        store.update_author(1, status='error', error='Rate limit exceeded', next_run=later, retry_full=1)
        Engine(store, Vault())
        author = store.author(1)
        assert 0 < (datetime.fromisoformat(author['next_run']) - datetime.now(timezone.utc)).total_seconds() <= 180
        assert author['next_run'] == store.get_setting('rate_limit_until')
        assert author['retry_full'] == 1


def test_rate_limit_response_retries_in_three_minutes_despite_server_reset(tmp_path, monkeypatch):
    import app.engine as engine_module
    from datetime import timedelta
    class Vault:
        def read(self): return {"auth_token": "test-token"}
    reset = int((datetime.now(timezone.utc) + timedelta(minutes=7)).timestamp())
    class Process:
        def __init__(self, *args, **kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO('\n'.join(json.dumps(x) for x in [
                {"event":"rate_limit", "actual":True, "reset":str(reset)},
                {"event":"log", "level":"error", "message":"Rate limit exceeded"}]) + '\n')
        def wait(self, **kwargs): return 1
        def poll(self): return 1
    monkeypatch.setattr(engine_module.subprocess, 'Popen', Process)
    store = Store(tmp_path / 'data')
    engine = Engine(store, Vault())
    engine._run(1, True)
    author = store.author(1)
    assert '429' in author['error']
    assert 2.9 < (datetime.fromisoformat(author['next_run']) - datetime.now(timezone.utc)).total_seconds() / 60 <= 3
    assert author['retry_full'] == 1


def test_cooled_x_does_not_block_other_platforms(tmp_path):
    import threading
    from datetime import timedelta
    class Vault:
        def read(self): return {'auth_token': 'fake'}
    store = Store(tmp_path / 'data')
    store.save_settings({'automatic': False, 'rate_limit_until': (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()})
    other = store.add_author('SecondUser')
    with store.connect() as db:
        db.execute("UPDATE authors SET handle='instagram:test' WHERE id=?", (other,))
    engine = Engine(store, Vault())
    seen = []
    done = threading.Event()
    engine._run = lambda author_id, full: (seen.append(author_id), done.set())
    # Direct tickets avoid creating or reading real platform credentials.
    for aid in (1, other):
        engine.pending[aid] = str(aid)
        engine.queue.put((1, next(engine.sequence), aid, False, str(aid), False))
    engine.start()
    try:
        assert done.wait(3)
        assert seen == [other]
    finally:
        engine.close()


def test_rate_limit_backoff_keeps_full_retry_and_watermark(tmp_path, monkeypatch):
    import app.engine as engine_module
    class Vault:
        def read(self): return {"auth_token": "test-token"}
    class Process:
        def __init__(self, *args, **kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO(json.dumps({"event":"log", "level":"error", "message":"Rate limit exceeded"}) + '\n')
        def wait(self, **kwargs): return 1
        def poll(self): return 1
    monkeypatch.setattr(engine_module.subprocess, 'Popen', Process)
    store = Store(tmp_path / 'data')
    store.update_author(1, last_success='2026-01-01T00:00:00+00:00')
    engine = Engine(store, Vault())
    engine._run(1, True)
    author = store.author(1)
    assert author['last_success'] == '2026-01-01T00:00:00+00:00'
    assert author['retry_full'] == 1 and author['retry_count'] == 1
    assert 2.9 < (datetime.fromisoformat(author['next_run']) - datetime.now(timezone.utc)).total_seconds() / 60 <= 3
    assert store.get_setting('rate_limit_until') == author['next_run']
    engine._run(1, True)
    assert store.author(1)['retry_count'] == 2
    assert 2.9 < (datetime.fromisoformat(store.author(1)['next_run']) - datetime.now(timezone.utc)).total_seconds() / 60 <= 3
    store.update_author(1, retry_count=18)
    engine._run(1, True)
    assert 2.9 < (datetime.fromisoformat(store.author(1)['next_run']) - datetime.now(timezone.utc)).total_seconds() / 60 <= 3
    assert engine.enqueue(1) is True
    assert engine.queue.get_nowait()[3] is True


def test_all_media_exceeds_old_page_limit(tmp_path):
    store = Store(tmp_path / 'data')
    with store.connect() as db:
        db.executemany('INSERT INTO media VALUES (?,?,?,?,?,?,?,?)', [
            (str(tmp_path / f'{i}.jpg'), 1, str(1000+i), 1, '2026-01-01', 1, 'image', '2026-01-01') for i in range(123)
        ])
    assert len(store.media_page()['items']) == 100
    result = store.media_page(all_items=True)
    assert result['total'] == len(result['items']) == 123


def test_manual_sync_promotes_cooled_retry_and_starts_immediately(tmp_path):
    import threading
    import time
    from datetime import timedelta
    class Vault:
        def read(self): return {'auth_token':'fake-token'}
    store = Store(tmp_path/'data')
    store.save_settings({'automatic': False, 'rate_limit_until': (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()})
    store.update_author(1, retry_full=1, retry_count=1)
    engine = Engine(store, Vault())
    started = threading.Event()
    seen = []
    def run(author_id, full):
        seen.append((author_id, full))
        started.set()
    engine._run = run
    engine.start()
    try:
        assert engine.enqueue(1)
        time.sleep(.2)
        assert not started.is_set()
        assert engine.enqueue(1, manual=True)
        assert started.wait(2)
        assert seen == [(1, True)]
    finally:
        engine.close()


def test_manual_waits_for_running_scan_then_goes_before_automatic(tmp_path):
    import threading
    import time
    class Vault:
        def read(self): return {'auth_token':'fake-token'}
    store = Store(tmp_path/'data')
    store.save_settings({'automatic': False})
    second = store.add_author('SecondUser')
    third = store.add_author('ThirdUser')
    engine = Engine(store, Vault())
    first_started, release, done = threading.Event(), threading.Event(), threading.Event()
    order = []
    def run(author_id, full):
        order.append(author_id)
        if author_id == 1:
            first_started.set(); release.wait(3)
        if len(order) == 3: done.set()
    engine._run = run
    engine.start()
    try:
        assert engine.enqueue(1, manual=True) and first_started.wait(2)
        assert engine.enqueue(second)
        assert engine.enqueue(third, manual=True)
        assert order == [1]
        release.set()
        assert done.wait(3)
        assert order == [1, third, second]
    finally:
        release.set(); engine.close()
