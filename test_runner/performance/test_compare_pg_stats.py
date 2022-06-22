import os
import threading
from typing import List

import time
import pytest
from fixtures.compare_fixtures import NeonCompare, PgCompare
from fixtures.pg_stats import PgStatTable
from fixtures.log_helper import log

from performance.test_perf_pgbench import get_durations_matrix, get_scales_matrix


def get_seeds_matrix(default: int = 100):
    seeds = os.getenv("TEST_PG_BENCH_SEEDS_MATRIX", default=str(default))
    return list(map(int, seeds.split(",")))


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_rw_with_pgbench_default(neon_with_baseline: PgCompare,
                                                  seed: int,
                                                  scale: int,
                                                  duration: int,
                                                  pg_stats_rw: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_rw):
        env.pg_bin.run_capture(
            ['pgbench', f'-T{duration}', f'--random-seed={seed}', '-Mprepared', env.pg.connstr()])
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wo_with_pgbench_simple_update(neon_with_baseline: PgCompare,
                                                        seed: int,
                                                        scale: int,
                                                        duration: int,
                                                        pg_stats_wo: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wo):

        stop_event = threading.Event()
        log_thread = threading.Thread(target=_log_pg_stats_rate,
                                      args=(env, pg_stats_wo, stop_event, 0.5))
        log_thread.start()

        env.pg_bin.run_capture([
            'pgbench',
            '-N',
            f'-T{duration}',
            f'--random-seed={seed}',
            '-r',
            '-Mprepared',
            env.pg.connstr()
        ])
        env.flush()

        stop_event.set()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_ro_with_pgbench_select_only(neon_with_baseline: PgCompare,
                                                      seed: int,
                                                      scale: int,
                                                      duration: int,
                                                      pg_stats_ro: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_ro):
        stop_event = threading.Event()
        log_thread = threading.Thread(target=_log_pg_stats_rate,
                                      args=(env, pg_stats_ro, stop_event, 0.5))
        log_thread.start()

        env.pg_bin.run_capture([
            'pgbench',
            '-S',
            f'-T{duration}',
            f'--random-seed={seed}',
            '-Mprepared',
            '-r',
            env.pg.connstr()
        ])
        env.flush()

        stop_event.set()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wal_with_pgbench_default(neon_with_baseline: PgCompare,
                                                   seed: int,
                                                   scale: int,
                                                   duration: int,
                                                   pg_stats_wal: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wal):
        env.pg_bin.run_capture(
            ['pgbench', f'-T{duration}', f'--random-seed={seed}', '-Mprepared', env.pg.connstr()])
        env.flush()


def _log_pg_stats_rate(env: PgCompare,
                       pg_stats: List[PgStatTable],
                       stop_event: threading.Event,
                       polling_interval=1.0):
    prev_data = env._retrieve_pg_stats(pg_stats)
    time.sleep(polling_interval)

    while not stop_event.is_set():
        data = env._retrieve_pg_stats(pg_stats)
        for k in set(prev_data) & set(data):
            log.info(f"{k}/s: {(data[k] - prev_data[k]) / polling_interval}")

        prev_data = data
        time.sleep(polling_interval)
