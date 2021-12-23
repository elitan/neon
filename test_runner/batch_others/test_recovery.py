from contextlib import closing
import psycopg2.extras
from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log
import os
import time

pytest_plugins = ("fixtures.zenith_fixtures")


def test_recovery(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()
    # Create a branch for us
    env.zenith_cli(["branch", "test_recovery", "main"])

    pg = env.postgres.create_start('test_recovery')
    log.info("postgres is running on 'test_recovery' branch")

    connstr = pg.connstr()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with closing(env.pageserver.connect()) as psconn:
                with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:
                    # Create and initialize test table
                    cur.execute("CREATE TABLE foo(x bigint)")
                    cur.execute("INSERT INTO foo VALUES (generate_series(1,100000))")

                    # Sleep for some time to let checkpoint create image layers
                    time.sleep(2)

                    # Configure failpoints
                    pscur.execute(
                        "failpoints checkpoint-before-sync=sleep(2000);checkpoint-after-sync=panic")

                    # Do some updates until pageserver is crashed
                    try:
                        while True:
                            cur.execute("update foo set x=x+1")
                    except Exception as err:
                        log.info(f"Excepted server crash {err}")

    log.info("Wait before server restart")
    env.pageserver.stop()
    env.pageserver.start()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from foo")
            assert cur.fetchone() == (100000, )
