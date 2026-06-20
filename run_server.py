import glob
import os
import sys
import uvicorn
from app.core.logging_config import setup_logging


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_server.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    try:
        # create tbi cache dir and clean stale index files
        os.makedirs("/tmp/tbi_cache", exist_ok=True)
        for file in glob.glob("/tmp/tbi_cache/**/*.tbi", recursive=True) + glob.glob("/tmp/tbi_cache/**/*.csi", recursive=True):
            os.remove(file)
        # verify every configured tabix/mapping file is reachable before serving;
        # raises and aborts startup (below) if any are missing or unreadable.
        # this is the authoritative fail-fast gate; actual warming and an
        # end-to-end query smoke test run in app.server's lifespan, on the
        # serving event loop, so no work is done twice or thrown away.
        from app.services.startup_checks import verify_all_data_files

        verify_all_data_files()
        # configure JSON logging before uvicorn starts so its loggers propagate to root
        setup_logging()
        # reload watches the source tree and serves from a worker subprocess; default
        # to a plain single process in production. Warming and the smoke query live in
        # app.server's lifespan, so they run once in whichever process serves either
        # way. Opt into reload for local dev with RELOAD=1.
        reload = os.environ.get("RELOAD", "").lower() in ("1", "true", "yes")
        # use asyncio event loop instead of uvloop - uvloop uses sockets instead of pipes
        # for subprocess stdin, which can break tabix's -R /dev/stdin option (uvloop issue #532)
        uvicorn.run("app.server:app", host="0.0.0.0", port=port, reload=reload, loop="asyncio", log_config=None)
    except Exception as e:
        # log error without exposing full traceback to stdout in production
        import logging

        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Server startup failed: {e}", exc_info=True)
        sys.exit(1)
