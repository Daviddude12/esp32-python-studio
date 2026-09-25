import asyncio
import fcntl
import importlib
import json
import os
import pty
import signal
import struct
import subprocess
import tempfile
import termios
from pathlib import Path

try:
    _fastapi = importlib.import_module("fastapi")
    FastAPI = _fastapi.FastAPI
    WebSocket = _fastapi.WebSocket
    WebSocketDisconnect = _fastapi.WebSocketDisconnect
    HTTPException = _fastapi.HTTPException
except ImportError:
    _starlette_applications = importlib.import_module("starlette.applications")
    _starlette_websockets = importlib.import_module("starlette.websockets")
    _starlette_exceptions = importlib.import_module("starlette.exceptions")
    FastAPI = _starlette_applications.Starlette
    WebSocket = _starlette_websockets.WebSocket
    WebSocketDisconnect = _starlette_websockets.WebSocketDisconnect
    HTTPException = _starlette_exceptions.HTTPException
try:
    FileResponse = importlib.import_module("fastapi.responses").FileResponse
except ImportError:
    FileResponse = importlib.import_module("starlette.responses").FileResponse
try:
    StaticFiles = importlib.import_module("fastapi.staticfiles").StaticFiles
except ImportError:
    StaticFiles = importlib.import_module("starlette.staticfiles").StaticFiles


ROOT = Path(__file__).resolve().parent

RUNNER_TOKEN = os.environ.get("RUNNER_TOKEN", "")

MAX_CODE_BYTES = 200_000

MAX_RUNTIME_SECONDS = 600


app = FastAPI()


def check_token(value: str | None):

    if RUNNER_TOKEN and value != RUNNER_TOKEN:

        raise HTTPException(
            status_code=401,
            detail="Invalid runner token"
        )


def set_pty_size(
    fd,
    rows=30,
    cols=120
):

    winsize = struct.pack(
        "HHHH",
        rows,
        cols,
        0,
        0
    )

    fcntl.ioctl(
        fd,
        termios.TIOCSWINSZ,
        winsize
    )


@app.get("/")
def index():

    return FileResponse(
        ROOT / "index.html"
    )


@app.get("/health")
def health():

    return {
        "ok": True
    }


@app.websocket("/ws/run")
async def run_python(
    websocket
):

    await websocket.accept()


    if RUNNER_TOKEN:

        supplied = websocket.query_params.get(
            "token",
            ""
        )

        if supplied != RUNNER_TOKEN:

            await websocket.send_text(
                json.dumps({
                    "type": "error",
                    "message": "Invalid runner token"
                })
            )

            await websocket.close(
                code=1008
            )

            return


    proc = None
    master_fd = None
    temp_path = None


    try:

        first = await websocket.receive_text()


        request = json.loads(first)


        if request.get("type") != "run":

            raise ValueError(
                "Expected a run request"
            )


        code = request.get("code", "")


        if not isinstance(code, str):

            raise ValueError(
                "code must be a string"
            )


        if len(
            code.encode("utf-8")
        ) > MAX_CODE_BYTES:

            raise ValueError(
                "Code is too large"
            )


        fd, temp_path = tempfile.mkstemp(
            suffix=".py",
            prefix="esp32_python_"
        )


        os.close(fd)


        Path(temp_path).write_text(
            code,
            encoding="utf-8"
        )


        master_fd, slave_fd = pty.openpty()


        set_pty_size(
            master_fd
        )


        env = os.environ.copy()

        env["PYTHONUNBUFFERED"] = "1"

        env["TERM"] = "xterm-256color"


        proc = subprocess.Popen(

            [
                "python3",
                "-u",
                temp_path
            ],

            stdin=slave_fd,

            stdout=slave_fd,

            stderr=slave_fd,

            start_new_session=True,

            cwd=str(ROOT),

            env=env,

            close_fds=True

        )


        os.close(slave_fd)

        slave_fd = None


        loop = asyncio.get_running_loop()


        output_queue = asyncio.Queue()


        def reader():

            try:

                while True:

                    data = os.read(
                        master_fd,
                        4096
                    )


                    if not data:

                        break


                    asyncio.run_coroutine_threadsafe(
                        output_queue.put(data),
                        loop
                    )


            except OSError:

                pass


            finally:

                asyncio.run_coroutine_threadsafe(
                    output_queue.put(None),
                    loop
                )


        reader_task = asyncio.create_task(
            asyncio.to_thread(reader)
        )


        async def send_output():

            while True:

                data = await output_queue.get()


                if data is None:

                    break


                await websocket.send_text(

                    json.dumps({

                        "type": "output",

                        "data":
                            data.decode(
                                "utf-8",
                                errors="replace"
                            )

                    })

                )


        async def receive_input():

            while True:

                message = await websocket.receive_text()


                payload = json.loads(message)


                kind = payload.get("type")


                if kind == "input":

                    text = payload.get(
                        "data",
                        ""
                    )


                    if isinstance(text, str):

                        os.write(

                            master_fd,

                            text.encode(
                                "utf-8"
                            )

                        )


                elif kind == "resize":

                    rows = int(
                        payload.get(
                            "rows",
                            30
                        )
                    )


                    cols = int(
                        payload.get(
                            "cols",
                            120
                        )
                    )


                    set_pty_size(
                        master_fd,
                        rows,
                        cols
                    )


                elif kind == "stop":

                    if proc.poll() is None:

                        os.killpg(
                            proc.pid,
                            signal.SIGTERM
                        )

                    return


        output_task = asyncio.create_task(
            send_output()
        )


        input_task = asyncio.create_task(
            receive_input()
        )


        try:

            await asyncio.wait_for(

                proc_wait(proc),

                timeout=MAX_RUNTIME_SECONDS

            )


        except asyncio.TimeoutError:

            if proc.poll() is None:

                os.killpg(
                    proc.pid,
                    signal.SIGKILL
                )


            await websocket.send_text(

                json.dumps({

                    "type": "error",

                    "message":
                        "Program timed out."

                })

            )


        finally:

            if proc.poll() is None:

                os.killpg(
                    proc.pid,
                    signal.SIGTERM
                )


            input_task.cancel()


            await asyncio.gather(
                input_task,
                return_exceptions=True
            )


            await reader_task


            await output_task


            await websocket.send_text(

                json.dumps({

                    "type": "exit",

                    "code": proc.returncode

                })

            )


    except WebSocketDisconnect:

        if proc and proc.poll() is None:

            try:

                os.killpg(
                    proc.pid,
                    signal.SIGTERM
                )

            except ProcessLookupError:

                pass


    except Exception as exc:

        try:

            await websocket.send_text(

                json.dumps({

                    "type": "error",

                    "message": str(exc)

                })

            )

        except Exception:

            pass


    finally:

        if master_fd is not None:

            try:

                os.close(master_fd)

            except OSError:

                pass


        if temp_path:

            try:

                os.unlink(
                    temp_path
                )

            except FileNotFoundError:

                pass


async def proc_wait(proc):

    return await asyncio.to_thread(
        proc.wait
    )


app.mount(
    "/static",
    StaticFiles(directory=ROOT),
    name="static"
)