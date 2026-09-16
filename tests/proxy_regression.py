"""Local-only end-to-end checks. Run after cargo build: python3 tests/proxy_regression.py."""
import http.client
import http.server
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
import unittest

BINARY = pathlib.Path(__file__).resolve().parents[1] / "target/debug/yu-ri"

class Origin(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        code = 404 if self.path == "/missing" else 200
        body = b"origin response"
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        if self.path == "/no-store":
            self.send_header("Cache-Control", "no-store")
        elif self.path == "/vary":
            self.send_header("Vary", "*")
        else:
            self.send_header("Cache-Control", "max-age=60")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

class ProxyRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.origin = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Origin)
        cls.thread = threading.Thread(target=cls.origin.serve_forever, daemon=True)
        cls.thread.start()
        cls.tmp = tempfile.TemporaryDirectory(prefix="yu-ri-e2e-")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            cls.port = sock.getsockname()[1]
        pathlib.Path(cls.tmp.name, "config.toml").write_text(
            '[settings]\nhost="127.0.0.1"\nport=' + str(cls.port)
            + '\nlog="off"\n[settings.cache]\ndir="cache"\nsize=1048576\nttl=60\n[upstream]\nurl="http://127.0.0.1:'
            + str(cls.origin.server_port) + '"\n')
        cls.proc = subprocess.Popen([str(BINARY)], cwd=cls.tmp.name, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=.1):
                    break
            except OSError:
                if cls.proc.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("proxy did not start")
                time.sleep(.02)

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        try:
            cls.proc.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            cls.proc.kill()
            cls.proc.communicate()
        cls.origin.shutdown()
        cls.origin.server_close()
        cls.thread.join()
        cls.tmp.cleanup()

    def request(self, path):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def test_uncacheable_requests_do_not_leave_waiters_stuck(self):
        for path, status in [("/no-store", 200), ("/missing", 404), ("/vary", 200)]:
            with self.subTest(path=path):
                self.assertEqual(self.request(path), (status, b"origin response"))
                self.assertEqual(self.request(path), (status, b"origin response"))

if __name__ == "__main__":
    unittest.main()
