"""
Python SDK tests. A local HTTP server stands in for the ingest endpoint, so these run
without a Vigil instance. The point is the same as the other SDKs' tests: it posts exactly
the envelope the endpoint validates, and it never changes how the host process crashes.

    python3 -m pytest test_vigil.py    # or: python3 test_vigil.py
"""
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import vigil

_received = []


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        _received.append(json.loads(body))
        self.send_response(202)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass  # keep the test output quiet


class VigilTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), _Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        _received.clear()
        vigil.init(key="vg_pub_test", endpoint=f"http://127.0.0.1:{self.port}/api/ingest")

    def tearDown(self):
        vigil.close()

    def test_capture_exception_sends_valid_envelope(self):
        try:
            {}["missing"]
        except Exception:
            vigil.capture_exception()
        vigil.flush()
        time.sleep(0.05)

        self.assertEqual(len(_received), 1)
        env = _received[0]
        self.assertEqual(env["sdk"]["name"], "vigil-python")
        self.assertEqual(env["project_key"], "vg_pub_test")
        ev = env["events"][0]
        self.assertEqual(ev["type"], "error")
        self.assertEqual(ev["exception"]["type"], "KeyError")
        # A traceback is the whole reason to use the SDK over a bare log line.
        self.assertTrue(len(ev["exception"]["stacktrace"]) > 0)
        # Server identity rides in tags, since the envelope context shape is fixed.
        self.assertTrue(ev["context"]["tags"]["runtime"].startswith("python-"))
        self.assertIn("server_name", ev["context"]["tags"])

    def test_capture_message(self):
        vigil.capture_message("worker booted", level="info")
        vigil.flush()
        time.sleep(0.05)
        self.assertEqual(_received[0]["events"][0]["type"], "message")

    def test_inert_before_init(self):
        vigil.close()  # simulate "not initialised"
        # Must be safe no-ops rather than raising.
        vigil.capture_exception()
        vigil.capture_message("nothing")
        vigil.flush()


if __name__ == "__main__":
    unittest.main()
