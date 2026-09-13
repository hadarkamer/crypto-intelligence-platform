"""Network-free HTTP boundary checks: no false ACK, bounded/safe diagnostics."""
import contextlib
from email.message import Message
import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import google_sheets_sync as sync


class Response:
    def __init__(self, body, *, url="https://script.googleusercontent.com/macros/echo?user_content_key=PRIVATE", content_type="application/json", status=200):
        self.body = body
        self.url = url
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type
        self.read_sizes = []

    def geturl(self):
        return self.url

    def read(self, size=-1):
        self.read_sizes.append(size)
        return self.body if size < 0 else self.body[:size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TransportTest(unittest.TestCase):
    def setUp(self):
        self.patches = [
            patch.object(sync, "enabled", return_value=True),
            patch.object(sync, "_WEBHOOK_URL", "https://script.google.com/macros/s/test/exec"),
            patch.object(sync, "_WEBHOOK_SECRET", "REQUEST_SECRET"),
            patch.object(sync, "_RECEIVER_VERSION", None),
            patch.object(sync, "_ORDERED_BATCH_FALLBACK", False),
            patch.object(sync, "_LAST_HTTP_DIAGNOSTIC", None),
            patch.object(sync, "_DURABLE_SNAPSHOT_MODE", False),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)
        self.payload = {"kind": "research_sheet_upserts", "upserts": []}

    def send(self, response=None, error=None):
        output = io.StringIO()
        with patch.object(sync, "urlopen", return_value=response, side_effect=error) as http, contextlib.redirect_stdout(output):
            result = sync.deliver_now(self.payload)
        self.assertEqual(http.call_count, 1, "durable outbox owns future retries")
        self.assertNotIn("REQUEST_SECRET", output.getvalue())
        self.assertNotIn("PRIVATE", output.getvalue())
        return result, output.getvalue()

    def test_valid_ack_is_bounded_and_keeps_legacy_compatibility(self):
        for body, content_type, limit in [
            (b'{"ok":true}', "text/plain", 1),
            (b'{"ok":true,"version":"sheets-batch-v3"}', "application/json; charset=utf-8", 8),
        ]:
            with self.subTest(body=body):
                response = Response(body, content_type=content_type)
                self.assertTrue(self.send(response)[0])
                self.assertEqual(response.read_sizes, [sync._ACK_RESPONSE_MAX_BYTES + 1])
                self.assertEqual(sync.ordered_outcome_batch_limit(), limit)
                diagnostic = sync.status()["last_http_diagnostic"]
                self.assertEqual(diagnostic["outcome"], "CONFIRMED")
                self.assertEqual(diagnostic["response_stage"], "CONTENT_RESPONSE")
                self.assertNotIn("PRIVATE", repr(diagnostic))

    def test_invalid_bodies_never_ack_or_promote_receiver(self):
        for body, outcome in [
            (b"", "INVALID_JSON"),
            (b"<html>PRIVATE</html>", "INVALID_JSON"),
            (b'{"ok":', "INVALID_JSON"),
            (b"\xff", "INVALID_JSON"),
            (b"[]", "INVALID_ACK_SHAPE"),
            (b"null", "INVALID_ACK_SHAPE"),
            (b'{"ok":1,"version":"sheets-batch-v3"}', "RECEIVER_REJECTED"),
            (b'{"ok":false,"error":"PRIVATE"}', "RECEIVER_REJECTED"),
            (b"x" * (sync._ACK_RESPONSE_MAX_BYTES + 2), "RESPONSE_TOO_LARGE"),
        ]:
            with self.subTest(outcome=outcome, size=len(body)):
                result, output = self.send(Response(body, content_type="text/html"))
                self.assertFalse(result)
                self.assertIsNone(sync._RECEIVER_VERSION)
                self.assertTrue(sync._ORDERED_BATCH_FALLBACK)
                self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], outcome)
                self.assertNotIn("<html>", output)

    def test_404_classifies_final_host_without_one_time_url(self):
        for host, stage in [("script.google.com", "WEBHOOK_RESPONSE"), ("script.googleusercontent.com", "CONTENT_RESPONSE")]:
            with self.subTest(host=host):
                headers = Message()
                headers["Content-Type"] = "text/html; charset=utf-8"
                exc = HTTPError("https://" + host + "/macros/echo?user_content_key=PRIVATE", 404, "PRIVATE", headers, io.BytesIO(b"PRIVATE"))
                self.assertFalse(self.send(error=exc)[0])
                diagnostic = sync.status()["last_http_diagnostic"]
                self.assertEqual(diagnostic["http_status"], 404)
                self.assertEqual(diagnostic["response_host"], host)
                self.assertEqual(diagnostic["response_stage"], stage)
                self.assertEqual(diagnostic["content_type"], "text/html")
                self.assertEqual(diagnostic["outcome"], "HTTP_ERROR")

    def test_timeout_does_not_guess_phase_and_valid_ack_recovers(self):
        self.assertFalse(self.send(error=TimeoutError("PRIVATE"))[0])
        self.assertEqual(sync.status()["last_http_diagnostic"]["response_stage"], "REQUEST_OR_REDIRECT")
        self.assertEqual(sync.ordered_outcome_batch_limit(), 1)
        self.assertTrue(self.send(Response(b'{"ok":true,"version":"sheets-batch-v3"}'))[0])
        self.assertEqual(sync.ordered_outcome_batch_limit(), 8)


if __name__ == "__main__":
    unittest.main()
