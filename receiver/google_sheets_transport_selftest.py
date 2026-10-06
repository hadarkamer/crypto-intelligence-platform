"""Network-free HTTP boundary checks: no false ACK, bounded/safe diagnostics."""
import contextlib
from email.message import Message
import hashlib
import hmac
from http.client import IncompleteRead
import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

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
            patch.object(sync, "_ACK_MODE", "json"),
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


class HTMLTransportTest(unittest.TestCase):
    setUp = TransportTest.setUp
    send = TransportTest.send

    def signed_response(self, request, *, raw_request=None, transform=None, **kwargs):
        raw_request = request.data if raw_request is None else raw_request
        signature = hmac.new(b"REQUEST_SECRET", b"sheets-ack-v1\n" + raw_request,
                             hashlib.sha256).hexdigest()
        title = "sheets-ack-v1:" + signature
        body = ("<!DOCTYPE html><html><head><title>" + title +
                "</title></head><body><script>var x = '<title>ignored</title>';</script>" +
                "<iframe src='https://script.googleusercontent.com/ignored'></iframe></body></html>").encode()
        if transform:
            body = transform(body, title)
        return Response(body, **dict({"url": "https://script.google.com/macros/s/test/exec",
                                     "content_type": "text/html; charset=utf-8"}, **kwargs))

    def test_signed_ack_covers_exact_unicode_body_and_preserves_payload(self):
        sync._ACK_MODE = "html_ack_v1"
        self.payload["upserts"] = [{"sheet": "תצוגת לייב", "row": {"symbol": "BTC", "value": "בדיקה"}}]
        original_payload = json.dumps(self.payload, ensure_ascii=False)
        captured = []

        def reply(request, **kwargs):
            captured.append(request.data)
            envelope = json.loads(request.data)
            self.assertEqual(envelope["response_mode"], "html_ack_v1")
            self.assertRegex(envelope["ack_nonce"], r"^[a-f0-9]{32}$")
            self.assertEqual(envelope["payload"], self.payload)
            return self.signed_response(request)

        self.assertTrue(self.send(error=reply)[0])
        self.assertEqual(json.dumps(self.payload, ensure_ascii=False), original_payload)
        self.assertEqual(sync.ordered_outcome_batch_limit(), 8)
        self.assertEqual(sync.status()["ack_mode"], "html_ack_v1")
        diagnostic = sync.status()["last_http_diagnostic"]
        self.assertEqual(diagnostic["ack_mode"], "html_ack_v1")
        self.assertEqual(diagnostic["outcome"], "CONFIRMED")
        self.assertNotIn(json.loads(captured[0])["ack_nonce"], repr(diagnostic))

    def test_earlier_ack_cannot_confirm_later_request_or_modified_payload(self):
        sync._ACK_MODE = "html_ack_v1"
        old_request = []

        def first(request, **kwargs):
            old_request.append(request.data)
            return self.signed_response(request)

        self.assertTrue(self.send(error=first)[0])
        self.payload["upserts"] = [{"sheet": "Snapshots", "row": {"snapshot_id": "next-generation"}}]

        def replay(request, **kwargs):
            self.assertNotEqual(json.loads(request.data)["ack_nonce"], json.loads(old_request[0])["ack_nonce"])
            return self.signed_response(request, raw_request=old_request[0])

        self.assertFalse(self.send(error=replay)[0])
        self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], "INVALID_HTML_ACK")
        self.assertEqual(sync.ordered_outcome_batch_limit(), 1)

    def test_each_retry_has_fresh_nonce_and_rejects_prior_attempt_signature(self):
        sync._ACK_MODE = "html_ack_v1"
        bodies = []

        def reply(request, **kwargs):
            bodies.append(request.data)
            if len(bodies) == 1:
                raise TimeoutError("PRIVATE")
            return self.signed_response(request, raw_request=bodies[0])

        with patch.object(sync, "urlopen", side_effect=reply), patch.object(sync.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
            result = sync._deliver_envelope({"secret": "REQUEST_SECRET", "payload": self.payload}, attempts=2)
        self.assertFalse(result)
        self.assertEqual(len(bodies), 2)
        self.assertNotEqual(json.loads(bodies[0])["ack_nonce"], json.loads(bodies[1])["ack_nonce"])

    def test_html_status_host_and_mime_must_all_match(self):
        sync._ACK_MODE = "html_ack_v1"
        for changes, expected in [
            ({"status": 201}, "INVALID_ACK_STATUS"),
            ({"status": 404}, "INVALID_ACK_STATUS"),
            ({"url": "https://accounts.google.com/ServiceLogin"}, "INVALID_ACK_HOST"),
            ({"url": "https://script.google.com.evil.invalid/"}, "INVALID_ACK_HOST"),
            ({"url": "http://script.google.com/"}, "INVALID_ACK_HOST"),
            ({"url": "https://script.google.com:8443/"}, "INVALID_ACK_HOST"),
            ({"url": "https://user@script.google.com/"}, "INVALID_ACK_HOST"),
            ({"content_type": "application/json"}, "INVALID_ACK_CONTENT_TYPE"),
            ({"content_type": "text/plain"}, "INVALID_ACK_CONTENT_TYPE"),
        ]:
            with self.subTest(changes=changes):
                self.assertFalse(self.send(error=lambda request, **kw: self.signed_response(request, **changes))[0])
                self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], expected)
                self.assertIsNone(sync._RECEIVER_VERSION)

    def test_html_must_contain_exactly_one_complete_valid_title(self):
        sync._ACK_MODE = "html_ack_v1"
        for transform in [
            lambda body, title: b"",
            lambda body, title: b"\xff",
            lambda body, title: b"<html><title>Sign in</title></html>",
            lambda body, title: body.replace(title.encode(), b"sheets-ack-v1:" + b"0" * 64),
            lambda body, title: body + ("<title>" + title + "</title>").encode(),
            lambda body, title: ("<title>" + title).encode(),
            lambda body, title: ("<title>" + title + "<b></b></title>").encode(),
            lambda body, title: ("<title>" + title + "<?ignored?></title>").encode(),
            lambda body, title: ("<script>var title='<title>" + title + "</title>';</script>").encode(),
            lambda body, title: ("<body>" + title + "</body>").encode(),
            lambda body, title: ("<title> " + title + " </title>").encode(),
            lambda body, title: b'{"ok":true,"version":"sheets-batch-v3"}',
        ]:
            with self.subTest(transform=transform):
                self.assertFalse(self.send(error=lambda request, **kw: self.signed_response(request, transform=transform))[0])
                self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], "INVALID_HTML_ACK")
                self.assertIsNone(sync._RECEIVER_VERSION)

    def test_oversized_response_rejected_before_ack_and_json_never_falls_back(self):
        sync._ACK_MODE = "html_ack_v1"
        oversized = lambda body, title: body + b"x" * sync._ACK_RESPONSE_MAX_BYTES
        self.assertFalse(self.send(error=lambda request, **kw: self.signed_response(request, transform=oversized))[0])
        self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], "RESPONSE_TOO_LARGE")
        self.assertFalse(self.send(Response(b'{"ok":true,"version":"sheets-batch-v3"}'))[0])
        self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], "INVALID_ACK_CONTENT_TYPE")
        self.assertIsNone(sync._RECEIVER_VERSION)

    def test_json_rollback_removes_html_fields_and_bad_mode_never_posts(self):
        envelope = {"secret": "REQUEST_SECRET", "payload": self.payload,
                    "response_mode": "html_ack_v1", "ack_nonce": "0" * 32}

        def reply(request, **kwargs):
            actual = json.loads(request.data)
            self.assertNotIn("response_mode", actual)
            self.assertNotIn("ack_nonce", actual)
            return Response(b'{"ok":true}')

        with patch.object(sync, "urlopen", side_effect=reply), contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(sync._deliver_envelope(envelope, attempts=1))
        sync._ACK_MODE = "unsupported"
        with patch.object(sync, "urlopen") as http, contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(sync._deliver_envelope(envelope, attempts=1))
        http.assert_not_called()
        self.assertEqual(sync.status()["last_http_diagnostic"]["outcome"], "INVALID_ACK_MODE")

    def test_audit_response_keeps_json_contract_even_when_html_mode_is_enabled(self):
        sync._ACK_MODE = "html_ack_v1"

        def reply(request, **kwargs):
            envelope = json.loads(request.data)
            self.assertNotIn("response_mode", envelope)
            self.assertNotIn("ack_nonce", envelope)
            self.assertEqual(envelope["payload"]["kind"], "telegram_event_audit_page")
            return Response(b'{"ok":true,"version":"sheets-batch-v3","audit":{"rows":[]}}')

        with patch.object(sync, "urlopen", side_effect=reply) as http:
            self.assertEqual(sync.read_telegram_audit_page(start_row=2, last_row=5), {"rows": []})
        self.assertEqual(http.call_count, 1)

    def test_capacity_json_exposes_only_allowlisted_error_and_never_acks(self):
        sync._ACK_MODE = "html_ack_v1"
        for ok, code, expected in [
            (False, "WORKBOOK_CAPACITY", "WORKBOOK_CAPACITY"),
            (False, "SHEET_CAPACITY", "SHEET_CAPACITY"),
            (False, "PRIVATE", None),
            (False, ["WORKBOOK_CAPACITY"], None),
            (True, "WORKBOOK_CAPACITY", None),
            (0, "WORKBOOK_CAPACITY", None),
        ]:
            with self.subTest(ok=ok, code=code):
                raw = json.dumps({"ok": ok, "error_code": code, "error": "PRIVATE"}).encode()
                self.assertFalse(self.send(Response(raw))[0])
                diagnostic = sync.status()["last_http_diagnostic"]
                self.assertEqual(diagnostic.get("receiver_error_code"), expected)
                self.assertNotIn("PRIVATE", repr(diagnostic))
                self.assertIsNone(sync._RECEIVER_VERSION)


class AuditTransportTest(unittest.TestCase):
    setUp = TransportTest.setUp

    def test_transient_http_errors_are_narrow_and_never_expose_redirect_url(self):
        for host, status, retryable in [
            ("script.googleusercontent.com", 404, True),
            ("script.google.com", 404, False),
            ("script.google.com", 403, False),
            ("script.googleusercontent.com", 401, False),
            ("script.google.com", 429, True),
            ("script.google.com", 503, True),
            ("script.googleusercontent.com", 502, True),
            ("other.invalid", 503, False),
        ]:
            with self.subTest(host=host, status=status):
                exc = HTTPError("https://" + host + "/macros/echo?user_content_key=PRIVATE", status,
                                "PRIVATE", {}, io.BytesIO(b"PRIVATE"))
                expected = sync.SheetAuditTransportRetry if retryable else HTTPError
                with patch.object(sync, "urlopen", side_effect=exc) as http:
                    with self.assertRaises(expected) as caught:
                        sync.read_telegram_audit_page(start_row=502, last_row=13890)
                self.assertEqual(http.call_count, 1)
                if retryable:
                    self.assertNotIn("PRIVATE", str(caught.exception))
                self.assertIsNone(sync._RECEIVER_VERSION)

    def test_timeouts_reset_connections_and_incomplete_read_are_retryable(self):
        errors = [TimeoutError("PRIVATE"), ConnectionResetError("PRIVATE"),
                  IncompleteRead(b"PRIVATE"), URLError(TimeoutError("PRIVATE")),
                  URLError(ConnectionAbortedError("PRIVATE"))]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                with patch.object(sync, "urlopen", side_effect=error):
                    with self.assertRaises(sync.SheetAuditTransportRetry) as caught:
                        sync.read_telegram_audit_page(start_row=502, last_row=13890)
                self.assertNotIn("PRIVATE", str(caught.exception))
        with patch.object(sync, "urlopen", side_effect=URLError("unknown URL type")):
            with self.assertRaises(URLError):
                sync.read_telegram_audit_page()

        class PartialResponse(Response):
            def read(self, size=-1):
                raise IncompleteRead(b"PRIVATE")

        with patch.object(sync, "urlopen", return_value=PartialResponse(b"")):
            with self.assertRaises(sync.SheetAuditTransportRetry):
                sync.read_telegram_audit_page()

    def test_invalid_json_logical_failure_and_corrupt_pages_are_not_retryable_transport(self):
        for body in [b"", b"<html>PRIVATE</html>", b"\xff", b"[]", b'{"ok":false}',
                     b'{"ok":true,"version":"wrong"}',
                     b'{"ok":true,"version":"sheets-batch-v3","audit":{"rows":null}}',
                     b"x" * 512_001]:
            with self.subTest(size=len(body)):
                with patch.object(sync, "urlopen", return_value=Response(body)):
                    with self.assertRaises(ValueError):
                        sync.read_telegram_audit_page(start_row=502, last_row=13890)
                self.assertIsNone(sync._RECEIVER_VERSION)

    def test_audit_uses_bounded_fifo_and_busy_does_not_make_http_request(self):
        @contextlib.contextmanager
        def busy_slot(*, wait_seconds):
            self.assertEqual(wait_seconds, 120)
            yield False

        with patch.object(sync, "delivery_slot", side_effect=busy_slot), patch.object(sync, "urlopen") as http:
            with self.assertRaises(sync.SheetReceiverBusy):
                sync.read_telegram_audit_page(start_row=502, last_row=13890)
        http.assert_not_called()

    def test_audit_maps_only_exact_bounds_failure_and_retains_safe_http_metadata(self):
        for error, code, expected in [
            ("Error: Audit row bounds changed; restart", None, sync.SheetAuditBoundsChanged),
            ("Audit row bounds changed; restart", None, sync.SheetAuditBoundsChanged),
            ("PRIVATE", "AUDIT_ROW_BOUNDS_CHANGED", sync.SheetAuditBoundsChanged),
            ("Error: unauthorized PRIVATE", None, ValueError),
            ("Audit row bounds changed; restart PRIVATE", None, ValueError),
        ]:
            with self.subTest(expected=expected.__name__, code=code):
                body = json.dumps({"ok": False, "error": error, "error_code": code}).encode()
                with patch.object(sync, "urlopen", return_value=Response(body)):
                    with self.assertRaises(expected) as caught:
                        sync.read_telegram_audit_page(start_row=502, last_row=13890)
                if expected is ValueError:
                    self.assertNotIsInstance(caught.exception, sync.SheetAuditBoundsChanged)
                diagnostic = caught.exception.sheet_audit_diagnostic
                self.assertEqual(diagnostic["http_status"], 200)
                self.assertEqual(diagnostic["response_bytes"], len(body))
                self.assertNotIn("PRIVATE", repr(diagnostic) + str(caught.exception))
                self.assertIsNone(sync._RECEIVER_VERSION)


if __name__ == "__main__":
    unittest.main()
