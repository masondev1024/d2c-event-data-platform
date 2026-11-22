"""Regression checks at the ambiguous acknowledgement boundary of the recovery drill."""

from io import BytesIO
import json
import unittest
from unittest.mock import Mock, patch

from scripts import raffle_recovery_source as source


def response(body):
    stream = BytesIO(body)
    stream.status = 200
    return stream


class RecoverySourceTest(unittest.TestCase):
    def run_one_user(self, last_response, counts="2\t2\t2"):
        stack = Mock()
        stack.url.return_value = "http://writer"
        stack.sql.side_effect = ["17\tRecovery item 001\n41\tRecovery item 002", counts]
        self.client = Mock()
        self.client.open.side_effect = [
            response(b'<meta name="csrf-token" content="before-login">'),
            response(b'{"status":"success","csrf_token":"authenticated"}'),
            response(b'{"status":"success","event_type":"raffle.entry.accepted.v1"}'),
            last_response,
        ]
        with patch.object(source, "USER_COUNT", 1), patch.object(source, "ITEM_COUNT", 2), \
                patch.object(source, "build_opener", return_value=self.client):
            return source.seed_and_apply(stack)

    def test_lost_apply_response_stops_without_resending_or_logging_secrets(self):
        with self.assertRaises(RuntimeError) as failure:
            self.run_one_user(TimeoutError("private transport details"))
        self.assertEqual(str(failure.exception),
                         "recovery cohort recovery_user_001 POST /api/apply item_id=41 failed "
                         "(TimeoutError, previously acknowledged for this user=1)")
        calls = self.client.open.call_args_list
        self.assertEqual(len(calls), 4)
        login, first, last = [call.args[0] for call in calls[1:]]
        self.assertEqual(login.get_header("X-csrftoken"), "before-login")
        for request, item in ((first, 17), (last, 41)):
            self.assertEqual(request.get_header("X-csrftoken"), "authenticated")
            self.assertEqual(json.loads(request.data), {"item_id": item})

    def test_success_responses_with_missing_outbox_event_fail_reconciliation(self):
        success = response(b'{"status":"success","event_type":"raffle.entry.accepted.v1"}')
        with self.assertRaisesRegex(RuntimeError, "2 acknowledged applies, 2 entries, 1 outbox"):
            self.run_one_user(success, counts="2\t1\t1")
