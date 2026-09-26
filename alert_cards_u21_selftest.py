"""The U21 source adapter stays data-only and preserves the frozen signal."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import Mock, patch

import alert_cards_forwarder as forwarder
import alert_cards_wire as wire

SCOPE = 'a' * 64
FIRST = '1' * 32
SECOND = '2' * 32


def delivered(identity=FIRST, *, price=1.234, entry=None):
    entry = entry or datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc)
    decision = entry-timedelta(minutes=1)
    stop, take = price*1.005, price*.92
    iso = lambda at: at.isoformat()
    return dict(intent_id=identity, status='DELIVERED',
        created_at=iso(entry+timedelta(seconds=2)),
        acknowledged_at=iso(entry+timedelta(seconds=3)),
        expires_at=iso(entry+timedelta(seconds=90)), message_id=93295,
        payload=dict(position_id='3'*32,rule_id='U21',symbol='XRP',direction='SHORT',
            decision_at=iso(decision),entry_at=iso(entry),entry_price=price,
            stop_price=stop,take_price=take,entry_price_decimal=str(price),
            stop_price_decimal=str(stop),take_price_decimal=str(take)),
        text=("🧪 <b>U21 · XRP · SHORT — ניסיונית, ללא חפיפה</b>\n"
              f"מחיר ייחוס לכניסה: <b>{price:.8g}</b>\n"
              f"זמן הייחוס: {entry.strftime('%Y-%m-%d %H:%M')} UTC\n"
              f"סטופ: <b>{stop:.8g}</b> (+0.5%)\n"
              f"טייק: <b>{take:.8g}</b> (−8%) · יחס 1:16\n"
              "זוהי התראה בלבד; מחיר הייחוס אינו אישור ביצוע עסקה."))


class U21SourceTests(unittest.TestCase):
    def value(self, item=None):
        return wire.u21_delivery(item or delivered(), SCOPE, wire.U21_CONFIG)

    def test_frozen_prices_and_original_expiry_survive_display_rounding(self):
        value = self.value()
        spec = wire.normalize(value)
        self.assertEqual(value['stop'], '1.2401699999999998')
        self.assertIn('1.24017', value['text'])
        self.assertEqual(spec['signal']['stop'], value['stop'])
        self.assertEqual(spec['threshold_pct'], None)
        self.assertEqual(spec['source_expires_at'], value['expires_at'])
        self.assertEqual(spec['signal']['at'], value['source_at'])

    def test_two_notifications_with_same_content_keep_different_identity(self):
        first=self.value()
        second=self.value(delivered(SECOND))
        self.assertNotEqual(wire.normalize(first)['signal']['event_id'],
                            wire.normalize(second)['signal']['event_id'])
        self.assertEqual(wire.normalize(first), wire.normalize(deepcopy(first)))

    def test_changes_to_prices_identity_direction_or_clock_fail_closed(self):
        value=self.value()
        variants=[('stop','1.24017'),('side','LONG'),('source_at','2026-01-01T12:01:01+00:00'),
                  ('expires_at','2026-01-01T12:05:00+00:00'),('message_id',None),
                  ('rule_id','other'),('config_version','other')]
        for field,replacement in variants:
            with self.subTest(field=field):
                altered=deepcopy(value);altered[field]=replacement
                with self.assertRaises(wire.WireError):wire.normalize(altered)
        altered=deepcopy(value);altered['text']=altered['text'].replace('1.24017','1.24018')
        with self.assertRaises(wire.WireError):wire.normalize(altered)

    def test_source_float_must_match_its_own_frozen_decimal(self):
        item=delivered();item['payload']['stop_price_decimal']='1.24017'
        with self.assertRaises(wire.WireError):self.value(item)

    def test_only_delivered_outbox_intent_is_read(self):
        now=datetime.now(timezone.utc)
        entry=now.replace(minute=now.minute//15*15,second=0,microsecond=0)+timedelta(minutes=1)
        item=delivered(entry=entry)
        pending=deepcopy(item);pending['status']='UNKNOWN';pending['intent_id']=SECOND
        state=dict(version='u21-xrp-experimental-cap1-v1',config_version=wire.U21_CONFIG,
                   intents=[item,pending])
        def query(sql, params):
            response=Mock()
            response.fetchone.return_value=({'value':__import__('json').dumps(state)}
                if params[0].startswith('u21-xrp-') else None)
            response.fetchall.return_value=[]
            return response
        conn=Mock();conn.execute.side_effect=query
        context=Mock();context.__enter__=Mock(return_value=conn)
        context.__exit__=Mock(return_value=False)
        with patch('psycopg.connect',return_value=context),patch.object(forwarder,'_source_dsn',return_value='unused'):
            rows=forwarder.read_delivered((SCOPE,),datetime(2026,9,17,tzinfo=timezone.utc))
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['intent_id'],item['intent_id'])
        import hashlib
        expected='u21-xrp-experimental-cap1-v1:'+hashlib.sha256(('general-watch:'+SCOPE).encode()).hexdigest()
        self.assertIn(expected,[call.args[1][0] for call in conn.execute.call_args_list])


if __name__=='__main__':unittest.main()
