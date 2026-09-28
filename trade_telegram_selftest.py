"""Read-only Testnet Telegram report contracts; no exchange calls."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import sys
import types
import unittest

# The production service installs python-telegram-bot. The isolated journal CI
# tests can run without that transport dependency.
try:
    import telegram
except ImportError:
    telegram = types.ModuleType('telegram')
    telegram.InlineKeyboardButton = lambda text, callback_data: SimpleNamespace(
        text=text, callback_data=callback_data)
    telegram.InlineKeyboardMarkup = lambda buttons: SimpleNamespace(inline_keyboard=buttons)
    ext = types.ModuleType('telegram.ext')
    ext.CommandHandler = lambda *args: args
    ext.CallbackQueryHandler = lambda *args, **kwargs: (args,kwargs)
    sys.modules['telegram'] = telegram
    sys.modules['telegram.ext'] = ext

import trade_telegram as t


def ms(iso):
    return int(datetime.fromisoformat(iso).timestamp()*1000)


def row(card_id, entry, exit_at=None, *, closed=False):
    return dict(card_id=card_id, symbol='ETH', account_role='long_account',
        first_entry_at_ms=ms(entry), last_exit_at_ms=ms(exit_at) if exit_at else None,
        evidence_at_ms=ms(exit_at or entry), entry_quantity='0.2318',
        exit_quantity='0.2318' if exit_at else '0',
        remaining_quantity='0' if exit_at else '0.2318',
        actual_entry_price='2716.4', actual_exit_price='2689.2' if exit_at else None,
        planned_prices={'entry':'2716.4','take_profit':'2689.2','stop':'2743.6'},
        active_order_ids=[], protection_verified=False, issues=[], closure_verified=closed)


class Reports(unittest.TestCase):
    def setUp(self):
        self.open = row('a'*64, '2026-09-27T21:30:00+00:00')
        self.closed = row('b'*64, '2026-09-27T22:30:00+00:00',
                          '2026-09-28T09:00:00+00:00', closed=True)
        self.unverified = row('c'*64, '2026-09-28T08:00:00+00:00',
                              '2026-09-28T09:10:00+00:00', closed=False)
        self.rows = {'L':[self.open,self.closed,self.unverified], 'S':[]}

    def test_private_allowlist_fail_closed(self):
        u = SimpleNamespace(effective_user=SimpleNamespace(id=42),
                            effective_chat=SimpleNamespace(id=42,type='private'))
        self.assertFalse(t.authorized(u, {}))
        self.assertFalse(t.authorized(u, {'HL_TESTNET_REPORT_TELEGRAM_USER_ID':'43'}))
        self.assertTrue(t.authorized(u, {'HL_TESTNET_REPORT_TELEGRAM_USER_ID':'42'}))
        u.effective_chat.type='group'
        self.assertFalse(t.authorized(u, {'HL_TESTNET_REPORT_TELEGRAM_USER_ID':'42'}))

    def test_filters_keep_unverified_closure_out_of_closed(self):
        self.assertEqual([r['card_id'] for _,r in t.selection(self.rows,'A','closed')],
                         [self.closed['card_id']])
        self.assertEqual({r['card_id'] for _,r in t.selection(self.rows,'A','open')},
                         {self.open['card_id'],self.unverified['card_id']})
        text, buttons=t.list_view(self.rows,'L','all')
        self.assertIn('LONG',text)
        self.assertEqual(len(buttons.inline_keyboard),4)
        self.assertIn('סגירה בבדיקה',t.detail(self.rows,'L','c'*16))
        self.unverified['remaining_quantity']='0.0000'
        self.assertEqual(t.status(self.unverified),'סגירה בבדיקה')
        self.assertIn('בדיקה',t.list_view(self.rows,'L','all')[0])

    def test_daily_uses_israel_day_and_both_account_sections(self):
        text,_=t.daily_view(self.rows,datetime(2026,9,28).date())
        self.assertIn('3 עסקאות',text)
        self.assertIn('חשבון LONG',text)
        self.assertIn('חשבון SHORT',text)
        self.assertIn('2689.2',text)
        self.assertIn('סגירה בבדיקה',text)

    def test_private_https_report_requires_token_and_expected_roles(self):
        class Response:
            status=200
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def read(self,n): return b'{"L":[],"S":[]}'
        with self.assertRaises(ValueError):
            t.load_trades({})
        with patch.object(t,'urlopen',return_value=Response()) as fetch:
            self.assertEqual(t.load_trades({'HL_TESTNET_REPORT_API_TOKEN':'y'*40}),
                             {'L':[],'S':[]})
            req=fetch.call_args.args[0]
            self.assertEqual(req.full_url,t.REPORT_URL)
            self.assertEqual(req.get_header('X-trade-report-token'),'y'*40)


if __name__=='__main__':
    unittest.main()
