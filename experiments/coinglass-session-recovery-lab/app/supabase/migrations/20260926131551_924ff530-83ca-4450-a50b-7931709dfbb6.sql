CREATE TABLE public.v2_automated_trade_cards (
  workspace_id uuid NOT NULL REFERENCES public.v2_workspaces(id) ON DELETE CASCADE,
  card_id text PRIMARY KEY,
  revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(payload) = 'object')
    CHECK (octet_length(payload::text) <= 16384)
    CHECK (
      payload - ARRAY[
        'symbol',
        'side',
        'rise_count',
        'fall_count',
        'entry_price',
        'stop_price',
        'take_profit_price',
        'status',
        'quantity_entered',
        'quantity_closed',
        'quantity_remaining',
        'realized_pnl',
        'pnl_verified',
        'closure_verified'
      ] = '{}'::jsonb
    ),
  observed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

GRANT SELECT ON public.v2_automated_trade_cards TO authenticated;
GRANT ALL ON public.v2_automated_trade_cards TO service_role;

ALTER TABLE public.v2_automated_trade_cards ENABLE ROW LEVEL SECURITY;

CREATE POLICY v2_automated_trade_cards_select
ON public.v2_automated_trade_cards
FOR SELECT
TO authenticated
USING (public.v2_is_workspace_member(workspace_id));

CREATE INDEX v2_automated_trade_cards_workspace_observed_idx
ON public.v2_automated_trade_cards (workspace_id, observed_at DESC, card_id);

CREATE TRIGGER v2_automated_trade_cards_updated_at
BEFORE UPDATE ON public.v2_automated_trade_cards
FOR EACH ROW EXECUTE FUNCTION public.v2_set_updated_at();