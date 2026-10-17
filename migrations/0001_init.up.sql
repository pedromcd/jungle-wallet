-- Money is stored as BIGINT minor units (cents). No floating point anywhere.

-- ---------------------------------------------------------------- wallets
CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL,
    balance_minor BIGINT      NOT NULL,
    version       BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive     CHECK (version >= 1),
    CONSTRAINT wallets_currency_format      CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wallets_player_currency_key  UNIQUE (player_id, currency)
);

-- Identity is immutable; the version moves by exactly +1 iff the balance changed.
CREATE FUNCTION wallets_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = '23514';
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'a balance change must bump the version by exactly 1' USING ERRCODE = '23514';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'the version only changes together with the balance' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER wallets_guard_trg BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();

-- ------------------------------------------------------ wager_transactions
CREATE TABLE wager_transactions (
    id                                UUID        PRIMARY KEY,
    origin                            TEXT        NOT NULL,
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      CHAR(64),
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    round_id                          TEXT,
    game_id                           TEXT,
    kind                              TEXT        NOT NULL,
    amount_minor                      BIGINT      NOT NULL,
    currency                          CHAR(3)     NOT NULL,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID        REFERENCES wager_transactions (id),
    status                            TEXT        NOT NULL,
    failure_code                      TEXT,
    result_balance_minor              BIGINT,
    attempts                          INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at                   TIMESTAMPTZ,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,

    CONSTRAINT wtx_origin_values CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    CONSTRAINT wtx_kind_values   CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT wtx_status_values CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    -- zero policy: LOSS is exactly zero, every other kind is strictly positive
    CONSTRAINT wtx_amount_rule CHECK ((kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),
    CONSTRAINT wtx_result_non_negative CHECK (result_balance_minor IS NULL OR result_balance_minor >= 0),
    -- the schema distinguishes internal (OPENING) from external operations
    CONSTRAINT wtx_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wtx_reference_rule CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
        OR (kind IN ('BET', 'LOSS', 'OPENING') AND reference_external_transaction_id IS NULL)
        OR kind = 'WIN'
    ),
    CONSTRAINT wtx_status_shape CHECK (
        (status = 'PROCESSED' AND failure_code IS NULL AND result_balance_minor IS NOT NULL)
        OR (status = 'REJECTED' AND failure_code IS NOT NULL AND result_balance_minor IS NOT NULL)
        OR (status = 'FAILED' AND failure_code IS NOT NULL)
        OR (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL)
    ),
    CONSTRAINT wtx_pending_reference_shape CHECK (
        status <> 'PENDING_REFERENCE'
        OR (next_attempt_at IS NOT NULL AND reference_external_transaction_id IS NOT NULL)
    ),
    -- idempotency, persisted: survives any restart
    CONSTRAINT wtx_provider_key_uq      UNIQUE (provider_id, idempotency_key),
    CONSTRAINT wtx_provider_external_uq UNIQUE (provider_id, external_transaction_id)
);

-- a single initial credit per wallet
CREATE UNIQUE INDEX wtx_one_opening_per_wallet_uq ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- a transaction can be successfully reversed at most once (REFUND xor ROLLBACK)
CREATE UNIQUE INDEX wtx_one_reversal_uq ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';
CREATE INDEX wtx_pending_idx ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wtx_wallet_idx  ON wager_transactions (wallet_id);

-- Terminal states are final; identity columns never change; rows are never deleted.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager_transactions rows cannot be deleted' USING ERRCODE = '23514';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is terminal (%) and immutable', OLD.id, OLD.status USING ERRCODE = '23514';
    END IF;
    IF NEW.id <> OLD.id OR NEW.wallet_id <> OLD.wallet_id OR NEW.kind <> OLD.kind
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id THEN
        RAISE EXCEPTION 'transaction identity is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER wager_transactions_guard_trg BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

-- ------------------------------------------------------ wallet_ledger_entries
CREATE TABLE wallet_ledger_entries (
    seq                  BIGINT      GENERATED ALWAYS AS IDENTITY,
    id                   UUID        PRIMARY KEY,
    wallet_id            UUID        NOT NULL REFERENCES wallets (id),
    transaction_id       UUID        NOT NULL REFERENCES wager_transactions (id),
    direction            TEXT        NOT NULL,
    amount_minor         BIGINT      NOT NULL,
    balance_before_minor BIGINT      NOT NULL,
    balance_after_minor  BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    CONSTRAINT ledger_direction_values CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ledger_amount_positive  CHECK (amount_minor > 0),
    CONSTRAINT ledger_balances_non_negative CHECK (balance_before_minor >= 0 AND balance_after_minor >= 0),
    CONSTRAINT ledger_arithmetic CHECK (
        (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor) OR
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)),
    CONSTRAINT ledger_wallet_transaction_uq UNIQUE (wallet_id, transaction_id)
);
CREATE UNIQUE INDEX ledger_seq_uq ON wallet_ledger_entries (seq);
CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq);

-- Append-only: corrections require new entries.
CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% rejected)', TG_OP USING ERRCODE = '23514';
END $$;

CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

-- ---------------------------------------------------------------- outbox
CREATE TABLE outbox_events (
    seq             BIGINT      GENERATED ALWAYS AS IDENTITY,
    event_id        UUID        PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ
);
CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;

-- The payload is an immutable snapshot; only delivery bookkeeping may change.
CREATE FUNCTION outbox_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id <> OLD.event_id OR NEW.aggregate_id <> OLD.aggregate_id
       OR NEW.event_type <> OLD.event_type OR NEW.occurred_at <> OLD.occurred_at
       OR NEW.payload IS DISTINCT FROM OLD.payload THEN
        RAISE EXCEPTION 'outbox events are immutable snapshots' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER outbox_guard_trg BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();

-- ----------------------------------------------------------------- inbox
CREATE TABLE inbox_messages (
    consumer_name TEXT        NOT NULL,
    message_id    TEXT        NOT NULL,
    payload_hash  CHAR(64)    NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);
