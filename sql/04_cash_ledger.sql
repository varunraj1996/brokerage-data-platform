-- 04 · Cash ledger: every deposit and withdrawal, one row per cash movement.
-- Trades also move cash, but they live in the trade stream; this table holds only money in and out of an account.
-- Amounts are DECIMAL, never FLOAT: money must add up to the exact cent.

CREATE TABLE brokerage.cash_ledger (
    cash_txn_id     BIGINT        NOT NULL CONSTRAINT pk_cash_ledger PRIMARY KEY,
    account_id      INT           NOT NULL CONSTRAINT fk_cash_ledger_account REFERENCES brokerage.accounts (account_id),
    txn_type        VARCHAR(12)   NOT NULL CONSTRAINT ck_cash_ledger_type CHECK (txn_type IN ('DEPOSIT', 'WITHDRAWAL')),
    amount          DECIMAL(14,2) NOT NULL CONSTRAINT ck_cash_ledger_amount CHECK (amount > 0),
    effective_date  DATE          NOT NULL,
    created_at      DATETIME2     NOT NULL CONSTRAINT df_cash_ledger_created DEFAULT SYSUTCDATETIME()
);

-- Most questions are "this account's cash over time", so index that path.
CREATE INDEX ix_cash_ledger_account_date ON brokerage.cash_ledger (account_id, effective_date);
