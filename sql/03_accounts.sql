-- Brokerage source system (Azure SQL Database: brokerage_oltp)
-- 03: accounts. One client can hold many accounts.

CREATE TABLE brokerage.accounts (
    account_id      INT           NOT NULL PRIMARY KEY,
    account_number  CHAR(10)      NOT NULL UNIQUE,      -- shown to clients (PII: masked later)
    client_id       INT           NOT NULL
        REFERENCES brokerage.clients(client_id),        -- must belong to a real client
    account_type    NVARCHAR(20)  NOT NULL
        CHECK (account_type IN ('Individual','Joint','IRA','Roth IRA')),
    status          NVARCHAR(10)  NOT NULL
        CHECK (status IN ('Open','Closed')),
    opened_date     DATE          NOT NULL,
    closed_date     DATE          NULL,                 -- empty while the account is open
    updated_at      DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),

    -- Business rules enforced by the database itself:
    CONSTRAINT ck_accounts_dates  CHECK (closed_date IS NULL OR closed_date >= opened_date),
    CONSTRAINT ck_accounts_status CHECK (
        (status = 'Open'   AND closed_date IS NULL) OR
        (status = 'Closed' AND closed_date IS NOT NULL)
    )
);
GO