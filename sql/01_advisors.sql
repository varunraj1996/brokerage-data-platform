-- Brokerage source system (Azure SQL Database: brokerage_oltp)
-- 01: schema + advisors

-- A schema is a named folder for tables inside the database.
CREATE SCHEMA brokerage;
GO

-- One row per financial advisor.
CREATE TABLE brokerage.advisors (
    advisor_id    INT           NOT NULL PRIMARY KEY,   -- unique ID for each advisor
    full_name     NVARCHAR(100) NOT NULL,
    email         NVARCHAR(200) NOT NULL UNIQUE,        -- no two advisors share an email
    team          NVARCHAR(50)  NOT NULL,               -- e.g. 'Boston East'
    hired_date    DATE          NOT NULL,
    updated_at    DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()  -- last change (UTC)
);
GO