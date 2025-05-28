/*
    Description: Procedure to insert a snapshot of the AUM data into the sp_gold_load_aum_snapshot table.
                This Procedure takes 1 parameter (snapshot date)
    Warning:    This procedure will delete/clear data for the date you are inserting.
                Ensure that you have backups or that you are okay with losing existing data before running this procedure.
    Example Usage: 
        BEGIN
        DECLARE @date DATE = GETDATE()
            EXEC gold.sp_load_aum_snapshot @snapshot_date = @date;
        END
*/

USE investment_dw;
GO


-- Declare the snapshot date (you can also pass it as a parameter in a stored proc)
CREATE OR ALTER PROCEDURE gold.sp_gold_load_aum_snapshot @snapshot_date DATE AS

--Clear snapshot data for this date if you're inserting fresh data
DELETE FROM gold.tbl_aum_snapshot WHERE snapshot_date = @snapshot_date;

WITH position AS (
    SELECT 
        account_id,
        security_symbol,
        SUM(CASE 
            WHEN transaction_type = 'Buy' THEN quantity
            WHEN transaction_type = 'Sell' THEN -quantity
            ELSE 0 
        END) AS quantity
    FROM gold.v_transaction_fact
    GROUP BY account_id, security_symbol
),
latest_prices AS (
    SELECT 
        symbol,
        closing_price
    FROM gold.v_security_prices
    WHERE market_date = @snapshot_date
)
-- Insert into snapshot table 
INSERT INTO gold.tbl_aum_snapshot (
    snapshot_date,
    account_id,
    security_symbol,
    total_investment_value
)
SELECT 
    @snapshot_date AS snapshot_date,
    p.account_id,
    p.security_symbol,
    p.quantity * lp.closing_price AS total_investment_value
FROM position p
JOIN latest_prices lp 
    ON p.security_symbol = lp.symbol;





