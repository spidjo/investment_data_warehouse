USE investment_dw;
GO 


DECLARE @results TABLE (
    test_name NVARCHAR(100),
    passed BIT,
    error_message NVARCHAR(4000),
    run_at DATETIME DEFAULT GETDATE()
);

BEGIN TRY
    -- Test 1: v_client_profile - Not Null Check
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_client_profile_not_null',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM gold.v_client_profile
    WHERE client_id IS NULL OR first_name IS NULL OR last_name IS NULL;

    -- Test 2: v_client_profile - Risk Rating Validity
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_client_profile_risk_rating',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM gold.v_client_profile
    WHERE risk_rating NOT IN ('Low', 'Medium', 'High');

    -- Test 3: v_account_summary - Investment Value Positive
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_account_summary_positive_investment_value',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM gold.v_account_summary
    WHERE total_investment_value < 0;

    -- Test 4: v_client_investment_summary - Match with Account Summary
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_client_investment_summary_total_match',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM (
        SELECT c.client_id
        FROM gold.v_client_investment_summary c
        JOIN (
            SELECT client_id, SUM(total_investment_value) AS sum_value
            FROM gold.v_account_summary
            GROUP BY client_id
        ) a ON c.client_id = a.client_id
        WHERE c.total_investment <> a.sum_value
    ) diff;

    -- Test 5: v_security_volume_trends - Volume Positive
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_security_volume_trends_positive_volume',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM gold.v_security_volume_trends
    WHERE avg_daily_volume < 0;

    -- Test 6: v_daily_aum_snapshot - Non-null Snapshot Date
    INSERT INTO @results (test_name, passed, error_message)
    SELECT 'v_daily_aum_snapshot_snapshot_date',
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END,
            NULL
    FROM gold.v_daily_aum_snapshot
    WHERE snapshot_date IS NULL;

END TRY
BEGIN CATCH
    INSERT INTO @results (test_name, passed, error_message)
    VALUES ('suite_error', 0, ERROR_MESSAGE());
END CATCH

SELECT * FROM @results;
