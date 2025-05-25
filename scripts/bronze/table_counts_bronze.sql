

    -- Check the number of records in each table
    -- to confirm that the data was inserted successfully
    PRINT '-------------------------------------------------';
    PRINT 'Checking record counts in bronze schema tables...';
    PRINT '-------------------------------------------------';

    DECLARE @table_name NVARCHAR(128),
            @schema_name NVARCHAR(128),
            @sql NVARCHAR(MAX);

    SET @schema_name = 'bronze';
    DECLARE table_cursor CURSOR FOR
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = @schema_name;

    OPEN table_cursor;

    FETCH NEXT FROM table_cursor INTO @table_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'SELECT ''' + @table_name + ''' AS TableName, COUNT(*) AS RecordCount FROM ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + ';';
        PRINT @schema_name + '.' + @table_name;
        PRINT '-------------------------------------------------';
        PRINT @sql;
        EXEC sp_executesql @sql;

        FETCH NEXT FROM table_cursor INTO @table_name;
    END;

    CLOSE table_cursor;
    DEALLOCATE table_cursor; 