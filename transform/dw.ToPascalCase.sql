CREATE OR ALTER FUNCTION dw.ToPascalCase (@text NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);

    SELECT @result =
        STRING_AGG(
            UPPER(LEFT(value,1)) +
            LOWER(SUBSTRING(value,2,LEN(value))),
            ' '
        )
    FROM STRING_SPLIT(LTRIM(RTRIM(@text)), ' ')
    WHERE value <> '';

    RETURN @result;
END;
GO