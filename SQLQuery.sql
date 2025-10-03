--DELETE FROM Users

exec SP_RENAME 'Cards.client_id','Client_id', 'COLUMN'

exec SP_RENAME 'Cards.cvv','CVV', 'COLUMN'

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Cards';

ALTER TABLE Cards
ALTER COLUMN Card_brand nvarchar(20) not null;

ALTER TABLE Cards
ALTER COLUMN Card_type nvarchar(20) not null;

ALTER TABLE Cards
ALTER COLUMN Has_chip nvarchar(10) not null;




-- Step 1: Ensure the temporary column exists
IF NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'Cards' AND COLUMN_NAME = 'Year_pin_last_changed_temp'
)
BEGIN
    ALTER TABLE Cards
    ADD Year_pin_last_changed_temp INTEGER;
END;

-- Step 2: Populate the temporary column with year values
UPDATE Cards
SET Year_pin_last_changed_temp = YEAR(Year_pin_last_changed);

-- Step 3: Verify data
SELECT Year_pin_last_changed, Year_pin_last_changed_temp
FROM Cards;

-- Step 4: Drop the original column
ALTER TABLE Cards
DROP COLUMN Year_pin_last_changed;

-- Step 5: Rename the temporary column
EXEC sp_rename 'Cards.Year_pin_last_changed_temp', 'Year_pin_last_changed', 'COLUMN';

-- Step 6: Add NOT NULL constraint (only if no NULL values exist)
ALTER TABLE Cards
ALTER COLUMN Year_pin_last_changed INTEGER NOT NULL;


ALTER TABLE Cards
ALTER COLUMN Card_number BIGINT;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Cards';

ALTER TABLE Cards
ALTER COLUMN Card_on_dark_web nvarchar(10) not null;

ALTER TABLE Merchants
ALTER COLUMN Business_type nvarchar(100);

ALTER TABLE Merchants
ALTER COLUMN Error nvarchar(100);

ALTER DATABASE [FinancialTransactions]
ADD FILE
(
    NAME = N'CARD_File',
    FILENAME = N'D:\Data Analysis\Projects\Financial Transactions Dataset Analytics\Database\CARD_File.ndf',
    SIZE = 64MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
TO FILEGROUP [CARD];

ALTER DATABASE [FinancialTransactions]
ADD FILE
(
    NAME = N'MERCHANT_File',
    FILENAME = N'D:\Data Analysis\Projects\Financial Transactions Dataset Analytics\Database\MERCHANT_File.ndf',
    SIZE = 64MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
TO FILEGROUP [MERCHANT];

ALTER DATABASE [FinancialTransactions]
ADD FILE
(
    NAME = N'USER_File',
    FILENAME = N'D:\Data Analysis\Projects\Financial Transactions Dataset Analytics\Database\USER_File.ndf',
    SIZE = 64MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
TO FILEGROUP [USER];


ALTER TABLE Merchants
DROP CONSTRAINT PK_Merchants;


ALTER TABLE Merchants
   ADD PK_Id INT IDENTITY
ALTER TABLE Merchants
   ADD CONSTRAINT PK_Merchants
   PRIMARY KEY(PK_Id)

ALTER TABLE Transactions
ADD CONSTRAINT FK_Transactions_Merchants
FOREIGN KEY (merchant_id) REFERENCES Merchants(Id);

ALTER TABLE Transactions
ADD CONSTRAINT FK_Transactions_Cards
FOREIGN KEY (card_id) REFERENCES Cards(Id);

ALTER TABLE Transactions
ADD CONSTRAINT FK_Transactions_Users
FOREIGN KEY (client_id) REFERENCES Users(Id);

ALTER TABLE Cards
ADD CONSTRAINT FK_Cards_Users 
FOREIGN KEY (Client_id) REFERENCES Users(Id);

SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE;