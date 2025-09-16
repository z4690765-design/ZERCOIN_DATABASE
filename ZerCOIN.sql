CREATE DATABASE ZerCOIN;
GO

USE ZerCOIN;
GO

CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(50) NOT NULL UNIQUE,
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    email NVARCHAR(100) UNIQUE,
    password_hash NVARCHAR(200),
    status NVARCHAR(20) DEFAULT 'active'
);
GO

CREATE TABLE wallets (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    address NVARCHAR(100) NOT NULL UNIQUE,
    balance DECIMAL(18,8) DEFAULT 0 CHECK (balance >= 0),
    CONSTRAINT fk_wallets_users FOREIGN KEY (user_id) REFERENCES users(id)
);
GO

CREATE TABLE transactions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    from_wallet INT NULL,
    to_wallet INT NULL,
    amount DECIMAL(18,8) NOT NULL CHECK (amount > 0),
    status NVARCHAR(20) DEFAULT 'pending',
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    type NVARCHAR(20) DEFAULT 'transfer',
    CONSTRAINT fk_tx_from_wallet FOREIGN KEY (from_wallet) REFERENCES wallets(id),
    CONSTRAINT fk_tx_to_wallet FOREIGN KEY (to_wallet) REFERENCES wallets(id)
);
GO

CREATE TABLE audit_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    action NVARCHAR(100),
    info NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT SYSDATETIME()
);
GO

CREATE OR ALTER VIEW vw_wallets_overview
AS
SELECT w.id AS wallet_id, w.address, u.username, w.balance
FROM wallets w
LEFT JOIN users u ON w.user_id = u.id;
GO

CREATE OR ALTER VIEW vw_user_wallets_transactions
AS
SELECT 
    u.id AS user_id,
    u.username,
    u.email,
    u.status,
    w.id AS wallet_id,
    w.address,
    w.balance,
    t.id AS last_tx_id,
    t.type AS last_tx_type,
    t.amount AS last_tx_amount,
    t.status AS last_tx_status,
    t.created_at AS last_tx_date
FROM users u
LEFT JOIN wallets w ON u.id = w.user_id
LEFT JOIN (
    SELECT t1.*
    FROM transactions t1
    WHERE t1.id = (
        SELECT TOP 1 t2.id 
        FROM transactions t2
        WHERE t2.from_wallet = t1.from_wallet OR t2.to_wallet = t1.to_wallet
        ORDER BY t2.created_at DESC
    )
) t ON t.from_wallet = w.id OR t.to_wallet = w.id;
GO

CREATE OR ALTER PROCEDURE sp_transfer_simple
    @from_wallet INT,
    @to_wallet INT,
    @amount DECIMAL(18,8)
AS
BEGIN
    SET NOCOUNT ON;
    IF @amount <= 0 THROW 60000,'Amount must be positive',1;
    BEGIN TRAN ZerCoinTran;
    BEGIN TRY
        IF NOT EXISTS(SELECT 1 FROM wallets WHERE id = @from_wallet) THROW 60001,'From wallet not found',1;
        IF NOT EXISTS(SELECT 1 FROM wallets WHERE id = @to_wallet) THROW 60002,'To wallet not found',1;
        IF (SELECT balance FROM wallets WHERE id = @from_wallet) < @amount THROW 60003,'Insufficient funds',1;

        UPDATE wallets SET balance = balance - @amount WHERE id = @from_wallet;
        UPDATE wallets SET balance = balance + @amount WHERE id = @to_wallet;

        INSERT INTO transactions (from_wallet,to_wallet,amount,status,type) 
        VALUES (@from_wallet,@to_wallet,@amount,'confirmed','transfer');

        COMMIT TRAN ZerCoinTran;
        SELECT 'OK' AS result;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN ZerCoinTran;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE sp_deposit
    @wallet_id INT,
    @amount DECIMAL(18,8)
AS
BEGIN
    SET NOCOUNT ON;
    IF @amount <= 0 THROW 60004,'Amount must be positive',1;

    UPDATE wallets SET balance = balance + @amount WHERE id = @wallet_id;

    INSERT INTO transactions (to_wallet,amount,status,type) 
    VALUES (@wallet_id,@amount,'confirmed','deposit');
END;
GO

CREATE OR ALTER PROCEDURE sp_withdraw
    @wallet_id INT,
    @amount DECIMAL(18,8)
AS
BEGIN
    SET NOCOUNT ON;
    IF @amount <= 0 THROW 60005,'Amount must be positive',1;
    IF (SELECT balance FROM wallets WHERE id = @wallet_id) < @amount 
        THROW 60006,'Insufficient funds',1;

    UPDATE wallets SET balance = balance - @amount WHERE id = @wallet_id;

    INSERT INTO transactions (from_wallet,amount,status,type) 
    VALUES (@wallet_id,@amount,'confirmed','withdraw');
END;
GO

CREATE OR ALTER TRIGGER trg_after_insert_transactions
ON transactions
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO audit_log (action, info)
    SELECT 'transaction_insert',
           CONCAT('tx_id=', id,
                  ',from=', ISNULL(CONVERT(NVARCHAR(20), from_wallet),'NULL'),
                  ',to=', ISNULL(CONVERT(NVARCHAR(20), to_wallet),'NULL'),
                  ',amount=', CONVERT(NVARCHAR(50), amount))
    FROM inserted;
END;
GO

CREATE OR ALTER TRIGGER trg_update_wallets
ON wallets
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO audit_log (action, info)
    SELECT 'wallet_update',
           CONCAT('wallet_id=', i.id, 
                  ',old_balance=', d.balance, 
                  ',new_balance=', i.balance)
    FROM inserted i
    JOIN deleted d ON i.id = d.id;
END;
GO

CREATE INDEX idx_wallet_user ON wallets(user_id);
CREATE INDEX idx_tx_from_wallet ON transactions(from_wallet);
CREATE INDEX idx_tx_to_wallet ON transactions(to_wallet);
GO

INSERT INTO users (username,email,password_hash) 
VALUES ('Fares','faris@mail.com','HASHED_PASS1'),
       ('Zeref','zeref7780@mail.com','HASHED_PASS2');
GO

INSERT INTO wallets (user_id,address,balance) 
VALUES (1,'WALLET_ALICE',1000.00),
       (2,'WALLET_BOB',100.00);
GO

EXEC sp_transfer_simple @from_wallet = 1, @to_wallet = 2, @amount = 150.00;
EXEC sp_deposit @wallet_id = 2, @amount = 50.00;
EXEC sp_withdraw @wallet_id = 1, @amount = 100.00;
GO

SELECT * FROM vw_wallets_overview;
SELECT * FROM vw_user_wallets_transactions;
SELECT * FROM transactions;
SELECT * FROM audit_log;
GO
