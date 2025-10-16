DELETE FROM accounts;

INSERT INTO accounts (id, balance)
SELECT generate_series(1, 1000), 100;
