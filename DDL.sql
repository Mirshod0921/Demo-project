--DDL

---- Link cards.user_id to users.id

ALTER TABLE cards
ADD CONSTRAINT fk_cards_user
FOREIGN KEY (user_id) REFERENCES users(id);

-- Link transactions.from_card_id and to_card_id to cards.id
ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_from_card
FOREIGN KEY (from_card_id) REFERENCES cards(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_to_card
FOREIGN KEY (to_card_id) REFERENCES cards(id);

-- Link logs.transaction_id to transactions.id
ALTER TABLE logs
ADD CONSTRAINT fk_logs_transaction
FOREIGN KEY (transaction_id) REFERENCES transactions(id);

-- Link scheduled_payments.user_id to users.id and card_id to cards.id
ALTER TABLE scheduled_payments
ADD CONSTRAINT fk_scheduled_user
FOREIGN KEY (user_id) REFERENCES users(id);

ALTER TABLE scheduled_payments
ADD CONSTRAINT fk_scheduled_card
FOREIGN KEY (card_id) REFERENCES cards(id);


CREATE TABLE retrieveinfo (
    retrieve_id INT IDENTITY(1,1) PRIMARY KEY,
    source_file VARCHAR(255),
    retrieved_at DATETIME,
    total_rows INT,
    processed_rows INT,
    errors INT,
    notes TEXT
);