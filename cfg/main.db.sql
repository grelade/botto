BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS `order` (
	`id`	INTEGER,
	`symbol`	TEXT,
	`type`	TEXT,
	`side`	TEXT,
	`transactTime`	INTEGER,
	`price`	NUMERIC,
	`origQty`	NUMERIC,
	`executedQty`	NUMERIC,
	`status`	TEXT
);
CREATE TABLE IF NOT EXISTS `data_harvester_data` (
	`id`	INTEGER,
	`harvester_id`	INTEGER,
	`trade_id`	INTEGER,
	`buyer_id`	INTEGER,
	`seller_id`	INTEGER,
	`timestamp`	FLOAT,
	`price`	FLOAT,
	`quantity`	FLOAT,
	PRIMARY KEY(`id`)
);
CREATE TABLE IF NOT EXISTS `data_harvester` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT,
	`symbol`	REAL,
	`active`	BOOLEAN,
	`running`	BOOLEAN
);
CREATE TABLE IF NOT EXISTS `config` (
	`id`	INTEGER,
	`module`	TEXT,
	`cfg`	JSON,
	PRIMARY KEY(`id`)
);
CREATE TABLE IF NOT EXISTS `coin` (
	`id`	INTEGER,
	`symbol`	TEXT
);
CREATE TABLE IF NOT EXISTS `agent` (
	`id`	INTEGER PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT,
	`init_order_id`	INTEGER,
	`harvester_id`	INTEGER,
	`type`	TEXT,
	`params`	TEXT,
	`active`	BOOLEAN,
	`running`	BOOLEAN
);
COMMIT;
