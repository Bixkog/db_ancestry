CREATE TABLE users(
	userId numeric PRIMARY KEY NOT NULL,
	data varchar(100),
	password varchar(20) NOT NULL 	
);	

CREATE TABLE ancestry(
	ancestorId numeric NOT NULL,
	descendantId numeric NOT NULL
);

ALTER TABLE ancestry ADD CONSTRAINT ancestor_userid FOREIGN KEY (ancestorId)
    REFERENCES users(userId);

ALTER TABLE ancestry ADD CONSTRAINT descendant_userid FOREIGN KEY (descendantId)
    REFERENCES users(userId);

CREATE USER app PASSWORD 'qwerty'; 