CREATE TABLE users(
	userId numeric PRIMARY KEY NOT NULL,
	data varchar(100),
	password varchar(100) NOT NULL 	
);	

CREATE TABLE ancestry(
	ancestorId numeric NOT NULL,
	descendantId numeric NOT NULL
);

ALTER TABLE ancestry ADD CONSTRAINT ancestor_userid FOREIGN KEY (ancestorId)
    REFERENCES users(userId) ON DELETE CASCADE;

ALTER TABLE ancestry ADD CONSTRAINT descendant_userid FOREIGN KEY (descendantId)
    REFERENCES users(userId) ON DELETE CASCADE;

-- CREATE USER app PASSWORD 'qwerty' SUPERUSER; 