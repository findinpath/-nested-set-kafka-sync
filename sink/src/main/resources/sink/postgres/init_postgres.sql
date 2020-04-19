CREATE TABLE nested_set_node(
    id bigint,
    label varchar(256),
    lft int NOT NULL,
    rgt int NOT NULL,
    active boolean NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);


CREATE TABLE nested_set_node_log(
    id bigserial,
    tree_node_id bigint,
    label varchar(256),
    lft int NOT NULL,
    rgt int NOT NULL,
    active boolean NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);

CREATE TABLE log_offset(
    name varchar(256),
    value bigint,
    primary key (name)
);

INSERT INTO log_offset (name, value) VALUES ('nested_set_node_log', 0);