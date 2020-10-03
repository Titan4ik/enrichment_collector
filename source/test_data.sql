CREATE TABLE relationship_in_tables (
    table1_name VARCHAR(64) NOT NULL,
    column_from_table1 VARCHAR(64) NOT NULL,
    table2_name VARCHAR(64) NOT NULL,
    column_from_table2 VARCHAR(64) NOT NULL,
    primary key(table1_name, column_from_table1, table2_name, column_from_table2)
);

CREATE TABLE type_columns_in_tables (
    table_name VARCHAR(64) NOT NULL,
    column_name VARCHAR(64) NOT NULL,
    column_type VARCHAR(64) NOT NULL,
    primary key(table_name, column_name, column_type)
);

CREATE TABLE select_request_log (
    table_name VARCHAR(64) NOT NULL,
    column_name VARCHAR(64) NOT NULL,
    column_value VARCHAR(64) NOT NULL,
    request_time TIMESTAMP
);


CREATE TABLE peoples (
    name VARCHAR(256) NOT NULL,
    telephone VARCHAR(11) NOT NULL primary key,
    email VARCHAR(128) NOT NULL,
    date_of_birth DATE NOT NULL
);

CREATE TABLE user_in_social (
    telephone VARCHAR(11) NOT NULL primary key,
    facebook_id INTEGER NOT NULL,
    vk_id INTEGER NOT NULL
);

CREATE TABLE vk_posts (
    vk_id INTEGER NOT NULL,
    post_url VARCHAR(256) NOT NULL
);

CREATE TABLE facebook_posts (
    facebook_id INTEGER NOT NULL,
    post_url VARCHAR(256) NOT NULL
);

CREATE TABLE mail (
    email_from VARCHAR(128) NOT NULL,
    email_to VARCHAR(128) NOT NULL,
    msg VARCHAR(256) NOT NULL
);



INSERT INTO peoples (name, telephone, email, date_of_birth)
VALUES
    ('aleks', '79992254552', 'sasha@mail.ru', '1998-04-29'),
    ('vasya', '79992254444', 'vasya@mail.ru', '2000-05-10'),
    ('katya', '79992254441', 'katya@mail.ru', '2001-07-15'),
    ('lena', '79992253333', 'lena@mail.ru', '2002-06-12')

INSERT INTO user_in_social (telephone, facebook_id, vk_id)
VALUES
    ('79992254552', 0, 0),
    ('79992254444', 1, 1),
    ('79992254441', 3, 10),
    ('79992253333', 4, 222)

INSERT INTO vk_posts (vk_id, post_url)
VALUES
    (0, 'url1'),
    (0, 'url2'),
    (0, 'url3'),
    (0, 'url5'),
    (1, 'url11'),
    (1, 'url12'),
    (1, 'url13'),
    (1, 'url15'),
    (5, 'url131'),
    (5, 'url151'),
    (3, 'url1312'),
    (3, 'url1512')

INSERT INTO mail (email_from, email_to, msg)
VALUES
    ('sasha@mail.ru', 'vasya@mail.ru', 'hello'),
    ('vasya@mail.ru', 'sasha@mail.ru', 'hello'),
    ('sasha@mail.ru', 'vasya@mail.ru', 'go in dota2'),
    ('vasya@mail.ru', 'sasha@mail.ru', 'go'),
    ('hehi@mail.ru', 'don@mail.ru', 'go in weesssese'),
    ('don@mail.ru', 'katya@mail.ru', 'go')


