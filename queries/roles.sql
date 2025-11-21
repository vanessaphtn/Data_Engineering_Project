-- roles
CREATE ROLE analyst_full;
CREATE ROLE analyst_limited;

-- users
CREATE USER user_full IDENTIFIED BY 'full';
CREATE USER user_limited IDENTIFIED BY 'limited';

-- match user with a role
GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;
