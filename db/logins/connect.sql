-- Lets only :"login" (and the database's owner) connect to :"db".
REVOKE CONNECT ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO :"login";
