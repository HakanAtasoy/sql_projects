-- Create a new user
create user new_user with password 'password';

-- Connect to the database with the new user 
-- Try to query cancelled orders
select 
	* 
from 
	orders 
where 
	status = 'CANCELLED';

-- Get the error "SQL Error [42501]: ERROR: permission denied for table orders"

-- Connect to database with original credentials (superuser account that has permissions)
-- Give permission to new user 
grant select on orders to new_user

-- Check to see the permissions users have on orders table and confirm new_user has select permission
select 
	* 
from 
	information_schema.table_privileges 
where 
	table_name = 'orders';

-- Connect ot the database with new_user and try again
select 
	* 
from 
	orders 
where 
	status = 'CANCELLED';

-- This time we get the results.

