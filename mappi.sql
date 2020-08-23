create database mappi;

CREATE TABLE `mappi`.`devices` (
  `ip_address` varchar(255) NOT NULL,
  `hostname` varchar(255) DEFAULT NULL,
  `last_seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `first_seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ip_address`)
);

--Call this to clear out stale devices
CREATE PROCEDURE `mappi`.`flush_old_devices`()
begin
		DECLARE done INT DEFAULT FALSE;  
		DECLARE v_last_seen datetime;
	    DECLARE v_ip_address varchar(255);
		DECLARE v_hostname varchar(255);
		declare cursor1 cursor for
select c.last_seen, c.ip_address, c.hostname
			from devices a
			join
			(select max(last_seen) last_seen, lower(hostname) hostname from devices group by lower(hostname))b
			on a.last_seen = b.last_seen
			and a.hostname = b.hostname
			right join (select last_seen ,ip_address, lower(hostname) hostname from devices) c
			on c.last_seen < a.last_seen 
			and c.hostname = b.hostname
			where a.last_seen is not null
			order by hostname;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
		open cursor1;
		read_loop: LOOP
			FETCH cursor1 into  v_last_seen,v_ip_address,v_hostname;
		    IF done THEN
      			LEAVE read_loop;
    		END IF;
    		delete from devices where ip_address = v_ip_address;
    	end loop;
    close cursor1;
   end
