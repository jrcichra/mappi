create database mappi;

create table mappi.devices (
ip_address varchar(255) primary key,
hostname varchar(255),
hostname_type varchar(255),
mac_address varchar(255),
state varchar(10),
vendor varchar(255),
last_seen timestamp default CURRENT_TIMESTAMP,
first_seen timestamp default CURRENT_TIMESTAMP );