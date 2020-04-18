create database mappi;

CREATE TABLE `mappi`.`devices` (
  `ip_address` varchar(255) NOT NULL,
  `hostname` varchar(255) DEFAULT NULL,
  `last_seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `first_seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ip_address`)
)