CREATE TABLE `tables` (
  `name` varchar(64) NOT NULL,
  `path` varchar(256) NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



CREATE TABLE `columns` (
  `table_name` varchar(64) NOT NULL,
  `column_name` varchar(64) NOT NULL,
  `column_type` enum('TEXT','NUMBER') NOT NULL,
  `not_null` tinyint(4) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`),
  KEY `cn` (`column_name`),
  CONSTRAINT `tn_c` FOREIGN KEY (`table_name`) REFERENCES `tables` (`name`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



CREATE TABLE `indexes` (
  `table_name` varchar(64) NOT NULL,
  `index_name` varchar(64) NOT NULL,
  `column` varchar(64) NOT NULL,
  `kind` enum('PRIMARY','UNIQUE','INDEX') NOT NULL,
  `position` int(11) NOT NULL,
  PRIMARY KEY (`index_name`,`table_name`,`column`),
  KEY `tn_i_idx` (`table_name`),
  KEY `cn_i_idx` (`column`),
  CONSTRAINT `cn_i` FOREIGN KEY (`column`) REFERENCES `columns` (`column_name`) ON DELETE CASCADE,
  CONSTRAINT `tn_i` FOREIGN KEY (`table_name`) REFERENCES `tables` (`name`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
