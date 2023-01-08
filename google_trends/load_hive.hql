CREATE EXTERNAL TABLE IF NOT EXISTS countries_trends 
( 
  `date` STRING,
  `Argentina` INT, 
  `France` INT, 
  `Croatia` INT, 
  `Spain` INT, 
  `Poland` INT, 
  `isPartial` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/user/proj/hive/countries'
OVERWRITE INTO TABLE countries_trends;

CREATE EXTERNAL TABLE IF NOT EXISTS players_trends 
( 
  `date` STRING,
  `Messi` INT, 
  `Mbappe` INT, 
  `Lewandowski` INT, 
  `Modric` INT, 
  `Neymar` INT, 
  `isPartial` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


LOAD DATA LOCAL INPATH '/user/proj/hive/players'
OVERWRITE INTO TABLE players_trends;

CREATE EXTERNAL TABLE IF NOT EXISTS wins_trends 
( 
  `date` STRING,
  `Argentina win` INT, 
  `France win` INT, 
  `isPartial` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


LOAD DATA LOCAL INPATH '/user/proj/hive/wins'
OVERWRITE INTO TABLE wins_trends;
 