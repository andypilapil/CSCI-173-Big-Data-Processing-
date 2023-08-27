CREATE EXTERNAL TABLE tweets (
  `interactions` array<struct<
    demographic:struct<
      gender:string
    >,
    `interaction`:struct<
      author:struct<
        username:string,
        name:string,
        id:string
      >,
      id:bigint
    >,
    klout:struct<
      score:int
    >
  >>
)

ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'hdfs:///user/andreapilapil/logs/';

CREATE TABLE tweets_exploded AS 
SELECT DISTINCT (inter.interaction.author.name) AS author_name,
inter.klout.score as klout_score 
FROM tweets LATERAL VIEW explode(interactions) exploded_table as inter SORT BY klout_score DESC LIMIT 0,5;
SELECT * FROM tweets_exploded;

-- submitted by: Andrea Pilapil & Mari Valle

