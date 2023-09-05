USE nfl;
SELECT * FROM betting WHERE week = 4;
SELECT * FROM ranking WHERE week = 4 LIMIT 10;
SELECT * FROM projection WHERE week = 4 LIMIT 10;
SELECT * FROM player WHERE name = "Darius Harris";
SELECT * FROM outlet WHERE outletId =3;
SELECT * FROM analyst WHERE analystId = 4;

SELECT * FROM team;
SELECT * FROM pos;




SELECT AVG(ranking) FROM ranking WHERE playerId = 1 AND  week = 1 and rankGroup ='WR';
SELECT ranking.*, p.name from ranking INNER JOIN player p on p.playerId = ranking.playerId
where ranking = 1 and week = 2 and rankGroup ='TE';

(SELECT playerId FROM player WHERE name = "Bailey Zappe");
UPDATE player 
SET espnId = 3122906, espnName= 'Darius Harris' 
WHERE playerId = 1764;	


INSERT INTO player (name, posId, teamId, espnId, espnName, fpId, fpName)
VALUES
	();
    
DELETE FROM ranking WHERE date < '0002-01-01';

SELECT * FROM player WHERE playerId = 463;
SELECT 
	away.name,
    home.name,
    b.*
FROM 
	betting b
INNER JOIN
	team away ON away.teamId = awayTeamId
INNER JOIN
	team home ON home.teamId = homeTeamId
	
	
