CREATE TABLE pos (
	posId INT AUTO_INCREMENT PRIMARY KEY,
    pos VARCHAR(20)
);

INSERT INTO pos
  (posId, pos) 
VALUES  
	(1,	'QB'),
	(2,	'RB'),
	(3,	'WR'),
	(4,	'TE'),
	(5,	'K'),
	(6,	'DST'),
	(7,	'LB'),
	(8,	'DL'),
	(9, 'DB'),
    (10, 'FB');

CREATE TABLE team (
	teamId INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(3),
    espnId INT,
    espnName VARCHAR(255),
    nflId INT,
    nflName VARCHAR(255),
    fpId INT,
    fpName VARCHAR(255),
    cbsId INT,
    cbsName VARCHAR(255),
    bpId INT,
    bpName VARCHAR(255)    
);

CREATE TABLE betting (
    date DATE,
    season INT,
    week INT,
    overUnder DECIMAL,
    overUnderCost DECIMAL,
    awayTeamId INT,
    awayMoneyLine DECIMAL,
    awaySpread DECIMAL,
    awayCost DECIMAL,
    homeTeamId INT,
    homeMoneyLine DECIMAL,
    homeSpread DECIMAL,
    homeCost DECIMAL,
    
    CONSTRAINT pk_betting
		PRIMARY KEY (date, season, week, awayTeamId, homeTeamId),
    CONSTRAINT fk_betting_awayTeamId
		FOREIGN KEY  (awayTeamId) REFERENCES team(teamId),
    CONSTRAINT fk_betting_homeTeamId
		FOREIGN KEY  (homeTeamId) REFERENCES team(teamId)
    );

CREATE TABLE player (
	playerId INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    posId INT,
    teamId INT,
    espnId INT,
    espnName VARCHAR(255),
    nflId INT,
    nflName VARCHAR(255),
    fpId INT,
    fpName VARCHAR(255),
    cbsId INT,
    cbsName VARCHAR(255),
    bpId INT,
    bpName VARCHAR(255),
    
    CONSTRAINT fk_player_posId
		FOREIGN KEY  (posId) REFERENCES pos(posId),
    CONSTRAINT fk_player_teamId 
		FOREIGN KEY (teamId) REFERENCES team(teamId)
);

CREATE TABLE outlet (
	outletId INT AUTO_INCREMENT PRIMARY KEY,
	outletName VARCHAR(255)
);

INSERT INTO outlet
  (outletName) 
VALUES 
	('espn'),
    ('yahoo'),
	('cbs'),
	('nfl'),
	('fantasyPros'),
	('bettingPros'),
    ('fantrax'),
	('ffc'),
	('sleeper');

CREATE TABLE analyst (
	analystId INT AUTO_INCREMENT PRIMARY KEY,
	analystName VARCHAR(255),
	outletId INT,
	
	CONSTRAINT fk_analyst_outletId
	FOREIGN KEY (outletId) REFERENCES outlet(outletId)
);

INSERT INTO analyst
  (analystName, outletId) 
VALUES 
	('Karabell', 1),
	('Yates', 1),
	('Cockcroft', 1),
	('Clay', 1),
	('Moody', 1),
    ('Bowen', 1),
    ('Dopp', 1),
    ('Loza', 1),
	('Jamey Eisenberg',	3),
	('Dave Richard', 3),
	('Heath Cummings', 3),
	('nfl', 4),
	('Derek Brown',	5),
	('Pat Fitzmaurice',	5),
	('Matthew Freedman', 5),
	('Joe Pisapia',	5),
    ('Andrew Erickson', 5),
    ('ecr', 5)
	;

CREATE TABLE projection (
	playerId INT, 
	date DATE,
    season INT,
    week INT,
	outletId INT, 
	gp INT,
	att DECIMAL,
	comp DECIMAL,
	passYd DECIMAL, 
	passYdPg DECIMAL, 
	passTd DECIMAL, 
	pInt DECIMAL, 
	passRtg DECIMAL,
	rush DECIMAL,
	rushYd DECIMAL, 
	ydPerRush DECIMAL, 
	rushTd DECIMAL,
	target DECIMAL, 
	rec DECIMAL, 
	recYd DECIMAL, 
	recYdPg DECIMAL, 
	ydPerRec DECIMAL,
	recTd DECIMAL,
	fmb DECIMAL,
	fgM DECIMAL,
	fgA DECIMAL,
	fgLong DECIMAL,
	fgM0119 DECIMAL,
	fgA0119 DECIMAL,
	fgM2029 DECIMAL,
	fgA2029 DECIMAL,
	fgM3039 DECIMAL,
	fgA3039 DECIMAL,
	fgM4049 DECIMAL,
	fgA4049 DECIMAL,
	fgM5099 DECIMAL,
	fgA5099 DECIMAL,
	xpM DECIMAL,
	xpA DECIMAL,
	defInt DECIMAL,
	sfty DECIMAL,
	sack DECIMAL,
	tckl DECIMAL,
	defFmbRec DECIMAL,
	defFmbFor DECIMAL,
	defTd DECIMAL, 
	retTd DECIMAL,
	ptsAllowed DECIMAL,
	ptsAllowedPg DECIMAL,
	pYdAllowedPg DECIMAL,
	rYdAllowedPg DECIMAL,
	totalYdAllowed DECIMAL, 
	totalYdAllowedPg DECIMAL,
    twoPt DECIMAL,
	fantasyPoints DECIMAL,
	fantasyPointsPg DECIMAL,
	
    CONSTRAINT pk_projection
		PRIMARY KEY (playerId, date, season, week, outletId),
    CONSTRAINT fk_projection_playerId
		FOREIGN KEY (playerId) REFERENCES player(playerId),
    CONSTRAINT fk_projection_outlet
		FOREIGN KEY (outletId) REFERENCES outlet(outletId)
);
    
CREATE TABLE ranking (
	outletId INT, 
	date DATE,
    season INT,
    week INT,
	rankGroup VARCHAR(255),
	analystId INT,
	ranking DECIMAL,
	high INT, 
    low INT,
	playerId INT,

	CONSTRAINT pk_ranking
		PRIMARY KEY (outletId, date, season, week, playerId, analystId, rankGroup),
	CONSTRAINT fk_ranking_playerId
		FOREIGN KEY (playerId) REFERENCES player(playerId),
    CONSTRAINT fk_ranking_outletId
		FOREIGN KEY (outletId) REFERENCES outlet(outletId),
    CONSTRAINT fk_ranking_analystId
		FOREIGN KEY (analystId) REFERENCES analyst(analystId)
);
    
CREATE TABLE  adp (
	outletId INT,
	date DATE,
    playerId INT,
	adp DECIMAL,
	high DECIMAL,
	low DECIMAL,
	
    CONSTRAINT pk_adp
		PRIMARY KEY (outletId, date, playerId),
	CONSTRAINT fk_adp_playerId
		FOREIGN KEY (playerId) REFERENCES player(playerId),
	CONSTRAINT fk_adp_outletId
		FOREIGN KEY (outletId) REFERENCES outlet(outletId)
);

        
