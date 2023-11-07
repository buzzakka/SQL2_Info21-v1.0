-- СОЗДАНИЕ БАЗЫ ДАННЫХ

CREATE DATABASE S21_Info;
\c s21_info

-- СОЗДАНИЕ ТАБЛИЦЫ

CREATE TABLE IF NOT EXISTS Peers
(
    Nickname VARCHAR(30) NOT NULL PRIMARY KEY,
    Birthday DATE        NOT NULL
);

CREATE TABLE IF NOT EXISTS Tasks
(
    Title      Varchar(50) DEFAULT NULL PRIMARY KEY,
    ParentTask Varchar(50) REFERENCES Tasks (Title),
    MaxXP      INTEGER NOT NULL
);

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS P2P
(
    ID           SERIAL       NOT NULL PRIMARY KEY,
    "Check"      SERIAL       NOT NULL,
    CheckingPeer VARCHAR(30)  NOT NULL REFERENCES Peers (Nickname),
    State        check_status NOT NULL,
    Time         TIME         NOT NULL
);

CREATE TABLE IF NOT EXISTS Verter
(
    ID      SERIAL       NOT NULL PRIMARY KEY,
    "Check" SERIAL       NOT NULL,
    State   check_status NOT NULL,
    Time    TIME         NOT NULL
);

CREATE TABLE IF NOT EXISTS Checks
(
    ID     SERIAL      NOT NULL PRIMARY KEY ,
    Peer   VARCHAR(30) NOT NULL REFERENCES Peers (Nickname),
    Task   VARCHAR(50) NOT NULL REFERENCES Tasks (Title),
    "Date" DATE        NOT NULL
);

CREATE TABLE IF NOT EXISTS TransferredPoints
(
    ID           SERIAL      NOT NULL PRIMARY KEY,
    CheckingPeer VARCHAR(30) NOT NULL,
    CheckedPeer  VARCHAR(30) NOT NULL,
    PointsAmount INTEGER     NOT NULL
);

CREATE TABLE IF NOT EXISTS Friends
(
    ID    SERIAL      NOT NULL PRIMARY KEY,
    Peer1 VARCHAR(30) NOT NULL,
    Peer2 VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS Recommendations
(
    ID              SERIAL      NOT NULL PRIMARY KEY,
    Peer            VARCHAR(30) NOT NULL,
    RecommendedPeer VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS XP
(
    ID       SERIAL  NOT NULL PRIMARY KEY,
    "Check"  SERIAL  NOT NULL,
    XPAmount INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS TimeTracking
(
    ID     SERIAL PRIMARY KEY,
    Peer   VARCHAR(30) NOT NULL,
    "Date" DATE        NOT NULL,
    Time   TIME        NOT NULL,
    State  INTEGER CHECK
        (State = 1 OR State = 2)
);

-- ПРИВЯЗКА FOREIGN KEY

ALTER TABLE P2P
    ADD FOREIGN KEY ("Check") REFERENCES Checks (ID);

ALTER TABLE Verter
    ADD FOREIGN KEY ("Check") REFERENCES Checks (ID);

ALTER TABLE TransferredPoints
    ADD FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    ADD FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname);

ALTER TABLE Friends
    ADD FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    ADD FOREIGN KEY (Peer2) REFERENCES Peers (Nickname);

ALTER TABLE Recommendations
    ADD FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    ADD FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname);

ALTER TABLE XP
    ADD FOREIGN KEY ("Check") REFERENCES Checks (ID);

ALTER TABLE TimeTracking
    ADD FOREIGN KEY (Peer) REFERENCES Peers (Nickname);

-- IMPORT

CREATE OR REPLACE PROCEDURE import_peers(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Peers(Nickname, Birthday) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE import_tasks(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Tasks(Title, ParentTask, MaxXP) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE import_p2p(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY P2P(ID, "Check", CheckingPeer, State, Time) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('p2p_id_seq', (SELECT MAX(id) FROM p2p)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_verter(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Verter(ID, "Check", State, Time) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('verter_id_seq', (SELECT MAX(id) FROM verter)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_checks(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Checks(ID, Peer, Task, "Date") FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('checks_id_seq', (SELECT MAX(id) FROM checks)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_transferred_points(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY TransferredPoints(ID, CheckingPeer, CheckedPeer, PointsAmount) FROM ''' || filename ||
            ''' WITH CSV HEADER DELIMITER ' || quote_literal(delimiter);
    PERFORM setval('transferredpoints_id_seq', (SELECT MAX(id) FROM transferredpoints)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_friends(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Friends(ID, Peer1, Peer2) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('friends_id_seq', (SELECT MAX(id) FROM friends)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_recommendations(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY Recommendations(ID, Peer, RecommendedPeer) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('recommendations_id_seq', (SELECT MAX(id) FROM recommendations)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_xp(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY XP(ID, "Check", XPAmount) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('xp_id_seq', (SELECT MAX(id) FROM xp)+1);
END;
$$;

CREATE OR REPLACE PROCEDURE import_time_tracking(filename VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY TimeTracking(ID, Peer, "Date", Time, State) FROM ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
    PERFORM setval('timetracking_id_seq', (SELECT MAX(id) FROM timetracking)+1);
END;
$$;

-- !!! На вход подается путь до файла С ФАЙЛАМИ: peers.csv, tasks.csv, p2p.csv, verter.csv, checks.csv, transferred_points.csv,
--                                               friends.csv, recommendations.csv, xp.csv, time_tracking.csv
CREATE OR REPLACE PROCEDURE import_all(path_to_dir VARCHAR, delimiter VARCHAR(1))
    LANGUAGE plpgsql AS
$$
BEGIN
    IF "right"(path_to_dir, 1) NOT IN ('/') THEN
        path_to_dir = concat(path_to_dir, '/');
    END IF;
    CALL import_peers(concat(path_to_dir, 'peers.csv'), delimiter);
    CALL import_tasks(concat(path_to_dir, 'tasks.csv'), delimiter);
    CALL import_checks(concat(path_to_dir, 'checks.csv'), delimiter);
    CALL import_p2p(concat(path_to_dir, 'p2p.csv'), delimiter);
    CALL import_verter(concat(path_to_dir, 'verter.csv'), delimiter);
    CALL import_transferred_points(concat(path_to_dir, 'transferred_points.csv'), delimiter);
    CALL import_friends(concat(path_to_dir, 'friends.csv'), delimiter);
    CALL import_recommendations(concat(path_to_dir, 'recommendations.csv'), delimiter);
    CALL import_xp(concat(path_to_dir, 'xp.csv'), delimiter);
    CALL import_time_tracking(concat(path_to_dir, 'time_tracking.csv'), delimiter);
END;
$$;

-- EXPORT

CREATE OR REPLACE PROCEDURE export_peers(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Peers) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_tasks(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Tasks) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_p2p(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM P2P) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' || quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_verter(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Verter) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_checks(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Checks) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_transferred_points(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM TransferredPoints) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_friends(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Friends) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_recommendations(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM Recommendations) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_xp(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM XP) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' || quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_time_tracking(filename VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY (SELECT * FROM TimeTracking) TO ''' || filename || ''' WITH CSV HEADER DELIMITER ' ||
            quote_literal(delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_all(path_to_dir VARCHAR, delimiter VARCHAR(5))
    LANGUAGE plpgsql AS
$$
BEGIN
    IF "right"(path_to_dir, 1) NOT IN ('/') THEN
        path_to_dir = concat(path_to_dir, '/');
    END IF;
    CALL export_peers(concat(path_to_dir, 'peers.csv'), delimiter);
    CALL export_tasks(concat(path_to_dir, 'tasks.csv'), delimiter);
    CALL export_checks(concat(path_to_dir, 'checks.csv'), delimiter);
    CALL export_p2p(concat(path_to_dir, 'p2p.csv'), delimiter);
    CALL export_verter(concat(path_to_dir, 'verter.csv'), delimiter);
    CALL export_transferred_points(concat(path_to_dir, 'transferred_points.csv'), delimiter);
    CALL export_friends(concat(path_to_dir, 'friends.csv'), delimiter);
    CALL export_recommendations(concat(path_to_dir, 'recommendations.csv'), delimiter);
    CALL export_xp(concat(path_to_dir, 'xp.csv'), delimiter);
    CALL export_time_tracking(concat(path_to_dir, 'time_tracking.csv'), delimiter);
END;
$$;

-- КОММЕНТАРИИ

COMMENT ON COLUMN Peers.Nickname IS 'Ник пира';
COMMENT ON COLUMN Peers.Birthday IS 'День рождения';

COMMENT ON COLUMN Tasks.Title IS 'Название задания';
COMMENT ON COLUMN Tasks.ParentTask IS 'Название задания, являющегося условием входа';
COMMENT ON COLUMN Tasks.MaxXP IS 'Максимальное количество XP';

COMMENT ON TYPE check_status IS 'Тип перечисления для статуса проверки';

COMMENT ON COLUMN P2P.ID IS 'ID';
COMMENT ON COLUMN P2P."Check" IS 'ID проверки';
COMMENT ON COLUMN P2P.CheckingPeer IS 'Ник проверяющего пира';
COMMENT ON COLUMN P2P.State IS 'Статус проверки Verter`ом';
COMMENT ON COLUMN P2P.Time IS 'Время';

COMMENT ON COLUMN Checks.ID IS 'ID';
COMMENT ON COLUMN Checks.Peer IS 'Ник пира';
COMMENT ON COLUMN Checks.Task IS 'Название задания';
COMMENT ON COLUMN Checks."Date" IS 'Дата проверки';

COMMENT ON COLUMN TransferredPoints.ID IS 'ID';
COMMENT ON COLUMN TransferredPoints.CheckingPeer IS 'Ник проверяющего пира';
COMMENT ON COLUMN TransferredPoints.CheckedPeer IS 'Ник проверяемого пира';
COMMENT ON COLUMN TransferredPoints.PointsAmount IS 'Количество переданных пир поинтов за всё время (только от проверяемого к проверяющему)';

COMMENT ON COLUMN Friends.ID IS 'Ник пира';
COMMENT ON COLUMN Friends.Peer1 IS 'Ник первого пира';
COMMENT ON COLUMN Friends.Peer2 IS 'Ник второго пира';

COMMENT ON COLUMN Recommendations.ID IS 'Ник пира';
COMMENT ON COLUMN Recommendations.Peer IS 'Ник пира';
COMMENT ON COLUMN Recommendations.RecommendedPeer IS 'Ник пира, к которому рекомендуют идти на проверку';

COMMENT ON COLUMN XP.ID IS 'ID';
COMMENT ON COLUMN XP."Check" IS 'ID проверки';
COMMENT ON COLUMN XP.XPAmount IS 'Количество полученного XP';

COMMENT ON COLUMN TimeTracking.ID IS 'ID';
COMMENT ON COLUMN TimeTracking.Peer IS 'Ник пира';
COMMENT ON COLUMN TimeTracking."Date" IS 'Дата';
COMMENT ON COLUMN TimeTracking.Time IS 'Время';
COMMENT ON COLUMN TimeTracking.State IS 'Состояние (1 - пришел, 2 - вышел)';
