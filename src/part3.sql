-- TASK 1

CREATE
OR REPLACE FUNCTION get_transderred_points_readeble()
    RETURNS TABLE
            (
                Peer1 varchar,
                Peer2 varchar,
                PointsAmount int
            )
AS
$$
BEGIN
RETURN QUERY WITH t AS (
            SELECT DISTINCT CASE
                                WHEN checkingpeer < checkedpeer THEN checkingpeer
                                ELSE checkedpeer
                                END AS peer1,
                            CASE
                                WHEN checkingpeer < checkedpeer THEN checkedpeer
                                ELSE checkingpeer
                                END AS peer2
            FROM transferredpoints
        )
SELECT t.peer1,
       t.peer2,
       COALESCE((SELECT transferredpoints.pointsamount
                 FROM transferredpoints
                 WHERE checkedpeer = t.peer2
                   AND checkingpeer = t.peer1), 0)
           -
       COALESCE((SELECT transferredpoints.pointsamount
                 FROM transferredpoints
                 WHERE checkedpeer = t.peer1
                   AND checkingpeer = t.peer2), 0) AS PointsAmount
FROM t;
END;
$$
LANGUAGE plpgsql;

-- TASK 2

CREATE
OR REPLACE FUNCTION get_success_tasks_info()
    RETURNS TABLE
            (
                Peer varchar,
                Task varchar,
                XP   integer
            )
AS
$$
BEGIN
RETURN QUERY
SELECT Checks.Peer,
       split_part(Checks.Task, '_', 1)::varchar AS Task, XPAmount
FROM Checks
         JOIN XP ON Checks.id = Xp."Check";
END;
$$
LANGUAGE plpgsql;

-- TASK 3

CREATE
OR REPLACE FUNCTION get_not_leaving_campus(needed_date Date)
    RETURNS TABLE
            (
                Peer varchar
            )
AS
$$
BEGIN
RETURN QUERY
SELECT tt1.peer
FROM timetracking AS tt1
WHERE state = 1
  AND tt1."Date" = needed_date
EXCEPT
SELECT tt2.peer
FROM timetracking AS tt2
WHERE state = 2
  AND tt2."Date" = needed_date;
END;
$$
LANGUAGE plpgsql;

-- TASK 4

CREATE
OR REPLACE FUNCTION get_changing_of_pierpoints()
    RETURNS TABLE
            (
                Peer         varchar,
                PointsChange bigint
            )
AS
$$
BEGIN
RETURN QUERY WITH ReceivedPeerpoints AS (
            SELECT nickname, COALESCE(sum(pointsamount), 0) as get
            FROM transferredpoints
                     RIGHT JOIN peers ON transferredpoints.checkingpeer = peers.nickname
            GROUP BY nickname
        ),
             GivenPeerpoints AS (
                 SELECT nickname, COALESCE(sum(pointsamount), 0) as give
                 FROM transferredpoints
                          RIGHT JOIN peers ON transferredpoints.checkedpeer = peers.nickname
                 GROUP BY nickname
             )
SELECT ReceivedPeerpoints.nickname AS peer, get - give AS PointsChange
FROM ReceivedPeerpoints
    JOIN GivenPeerpoints
ON GivenPeerpoints.nickname = ReceivedPeerpoints.nickname
ORDER BY PointsChange DESC;
END;
$$
LANGUAGE plpgsql;

-- TASK 5

CREATE
OR REPLACE FUNCTION get_changing_of_pierpoints_2()
    RETURNS TABLE
            (
                Peer         varchar,
                PointsChange bigint
            )
AS
$$
BEGIN
RETURN QUERY WITH TempTable AS (
            SELECT
                peer1,
                pointsamount
            FROM get_transderred_points_readeble()
            UNION ALL
            SELECT
                peer2 as peer1,
                -pointsamount
            FROM get_transderred_points_readeble()
        )
SELECT nickname                                 AS Peer,
       COALESCE(SUM(TempTable.pointsamount), 0) AS PointsChange
FROM TempTable
         RIGHT JOIN peers ON nickname = peer1
GROUP BY nickname
ORDER BY PointsChange DESC, Peer DESC;
END;
$$
LANGUAGE plpgsql;

-- TASK 6

CREATE
OR REPLACE FUNCTION get_frequently_checked_task_for_day()
    RETURNS TABLE
            (
                Day  date,
                task varchar
            )
AS
$$
BEGIN
RETURN QUERY WITH task_count_per_day AS (
            SELECT "Date", checks.task, count(checks.task) as c
            FROM checks
            GROUP BY checks.task, "Date"
            ORDER BY "Date"
        ),
             dates AS (
                 SELECT DISTINCT "Date"
                 FROM task_count_per_day
             )
SELECT dates."Date", tc.task
FROM dates
         JOIN task_count_per_day AS tc ON dates."Date" = tc."Date"
WHERE c = (SELECT MAX(c)
           FROM task_count_per_day
           WHERE task_count_per_day."Date" = dates."Date");
END;
$$
LANGUAGE plpgsql;

-- TASK 7

CREATE
OR REPLACE FUNCTION get_completed_the_last_task(task_block varchar)
    RETURNS TABLE
            (
                Peer   varchar,
                "Date" date
            )
AS
$$
DECLARE
last_task varchar = (
        SELECT title
        from tasks
        WHERE title ~* concat('^', task_block, '\d+_.+$')
        ORDER BY title DESC
        LIMIT 1
    );
BEGIN
RETURN QUERY
SELECT checks.peer, MIN(checks."Date") as "Date"
FROM checks
         JOIN p2p ON checks.id = p2p."Check"
         LEFT JOIN verter ON checks.id = verter."Check"
WHERE checks.task = last_task
  AND p2p.state = 'Success'
  AND (NOT exists(SELECT * FROM verter WHERE verter."Check" = checks.id) OR verter.state = 'Success')
GROUP BY checks.peer
ORDER BY "Date";
END;
$$
LANGUAGE plpgsql;

-- TASK 8

CREATE
OR REPLACE FUNCTION get_recommended_by_friends_peer()
    RETURNS TABLE
            (
                Peer            varchar,
                RecommendedPeer varchar
            )
AS
$$
BEGIN
RETURN QUERY WITH t AS (
            SELECT peer2, r.recommendedpeer, COUNT(nickname) as c
            FROM peers
                     LEFT JOIN friends ON peers.nickname = friends.peer1
                     RIGHT JOIN recommendations as r ON peers.nickname = r.peer
            WHERE peer2 <> r.recommendedpeer
            GROUP BY r.recommendedpeer, peer2
        )
SELECT t1.peer2, t1.recommendedpeer
FROM t AS t1
WHERE t1.c = (
    SELECT MAX(c)
    FROM t AS t2
    WHERE t2.peer2 = t1.peer2)
ORDER BY t1.peer2, t1.recommendedpeer;
END;
$$
LANGUAGE plpgsql;

-- TASK 9

CREATE
OR REPLACE FUNCTION get_info_about_task_blocks(task_block1 varchar, task_block2 varchar)
    RETURNS TABLE
            (
                StartedBlock1      bigint,
                StartedBlock2      bigint,
                StartedBothBlocks  bigint,
                DidntStartAnyBlock bigint
            )
AS
$$
BEGIN
RETURN QUERY WITH StartedBlock1 AS (
            SELECT DISTINCT peer,
                            task_block1 AS block
            FROM checks
            WHERE task ~* concat('^', task_block1, '\d+_.+$')
        ),
             StartedBlock2 AS (
                 SELECT DISTINCT peer,
                                 task_block2 AS block
                 FROM checks
                 WHERE task ~* concat('^', task_block2, '\d+_.+$')
             ),
             StartedBothBlock AS (
                 SELECT peer
                 FROM StartedBlock1
                 INTERSECT
                 SELECT peer
                 FROM StartedBlock2
             ),
             DidntStartAnyBlock AS (
                 SELECT nickname
                 FROM peers
                 EXCEPT
                 SELECT peer
                 FROM checks
                 WHERE task ~* concat('^', task_block1, '\d+_.+$')
                    OR task ~* concat('^', task_block2, '\d+_.+$')
             )
SELECT (SELECT COUNT(*) FROM StartedBlock1)      AS StartedBlock1,
       (SELECT COUNT(*) FROM StartedBlock2)      AS StartedBlock2,
       (SELECT COUNT(*) FROM StartedBothBlock)   AS StartedBothBlock,
       (SELECT COUNT(*) FROM DidntStartAnyBlock) AS DidntStartAnyBlock;
END
$$
LANGUAGE plpgsql;

-- TASK 10

CREATE
OR REPLACE FUNCTION get_info_about_checks_in_birthday()
    RETURNS TABLE
            (
                SuccessfulChecks   bigint,
                UnsuccessfulChecks bigint
            )
AS
$$
BEGIN
RETURN QUERY WITH BirthdayInfo AS (
            SELECT nickname,
                   extract('DAY' FROM birthday)   AS day,
                   extract('MONTH' FROM birthday) AS month
            FROM peers
        ),
             SuccessfulChecks AS (
                 SELECT peer,
                        extract('DAY' FROM "Date")   AS day,
                        extract('MONTH' FROM "Date") AS month
                 FROM checks
                 WHERE exists(SELECT * FROM p2p WHERE "Check" = checks.id AND state = 'Success')
                   AND (exists(SELECT * FROM verter WHERE "Check" = checks.id AND state = 'Success') OR
                        NOT exists(SELECT * FROM verter WHERE "Check" = checks.id))
             ),
             FailureChecks AS (
                 SELECT peer,
                        extract('DAY' FROM "Date")   AS day,
                        extract('MONTH' FROM "Date") AS month
                 FROM checks
                 WHERE exists(SELECT * FROM p2p WHERE "Check" = checks.id AND state = 'Failure')
                    OR exists(SELECT * FROM verter WHERE "Check" = checks.id AND state = 'Failure')
             ),
             AllChecks AS (
                 SELECT peer,
                        extract('DAY' FROM "Date")   AS day,
                        extract('MONTH' FROM "Date") AS month
                 FROM checks
             ),
             SuccessfulChecksInBirthday AS (
                 SELECT *
                 FROM SuccessfulChecks AS sc
                          LEFT JOIN BirthdayInfo AS bi ON sc.peer = bi.nickname
                 WHERE bi.month = sc.month
                   AND bi.day = sc.day
             ),
             FailureChecksInBirthday AS (
                 SELECT *
                 FROM FailureChecks AS fc
                          LEFT JOIN BirthdayInfo AS bi ON fc.peer = bi.nickname
                 WHERE bi.month = fc.month
                   AND bi.day = fc.day
             ),
             AllChecksInBirthday AS (
                 SELECT *
                 FROM AllChecks AS ac
                          LEFT JOIN BirthdayInfo AS bi ON ac.peer = bi.nickname
                 WHERE ac.month = bi.month
                   AND ac.day = bi.day
             ),
             SuccessfulChecksInBirthdayCount AS (
                 SELECT COUNT(*)
                 FROM SuccessfulChecksInBirthday
             ),
             FailureChecksInBirthdayCount AS (
                 SELECT COUNT(*)
                 FROM FailureChecksInBirthday
             ),
             AllChecksInBirthdayCount AS (
                 SELECT COUNT(*)
                 FROM AllChecksInBirthday
             )
SELECT (select * from SuccessfulChecksInBirthdayCount) * 100 /
       (select * from AllChecksInBirthdayCount) as SuccessfulChecks,
       (select * from FailureChecksInBirthdayCount) * 100 /
       (select * from AllChecksInBirthdayCount)    UnsuccessfulChecks;
END;
$$
LANGUAGE plpgsql;

-- TASK 11

CREATE
OR REPLACE FUNCTION from_3_tasks_done_2(task1 varchar, task2 varchar, task3 varchar)
    RETURNS TABLE
            (
                peers varchar
            )
AS
$$
BEGIN
RETURN QUERY WITH SuccessTasks AS (
            SELECT peer,
                   task
            FROM checks
                     JOIN p2p ON checks.id = p2p."Check"
                     LEFT JOIN verter ON checks.id = verter."Check"
            WHERE p2p.state = 'Success'
              AND (NOT exists(SELECT * FROM verter WHERE verter."Check" = checks.id) OR
                   verter.state = 'Success')
        )
SELECT DISTINCT peer as nickname
FROM SuccessTasks
WHERE peer in (SELECT peer FROM SuccessTasks WHERE task = task1)
  AND peer in (SELECT peer FROM SuccessTasks WHERE task = task2)
  AND peer NOT IN (SELECT peer FROM SuccessTasks WHERE task = task3);
END
$$
LANGUAGE plpgsql;

-- TASK 12

CREATE
OR REPLACE FUNCTION get_prev_count_for_tasks()
    RETURNS TABLE
            (
                peer      varchar,
                PrevCount int
            )
AS
$$
BEGIN
RETURN QUERY WITH RECURSIVE r AS (
            SELECT title,
                   0 AS PrevCount
            FROM tasks
            WHERE parenttask IS NULL

            UNION ALL

            SELECT tasks.title,
                   r.PrevCount + 1
            FROM r,
                 tasks
            WHERE r.title = tasks.parenttask
        )
SELECt *
FROM r;
END
$$
LANGUAGE plpgsql;

-- TASK 13

CREATE
OR REPLACE FUNCTION find_lucky_days_for_checks(N bigint)
    RETURNS TABLE
            (
                lucky_day date
            )
AS
$$
BEGIN
RETURN QUERY WITH check_with_time AS (
            SELECT c.id as id, c."Date" c_date, task, MIN(p2p.time) as c_time
            FROM checks c
                     JOIN p2p ON c.id = p2p."Check"
            GROUP BY c.id
        ),
             check_with_status AS (
                 SELECT c.id as id, c_date, task, c_time, xp.xpamount as exp
                 FROM check_with_time c
                          LEFT JOIN xp ON c.id = xp."Check"
                 WHERE xp.xpamount IS NOT NULL
             ),
             check_percent AS (
                 SELECT c.id as id, c_date, c_time, 1 as exp_perc
                 FROM check_with_status c
                          JOIN tasks t ON c.task like concat('%', t.title, '%')
                 WHERE (cast(exp as float) / cast(t.maxxp as float)) >= 0.8
             ),
             consecutive_successful_checks AS (
                 select id, c_date, row_number() over (partition by c_date, grp order by c_time) "Count"
                 from (
                          select id, c_date, c_time, exp_perc, sum(grp) over (partition by c_date order by c_time) grp
                          from (
                                   select *,
                                          case exp_perc
                                              when lag(exp_perc) over (partition by c_date order by c_time, exp_perc)
                                                  then 0
                                              else 1 end grp
                                   from check_percent
                               ) x
                      ) y
             )
SELECT c.c_date as lucky_day
FROM (SELECT * FROM consecutive_successful_checks csc WHERE csc."Count" >= N) c
GROUP BY c.c_date;
END
$$
LANGUAGE plpgsql;

-- TASK 14

CREATE
OR
    REPLACE
    FUNCTION
    get_peer_with_max_xp(
)
    RETURNS
        TABLE
        (
            Peer varchar,
            XP   integer
        )
AS
$$
BEGIN
RETURN QUERY WITH peer_and_xp AS (
            SELECT checks.peer, SUM(xpamount)::INTEGER as XP
            from xp
                     LEFT JOIN checks ON xp."Check" = checks.id
            GROUP BY checks.peer
        )
SELECT *
FROM peer_and_xp
WHERE peer_and_xp.xp = (SELECT MAX(peer_and_xp.xp) FROM peer_and_xp);
END;
$$
LANGUAGE plpgsql;

-- TASK 15

CREATE
OR REPLACE FUNCTION get_peer_early_arrivals_N("Time" time, N int)
    RETURNS TABLE
            (
                Peer varchar
            )
AS
$$
BEGIN
RETURN QUERY WITH temp_table AS (
            SELECT timetracking.peer,
                   COUNT(timetracking.peer) as c
            FROM timetracking
            WHERE time < "Time"
              AND state = 1
            GROUP BY timetracking.peer)
SELECT temp_table.peer
FROM temp_table
WHERE temp_table.c >= N;
END;
$$
LANGUAGE plpgsql;

-- TASK 16

CREATE
OR REPLACE FUNCTION get_peer_with_M_exits_for_N_days(M int, N int)
RETURNS TABLE(Peer varchar) AS $$
BEGIN
RETURN QUERY
SELECT timetracking.peer
FROM timetracking
WHERE state = 2
  AND "Date" BETWEEN current_date - N AND current_date
GROUP BY timetracking.peer
HAVING COUNT(*) >= M;
END;
$$
LANGUAGE plpgsql;

-- TASK 17

CREATE
OR REPLACE FUNCTION get_birthday_attendance()
RETURNS TABLE(Month varchar, EarlyEntries int) AS $$
BEGIN
RETURN QUERY WITH AlterTimetracking AS (
                SELECT
                    peer,
                    to_char("Date", 'Month') AS month,
                    time,
                    state
                FROM timetracking
            ), AlterPeers AS (
                SELECT
                    nickname,
                    to_char(birthday, 'Month') AS bmonth
                FROM peers
            ), TempOne AS (
                SELECT DISTINCT AlterTimetracking.month, COUNT(peer) AS c1
                FROM AlterTimetracking
                JOIN AlterPeers ON peer = nickname
                WHERE AlterTimetracking.month = bmonth AND state = 1
                GROUP BY AlterTimetracking.month
            ), TempTwo AS (
                SELECT DISTINCT AlterTimetracking.month, COUNT(peer) AS c2
                FROM AlterTimetracking
                JOIN AlterPeers ON peer = nickname
                WHERE AlterTimetracking.month = bmonth AND state = 1 AND time < '12:00'
                GROUP BY AlterTimetracking.month
            )
SELECT TempOne.month::varchar , (TempTwo.c2 * 100 / TempOne.c1) ::int
FROM TempOne
         JOIN TempTwo ON TempOne.month = TempTwo.month;
END
$$
LANGUAGE plpgsql;