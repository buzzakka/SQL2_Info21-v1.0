CREATE OR REPLACE PROCEDURE p2p_insert(
    checked_peer VARCHAR,
    checking_peer VARCHAR,
    task_name VARCHAR,
    p2p_state check_status,
    p2p_time TIME
) LANGUAGE plpgsql AS $$
DECLARE
    last_p2p_check_state check_status = (
        SELECT state
        FROM p2p
        JOIN checks ON p2p."Check" = checks.id
        WHERE checkingpeer = checking_peer AND
              peer = checked_peer AND
              task = task_name
        ORDER BY p2p.id DESC LIMIT 1
    );
BEGIN
    IF p2p_state = 'Start' THEN
        IF last_p2p_check_state IS NULL OR last_p2p_check_state != 'Start' THEN
            INSERT INTO checks(peer, task, "Date")
            VALUES (checked_peer, task_name, CURRENT_DATE);

            INSERT INTO p2p("Check", checkingpeer, state, time)
            VALUES (
                (SELECT MAX(id) FROM checks),
                checking_peer,
                p2p_state,
                p2p_time
            );
        ELSE
            RAISE EXCEPTION 'В таблице не может быть больше одной незавершенной P2P проверки, относящейся к конкретному заданию, пиру и проверяющему.';
        END IF;
    ELSIF p2p_state IN ('Success', 'Failure') THEN
        IF last_p2p_check_state = 'Start' AND last_p2p_check_state IS NOT NULL THEN
            INSERT INTO p2p("Check", checkingpeer, state, time)
            VALUES (
                (SELECT MAX("Check")
                 FROM p2p
                 JOIN checks ON p2p."Check" = checks.id
                 WHERE checkingpeer = checking_peer AND
                       peer = checked_peer AND
                       task = task_name
                 ),
                checking_peer,
                p2p_state,
                p2p_time
            );
        ELSE
            RAISE EXCEPTION 'Нет начатой проверки относящейся к конкретному заданию, пиру и проверяющему.';
        END IF;
    END IF;
END
$$;

CREATE OR REPLACE PROCEDURE verter_insert(
    checked_peer VARCHAR,
    task_name VARCHAR,
    verter_state check_status,
    verter_time TIME
) LANGUAGE plpgsql AS $$
DECLARE
    last_check int = (
        SELECT checks.id
        FROM checks
        JOIN p2p ON checks.id = p2p."Check"
        WHERE peer = checked_peer AND
              task = task_name
        ORDER BY p2p.id DESC LIMIT 1
    );
BEGIN
    IF last_check IS NULL OR
       NOT exists(SELECT state FROM p2p WHERE "Check" = last_check AND state = 'Success') THEN
        RAISE EXCEPTION 'Успешной проверки с такими параметрами нет.';
    END IF;
    IF verter_state = 'Start' AND
       exists(SELECT * FROM verter WHERE "Check" = last_check AND state = 'Start') THEN
        RAISE EXCEPTION 'У этой проверки уже есть проверка вертером.';
    END IF;
    IF verter_state IN ('Success', 'Failure') AND
       (exists(SELECT * FROM verter WHERE "Check" = last_check AND state IN ('Success', 'Failure')) OR
        NOT exists(SELECT * FROM verter WHERE "Check" = last_check AND state = 'Start')) THEN
        RAISE EXCEPTION 'У этой проверки нет начатой проверки вертером.';
    END IF;
    INSERT INTO Verter("Check", State, Time)
    VALUES (
        last_check,
        verter_state,
        verter_time
   );
END
$$;


CREATE OR REPLACE FUNCTION p2p_tranferred_points_change_trigger_fnc()
    RETURNS trigger AS
$$
DECLARE
    checked_peer VARCHAR = (SELECT peer FROM Checks WHERE id = NEW."Check" LIMIT 1);
BEGIN
    IF NEW.state = 'Start'
    THEN
        IF EXISTS(
            SELECT * FROM TransferredPoints
            WHERE
                CheckingPeer = NEW.CheckingPeer AND
                CheckedPeer = checked_peer
        ) THEN
            UPDATE TransferredPoints
            SET PointsAmount = PointsAmount + 1
            WHERE
                CheckingPeer = NEW.CheckingPeer AND
                CheckedPeer = checked_peer;
        ELSE
            INSERT INTO TransferredPoints(checkingpeer, checkedpeer, pointsamount)
            VALUES (NEW.CheckingPeer, checked_peer, 1);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE TRIGGER p2p_tranferred_points_change_trigger
BEFORE INSERT ON P2P
FOR EACH ROW
EXECUTE PROCEDURE p2p_tranferred_points_change_trigger_fnc();


CREATE OR REPLACE FUNCTION xp_insert_is_correct_trigger_fnc()
    RETURNS trigger AS
$$
DECLARE
    max_xp_for_task INTEGER = (
        SELECT MaxXP
        FROM Tasks
        WHERE title = (SELECT Task FROM Checks WHERE Checks.id = NEW."Check")
    );
    p2p_state check_status = (
        SELECT state
        FROM P2P
        WHERE P2P."Check" = NEW."Check"
        ORDER BY P2P.id DESC LIMIT 1
    );
    verter_state check_status = (
        SELECT state
        FROM Verter
        WHERE Verter."Check" = NEW."Check"
        ORDER BY Verter.id DESC LIMIT 1
    );
    is_success_check bool = (
        SELECT p2p_state = 'Success' AND (verter_state IS NULL OR verter_state = 'Success')
    );
BEGIN
    IF NOT is_success_check THEN
        RAISE EXCEPTION 'Данная проверка неуспешна';
    ELSIF NEW.xpamount > max_xp_for_task THEN
        RAISE EXCEPTION 'Количество начисляемого опыта больше максимального допустимого';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE TRIGGER xp_insert_is_correct_trigger
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE PROCEDURE xp_insert_is_correct_trigger_fnc();