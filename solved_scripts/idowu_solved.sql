--  convert the punch_type to numbers with case
WITH punch_to_number AS (SELECT EMPLOYEE_ID, AT_DATE, CAST(AT_TIME AS TIME), punch_type, CASE WHEN punch_type = 'In' THEN 1
ELSE 0 END AS punch_type_to_number
    FROM 
    employee_attendance
    ORDER BY at_date,employee_id,CAST(at_time AS TIME)),

  --  get the previous punch_type_number before the current row, and also get the next  punch_type_number and the current row and the row numbe
    /* the query below, fetches the previous_punch_type_number before the current row, the next punch_type_number after the current row,
  the row number and count of each element in the partitioned.
  The window function divides the row by employee_id, the at_date and order it by the at_time
*/
    before_at_time_row AS
    (
    SELECT *, LAG(punch_type_to_number,1, 0) OVER(PARTITION BY  employee_id, at_date ORDER BY at_time  ROWS BETWEEN 1
    PRECEDING AND CURRENT ROW) AS previous_row_punch_type,
    LEAD(punch_type_to_number, 1,0) OVER(PARTITION BY  employee_id, at_date ORDER BY at_time  ROWS BETWEEN CURRENT ROW
    AND 1 FOLLOWING) AS next_row_punch_type,
    
    ROW_NUMBER() OVER(PARTITION BY  employee_id, at_date ORDER BY at_time  RANGE BETWEEN UNBOUNDED
    PRECEDING AND UNBOUNDED FOLLOWING ) row_num,
    
    COUNT(*) OVER(PARTITION BY  employee_id, at_date ORDER BY at_time  RANGE BETWEEN UNBOUNDED
    PRECEDING AND UNBOUNDED FOLLOWING ) length_partition
    FROM punch_to_number
    
    ),
   /* the query below, get all the content from the previous CTE, and substract the punch_type_number from the previous punch type number
  and also substract the punch_type_number from the next punch_type_number
*/
    dec_apple
    AS 
     (SELECT employee_id, at_date, at_time, punch_type, punch_type_to_number, 
    punch_type_to_number - previous_row_punch_type curr_sub_prev, 
    punch_type_to_number -  next_row_punch_type curr_sub_next,
    row_num,
    length_partition
    FROM before_at_time_row)
	,
    /* the below query, filters and get records that meet the requirement needed using this criteria
  If the punch_type_to_number is 1 and the difference between the current and the previous is 1 and the difference between the current and next is 1 (then this signifies a In) since the next punch_type_number is 0 (means out)
  If the punch_type_to_number is 0 and the difference between the current and the previous is 0 and the difference between the current and next is 1 and the row_number is not equal to 1 (then this is the last out before another in)
  If the punch_type_to_number is 0 and the difference between the current and the previous is -1  and the difference between the current and next is 1 and the row_number is not equal to 0  and the row_number is equal to the length of the window 
      (then this is the last out in the windo frame)
  If the punch_type_to_number is 0 and the difference between the current and the previous is 0  and the difference between the current and next is 0 and the row_number is not equal to 0  and the row_number is equal to the length of the window 
        (then this is the last out after one or more previous out)
*/
    dec_mango AS (SELECT employee_id, at_date, at_time, punch_type, punch_type_to_number
    FROM 
    dec_apple
    WHERE (punch_type_to_number=1 AND curr_sub_prev=1 AND curr_sub_next =1)
    OR (punch_type_to_number=0 AND curr_sub_prev=0 AND curr_sub_next =-1 AND row_num != 1 )
    OR
    (punch_type_to_number=0 AND curr_sub_prev=-1 AND curr_sub_next =0 AND row_num = length_partition)
    OR 
    (punch_type_to_number=0 AND curr_sub_prev=0 AND curr_sub_next =0 AND row_num = length_partition)
    ORDER BY at_date, employee_id, at_date, at_time),

  /* 
  This query get the row_number of the entire table without a partiton 
*/
    row_number_in_out AS (
    SELECT employee_id, at_date, at_time, punch_type,
    ROW_NUMBER() OVER() AS  row_num_
    FROM dec_mango
    ),
   /* 
  This query previous row_number of the current_row
*/
    lag_row_before_in_out AS (
    SELECT employee_id, at_date, at_time, punch_type, CAST(row_num_ AS INT),LAG(CAST(row_num_ AS INT),1,0) 
    OVER() before_number
    FROM row_number_in_out
    ),
     /* 
  This query does a self join, with itself, so the corresponding out can be match with the in 
  the join clause is based on the row_number and the lag_of the row_number i.e if row_number is 1 (in), the second row which is out will have lag_number of 1, hence you can join this together 
  the MOD(row_num,2) !=0 is to filter out even number i.e if row_number os 2(this is out), and you join with the next 2 (you will have a mismatch)
*/
    punch_result AS (SELECT lb.employee_id, lb.at_date in_date, lb.punch_type,CAST(lb.at_time AS TIME) || '.000' time_in,lo.at_date date_out, lo.punch_type,CAST(lo.at_time AS TIME) || '.000' time_out,
        (CAST(lo.at_time AS TIME) - CAST(lb.at_time AS TIME)) AS time_between
        FROM lag_row_before_in_out lb
        INNER JOIN lag_row_before_in_out lo 
        ON lb.row_num_ = lo.before_number AND (MOD(lb.row_num_,2) != 0 AND MOD(lo.before_number,2) != 0)),

  -- this get the employee details, the name, title, department 
    employee_details AS (
    SELECT     em.employee_id, em.employee_name, dem.department, dm.designation
    FROM employee_master em
    INNER JOIN designation_master dm
    ON em.designation_id = dm.designation_id
    INNER JOIN department_master AS dem
    ON em.department_id = dem.department_id
    
    )

  --  final query to join the employee_details with the punch_result. 
                                               
                                               SELECT ed.employee_id, ed.employee_name,ed.department, ed.designation, 
    pr.in_date, pr.time_in, pr.date_out, pr.time_out, pr.time_between
    FROM employee_details AS ed
    INNER JOIN punch_result pr
    ON ed.employee_id = pr.employee_id
    ORDER BY pr.employee_id, pr.in_date, pr.time_in
