CREATE TABLE department_master(
    department_id INT PRIMARY KEY,
    department VARCHAR(255) NOT NULL
);

CREATE TABLE designation_master (
    designation_id INT PRIMARY KEY,
    designation VARCHAR(255) NOT NULL
);

-- Create employee_master table
CREATE TABLE employee_master (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255) NOT NULL,
    department_id INT,
    designation_id INT,
    FOREIGN KEY (department_id) REFERENCES department_master(department_id),
    FOREIGN KEY (designation_id) REFERENCES designation_master(designation_id)
);

-- Create employee_attendance table
CREATE TABLE employee_attendance (
    employee_id INT,
    at_date DATE NOT NULL,
    at_time VARCHAR(30) NOT NULL,
    punch_type VARCHAR(5),
    FOREIGN KEY (employee_id) REFERENCES employee_master(employee_id)
);

INSERT INTO employee_ATTENDANCE (employee_id, at_date, at_time, punch_type) VALUES
(1, '2021-02-01', '08:00', 'In'),
(2, '2021-02-01', '08:10', 'In'),
(1, '2021-02-01', '11:30', 'Out'),
(1, '2021-02-01', '11:35', 'Out'),
(1, '2021-02-01', '12:45', 'In'),
(2, '2021-02-01', '16:45', 'Out'),
(1, '2021-02-01', '17:30', 'Out'),
(1, '2021-02-01', '1:00', 'Out');

INSERT INTO department_master (department_id, department) VALUES
(1, 'Accounts'),
(2, 'Human Resource');

-- Insert data into designation_master
INSERT INTO designation_master (designation_id, designation) VALUES
(1, 'Manager'),
(2, 'Sr. Manager');

-- Insert data into employee_master
INSERT INTO employee_master (employee_id, employee_name, department_id, designation_id) VALUES
(1, 'Sunil Kumar Goel', 2, 1),
(2, 'Kamli Dawar', 1, 2);
