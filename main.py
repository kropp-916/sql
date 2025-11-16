from sql import SQL

current_con = SQL.sql_connection()

SQL.create_table_students(current_con)
SQL.create_table_courses(current_con)
SQL.create_table_student_courses(current_con)

student_data = [
    (1, 'Max', 'Brooks', 24, 'Spb'),
    (2, 'John', 'Stones', 15, 'Spb'),
    (3, 'Andy', 'Wings', 45, 'Manchester'),
    (4, 'Kate', 'Brooks', 34, 'Spb')
]

courses_data = [
    (1, 'python', '21.07.21', '21.08.21'),
    (2, 'java', '13.07.21', '16.08.21')
]

student_courses_data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2)
]


for item in student_data:
    SQL.students_add_data(current_con, item)

for item in courses_data:
    SQL.courses_add_data(current_con, item)

for item in student_courses_data:
    SQL.student_courses_add_data(current_con, item)


min_age = 30
city_name = 'Spb'
course_name = 'python'

SQL.get_student_older_than(current_con, min_age)
SQL.get_student_studying(current_con, course_name)
SQL.get_student_studying_from_city(current_con, city_name, course_name)

SQL.finish_session(current_con)


