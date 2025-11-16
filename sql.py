import sqlite3

class SQL() :
    @staticmethod
    def sql_connection() :
        try :
            con = sqlite3.connect('data.db')
            print("\n***connection successful***\n")
            return con
        except sqlite3.Error as error:
            print("\n***connection unsuccessful***\n", error)

    @staticmethod
    def create_table_students(con):
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Students(
                    s_id INTEGER PRIMARY KEY, 
                    first_name TEXT, 
                    last_name TEXT, 
                    age INTEGER, 
                    city TEXT)''')
        con.commit()

    @staticmethod
    def create_table_courses(con):
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Courses(
                    c_id integer PRIMARY KEY,
                    c_name text,
                    time_start text,
                    time_end text)''')
        con.commit()

    @staticmethod
    def students_add_data(con, values:tuple):
        cur = con.cursor()
        cur.execute('INSERT OR IGNORE INTO Students VALUES(?,?,?,?,?)', values)
        con.commit()

    @staticmethod
    def courses_add_data(con, values: tuple):
        cur = con.cursor()
        cur.execute('INSERT OR IGNORE  INTO Courses VALUES(?,?,?,?)', values)
        con.commit()


    @staticmethod
    def create_table_student_courses(con) :
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS StudentCourses (
                    course_id integer,
                    student_id integer,
                    FOREIGN KEY(course_id) REFERENCES Courses(c_id),
                    FOREIGN KEY(student_id) REFERENCES Students(s_id))''')
        con.commit()

    @staticmethod
    def student_courses_add_data(con, values:tuple):
        cur = con.cursor()
        cur.execute('INSERT OR IGNORE INTO StudentCourses VALUES(?,?)', values)
        con.commit()

    @staticmethod
    def get_student_older_than(con, min_age:int):
        cur = con.cursor()
        query = '''SELECT first_name, last_name FROM Students WHERE age >= ?'''
        try :
            cur.execute(query, (min_age,))
            res = cur.fetchall()
            print(f'Студенты, старше {min_age} лет:')
            for item in res :
                print(f'{item[1]}\t{item[0]}')
            print('\n')
        except sqlite3.Error as error:
            print(error)

    @staticmethod
    def get_student_studying(con, course_name:str):
        cur = con.cursor()
        query = '''SELECT first_name, last_name 
                FROM Students
                JOIN Courses ON StudentCourses.course_id = Courses.c_id
                JOIN Students ON StudentCourses.student_id = Students.s_id
                WHERE Courses.c_name = ?'''
        try:
            cur.execute(query, (course_name,))
            res = cur.fetchall()
            print(f'Студенты, обучающиеся на курсе {course_name}:')
            for item in res:
                print(f'{item[1]}\t{item[0]}\n')

        except sqlite3.Error as error:
            print(error)


    @staticmethod
    def get_student_studying_from_city(con, city_name:str, course_name:str):
        cur = con.cursor()
        query_city = '''SELECT first_name, last_name
                     FROM Students WHERE city = ?'''
        query_course = '''SELECT first_name, last_name 
                FROM Students
                JOIN Courses ON StudentCourses.course_id = Courses.c_id
                JOIN Students ON StudentCourses.student_id = Students.s_id
                WHERE Courses.c_name = ?'''

        try :
            cur.execute(query_city, (city_name,))
            res_city = cur.fetchall()
            cur.execute(query_course, (course_name,))
            res_course = cur.fetchall()
            print(f'Студенты, обучающиеся на курсе {course_name} и проживающие в {city_name}:')
            for item in res_city:
                if item in res_course:
                    print(f'{item[1]}\t{item[0]}\n')

        except sqlite3.Error as error:
            print(error)

    @staticmethod
    def any_command(con, command:str):
        cur = con.cursor()
        query = command
        try :
            cur.execute(query)
            if ('SELECT' in command):
                res = cur.fetchall()
                for item in res:
                    print(item)

            else:
                con.commit()
        except sqlite3.Error as error:
            print(error)




    @staticmethod
    def finish_session(con):

        con.close()

        print("\n***disconnected from database***\n")






